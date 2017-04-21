# -*- coding: utf-8 -*-
from collective.jsonmigrator import logger
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from cpskin.migration.blueprints.utils import BATCH_CURRENT_KEY
from cpskin.migration.blueprints.utils import BATCH_SIZE_KEY
from cpskin.migration.blueprints.utils import TOTAL_OBJECTS_KEY
from zope.annotation.interfaces import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

import base64
import json
import logging
import requests
import threading
import time
import urllib
import urllib2

logging.getLogger('requests').setLevel(logging.WARNING)


class CatalogSourceSection(object):

    """A source section which creates items from a remote Plone site by
       querying it's catalog.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.options = options
        self.context = transmogrifier.context

        self.remote_url = self.get_option('remote-url',
                                          'http://localhost:8080')
        self.remote_username = self.get_option('remote-username', 'admin')
        self.remote_password = self.get_option('remote-password', 'admin')

        catalog_path = self.get_option('catalog-path', '/portal_catalog')
        self.site_path_length = len('/'.join(catalog_path.split('/')[:-1]))

        catalog_query = self.get_option('catalog-query', None)
        catalog_query = ' '.join(catalog_query.split())
        catalog_query = base64.b64encode(catalog_query)

        self.remote_skip_paths = self.get_option('remote-skip-paths',
                                                 '').split()
        self.queue_length = int(self.get_option('queue-size', '2'))
        # Install a basic auth handler
        auth_handler = urllib2.HTTPBasicAuthHandler()
        auth_handler.add_password(realm='Zope',
                                  uri=self.remote_url,
                                  user=self.remote_username,
                                  passwd=self.remote_password)
        opener = urllib2.build_opener(auth_handler)
        urllib2.install_opener(opener)
        req = urllib2.Request( '%s%s/get_catalog_results' % (self.remote_url, catalog_path), urllib.urlencode( { 'catalog_query': catalog_query}))
        try:
            f = urllib2.urlopen(req)
            resp = f.read()
        except urllib2.URLError:
            raise

        self.item_paths = sorted(json.loads(resp))
        anno = IAnnotations(self.context)
        anno[TOTAL_OBJECTS_KEY] = len(self.item_paths)
        batch_current = int(anno[BATCH_CURRENT_KEY])
        batch_size = int(anno[BATCH_SIZE_KEY])
        batch_end = batch_current + batch_size
        self.item_paths = self.item_paths[batch_current:batch_end]

    def get_option(self, name, default):
        """Get an option from the request if available and fallback to the
        transmogrifier config.
        """
        request = getattr(self.context, 'REQUEST', None)
        if request is not None:
            value = request.form.get('form.widgets.' + name.replace('-', '_'),
                                     self.options.get(name, default))
        else:
            value = self.options.get(name, default)
        if isinstance(value, unicode):
            value = value.encode('utf8')
        return value

    def __iter__(self):
        for item in self.previous:
            yield item

        queue = QueuedItemLoader(self.remote_url, self.item_paths,
                                 self.remote_skip_paths, self.queue_length,
                                 self.remote_username, self.remote_password)
        queue.start()
        anno = IAnnotations(self.context)
        for item in queue:
            if not item:
                continue

            item['_path'] = item['_path'][self.site_path_length:]
            anno[BATCH_CURRENT_KEY] += 1
            yield item


class QueuedItemLoader(threading.Thread):

    def __init__(self, remote_url, paths, remote_skip_paths, queue_length,
                 remote_username, remote_password):
        super(QueuedItemLoader, self).__init__()

        self.remote_url = remote_url
        self.paths = list(paths)
        self.remote_skip_paths = remote_skip_paths
        self.queue_length = queue_length
        self.remote_username = remote_username
        self.remote_password = remote_password

        self.queue = []
        self.finished = len(paths) == 0

    def __iter__(self):
        while not self.finished or len(self.queue) > 0:
            while len(self.queue) == 0:
                time.sleep(0.00001)

            yield self.queue.pop(0)

    def run(self):
        while not self.finished:
            while len(self.queue) >= self.queue_length:
                time.sleep(0.00001)

            path = self.paths.pop(0)
            if not self._skip_path(path):
                item = self._load_path(path)
                self.queue.append(item)

            len_path = len(self.paths)
            if len_path == 0:
                self.finished = True

    def _skip_path(self, path):
        for skip_path in self.remote_skip_paths:
            if path.startswith(skip_path):
                return True
            # also skip path if not in good batch

        return False

    def _load_path(self, path):
        item_url = '%s%s/get_item' % (self.remote_url, urllib.quote(path))

        resp = requests.get(
            item_url,
            auth=(self.remote_username, self.remote_password),
            timeout=60)
        content_type = resp.headers['content-type']
        if content_type != 'application/json':
            logger.error('Could not get : {}'.format(resp.url))
            return None
        else:
            item = resp.json()

        return item
