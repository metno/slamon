""" Get thredds data catalogs, extract necessary information
"""

__author__ = "Christian Skarby"
__version__ = "0.2.0"
__license__ = "GPLv2+"

import datetime
import xml.etree.ElementTree

import requests
from logzero import logger


class ApplicationNode:
    """ Fetch from single node and get the node's result for all monitored models
    """

    XMLNS = {'InvCatalog': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}

    def __init__(self, apikey=None, models=(), name='node'):
        self.result = {}
        for model in models:
            self.result[model] = model()
        self.urls = []
        self.name = name
        self.headers = {}
        if apikey:
            self.headers['Authorization'] = 'Bearer {}'.format(apikey)

    def models(self):
        """ Generator providing module objects
        """
        for cls in self.result:
            yield self.result[cls]

    def fetch(self):
        """ Fetch URL and send response.content to callback
        """
        for model in self.models():
            if not model.URL in self.urls:
                self.urls.append(model.URL)
                response = requests.get(model.URL, headers=self.headers, timeout=10)
                response.raise_for_status()
                self.__callback(response.content)
                logger.debug("Fetched %s from %s", model.URL, self.name)
            logger.debug(
                "Most recent bulletin for %s on %s is %s.",
                model.NAME, self.name, model.bulletin
            )

    def __callback(self, value):
        """ Store most recent bulletin for each model
        """
        xmltree = xml.etree.ElementTree.fromstring(value)
        for dataset in xmltree.findall(".//InvCatalog:dataset/*[@name]", self.XMLNS):
            for model in self.models():
                try:
                    bulletin = datetime.datetime.strptime(dataset.get('name', ''), model.PATTERN)
                except ValueError:
                    pass
                else:
                    bulletin = bulletin.replace(tzinfo=datetime.timezone.utc)
                    # Set model bulletin to most recent for that model available through this node
                    if not model.bulletin or bulletin > model.bulletin:
                        model.bulletin = bulletin
