# -*- coding: UTF-8 -*-

import json
import sys

import requests

from pandasticsearch.errors import ServerDefinedException


class RestClient(object):
    """
    RestClient talks to Elasticsearch cluster through native RESTful API.
    """

    def __init__(self, url, endpoint='', username=None, password=None, verify_ssl=True):
        """
        Initialize the RESTful from the keyword arguments.

        :param str url: URL of Broker node in the Elasticsearch cluster
        :param str endpoint: Endpoint that Broker listens for queries on
        """
        self.url = url
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl

    def _prepare_url(self):
        if self.url.endswith('/'):
            url = self.url + self.endpoint
        else:
            url = self.url + '/' + self.endpoint
        return url

    def get(self, params=None):
        """
        Sends a GET request to Elasticsearch.

        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200', '_mapping/index')
        >>> print(client.get())
        """
        try:
            url = self._prepare_url()
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            auth = None
            if username is not None and password is not None:
                auth = (username, password)

            res = requests.get(url, auth=auth, params=params)
            res.raise_for_status()
            return res.json()
        except:
            reason = res.json()
            raise ServerDefinedException(reason)

    def post(self, data, params=None):
        """
        Sends a POST request to Elasticsearch.

        :param data: The json data to send in the body of the request.
        :param optional params: Dictionary to be sent in the query string.
        :return: The response as a dictionary.

        >>> from pandasticsearch import RestClient
        >>> client = RestClient('http://localhost:9200', 'index/type/_search')
        >>> print(client.post(data={"query":{"match_all":{}}}))
        """
        try:
            url = self._prepare_url()
            username = self.username
            password = self.password
            verify_ssl = self.verify_ssl

            auth = None
            if username is not None and password is not None:
                auth = (username, password)

            res = requests.post(url, json=data, auth=auth, params=params)
            res.raise_for_status()
            return res.json()
        except:
            reason = res.json()
            raise ServerDefinedException(reason)

