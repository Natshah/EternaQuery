#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib
import urlparse
import sys
import csv
import os
import json

import getpass
import requests
from collections import namedtuple

import pprint
pprint = pprint.PrettyPrinter(indent=2, depth=2)


import logging
logger = logging.getLogger( __name__ )

import httplib2
from apiclient.http import MediaFileUpload
import apiclient.discovery as discovery
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

import authorization.oauth2 as oauth2
import authorization.clientlogin as clientlogin

__app__ = os.path.splitext(sys.argv[0])[0]



                     
class FTClientOAuth2():
                        
    def __init__(self, table_id=None):

        self.table_id = table_id
        self.Table()

        # init URLs
        self.FUSIONTABLES_URL = self.build_uri('/view')
                         
        # authorize http on init
        self.oauth2 = oauth2.OAuth2()
        self.http = self.oauth2.http()

        self.apis = namedtuple('apis', ['fusiontables'])
        self.init_services()

        self._table = self.apis.fusiontables.table
        self._column = self.apis.fusiontables.column
        self._query = self.apis.fusiontables.query
    
        self.request = None

     
    def Table(self, table_id=None):
        if table_id:
            self.table_id = table_id
        logger.debug("(TableID={})".format(self.table_id))
        return self

                         
    def init_services(self):
        """
        :build service for each api in self.apis
        """
        for api in self.apis._fields:
            service = getattr(self.apis, api)
            # skip if api service already built
            if not isinstance(service, property):
                continue
            # build api service
            logger.info("Building API Service ... (api={})".format(api))
            service = discovery.build(api, 'v2', http=self.http)
            setattr(self.apis, api, service)


    def build_uri(self, uri_type, params=None):
        """
        :generate uri
        """
        uri_map = {'/view': {'host': "https://fusiontables.google.com",
                            'path': "/DataSource?docid={table_id}"}}
        uri = "https://{host}{path}".format(**uri_map[uri_type])
        # check for params (optional)  
        if params:
            if type(params) != str:
                params = urllib.urlencode(params) 
            uri = "{uri}?{params}".format(uri=uri, params=params)
        # check for table_id
        if '{table_id}' in uri:
            uri = uri.format(table_id=self.table_id)
        return uri


    def _process_request(self, name='request', resumable=False):
        response = None
        if resumable is True:
            # resumable 
            while response is None:
                status, response = self.request.next_chunk()
                logger.debug("response: {}".format(response))
                if status:
                    prog = int(status.progress()*100)
                    logger.debug("{}.progress: {}".format(name, prog))
        else:
            try:
                response = self.request.execute()
            except:
                response = self.request 

        # process multiple pages
        if response.get('nextPageToken'):
            _response = response
            #logging.debug(pprint.pformat(_response))  
            while response.get('nextPageToken'):
                # process url, params (add pageToken=nextPageToken to url)
                url = urlparse.urlparse(self.request.uri)
                query = dict(urlparse.parse_qsl(url.query)) 
                query.update(pageToken=response.get('nextPageToken'))
                self.request.uri = url._replace(
                    query=urllib.urlencode(query)).geturl()
                logger.debug(self.request.uri)

                # execute response
                response = self.request.execute()
                #logging.debug(pprint.pformat(response))

                # update items in _response container
                #logging.debug(pprint.pformat(len(_response)))
                for k, v in _response.iteritems():
                    if type(v) is list:
                        _response[k] += response[k]
       
            # set response
            response = _response
            logging.debug(pprint.pformat(_response))

        # process response
        if type(response) == str:
           response = json.loads(response)

        # signal complete
        logger.info("{} complete.".format(name))
        
        self.request = response
        return True


    def copy_table(self):
        self.request = self._table().copy(tableId=self.table_id)
        self._process_request(name='copy_table')
        table_id = self.request.get('tableId')
        return table_id
                

    def list_columns(self):
        self.request = self._column().list(tableId=self.table_id)
        self._process_request(name='list_columns')
        column_data = self.request.get('items', [])
        return column_data


    def insert_columns(self, columns):
        responses = []
        for column in columns:
            column_data = {"name":column, "type": "NUMBER"}
            self.request = self._column().insert(self.table_id, body=column_data)
            self._process_request(name='insert_columns')
            responses.append(self.request)
        return responses


    def import_rows(self, csv_file, table_id=None):
        """
        Imports more rows into a table.
        :wrapper for importRows
            see: https://developers.google.com/resources/api-libraries/documentation/fusiontables/v2/python/latest/fusiontables_v2.table.html
            importRows(tableId=*, media_body=None, **params)
        """
        if table_id:
            self.table_id = table_id

        params = {'startLine': 1, # skip cols?
                  'encoding': "UTF-8",
                  'delimiter': ",",
                  'isStrict': True}

        media = MediaFileUpload(csv_file, mimetype='text/csv', resumable=True)
        self.request = self._table().importRows(tableId=self.table_id, media_body=media, **params)
        self._process_request(name='import_rows', resumable=True)
        
        # URL for new look 
        logger.info("The fusion table is located at: {}".format(
                self.build_uri('/view')))
        return True   


    def sql_select(self, columns="*"):
        if type(columns) == list:
            columns = ", ".join(columns)
        self.request = self._query().sqlGet(
            sql="SELECT {columns} FROM {table_id}".format(
            columns=columns, table_id=self.table_id))
        self._process_request('sql_select_column')
        return self.request









class ClientLoginFusionClient():

    def __init__(self, token=None):
        if token is None:
            token = ClientLogin().authorize()
        self.auth_token = token
        self.headers = {
            "Authorization": "GoogleLogin auth=" + self.auth_token,
        }

    def format_url(self, *args):
        url = 'https://www.googleapis.com/fusiontables/v2/tables/{}'
        return url.format('/'.join(args))


    def get(self, url):
        """
        """
        resp = requests.get(url, headers=self.headers)
        return resp.json()

        
    def post(self, url, data):
        if type(data) is not dict:
            data = json.loads(data)
        resp = requests.post(url, headers=self.headers, json=data)
        return resp.json()



    def insert_columns_legacy(self):
        """
        """
          
        def _format_curl(column):
            url =  'https://www.googleapis.com/fusiontables/v2/tables/{tableId}/columns' 
            curl = ' '.join(['curl',
                             '-X', 'POST',
                             '-H', "'Content-Type: application/json'",
                             '-H', "'Authorization: Bearer {auth_token}'",
                             '-d', "'{data}'", url])
            curl = curl.format(auth_token=self.options.auth_token, 
                               data=json.dumps({"name": column, "type": "NUMBER"}),
                               tableId=self.options.tableID) 
            return curl 


        self._authenticate_token()
        self.df = self.verify_columns()
        curl_cmds = map(_format_curl, self.df.columns)

        if self.options.dry is True:
            print "\n".join(("[curl]\t{}".format, curl_cmds))
        else:
            for curl_cmd in curl_cmds:           
                o = submit_command(curl_cmd, verbose=self.options.verbose)
                if self.options.debug is True:
                    print " [debug]\thit enter to continue"
                    x = raw_input()

        return self.df
