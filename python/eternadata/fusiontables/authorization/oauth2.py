# -*- coding: utf-8 -*-

import sys
import os
import json

import inspect
__source_file__ = os.path.splitext(os.path.basename(
    inspect.getsourcefile(inspect.currentframe())))[0]
__source_dir__ = os.path.dirname(
    inspect.getsourcefile(inspect.currentframe()))

import pprint
pprint = pprint.PrettyPrinter(indent=2, depth=2)

import logging
logger = logging.getLogger( __name__ )

import httplib2
from apiclient.http import MediaFileUpload
import apiclient.discovery as discovery
import oauth2client.file as oa2file
import oauth2client.client as oa2client
import oauth2client.tools as oa2tools

__app__ = os.path.splitext(os.path.basename(sys.argv[0]))[0]


class OAuth2():
  
    def __init__(self):
        self.client_secrets = None
        self.storage_file = None
        self._credentials = None
        self._credentials = self.oauth2init()
 
      
    def oauth2init(self):
        if self._credentials and not self._credentials.invalid:
            logger.debug("(credentials={})".format(self._credentials))
            return self._credentials
          
        # init storage file 
        elif self.storage_file is None:
            self.storage_file = '{}.creds'.format(__source_file__)
            storage_path = os.path.join(__source_dir__, '.credentials')
            if not os.path.exists(storage_path):
                os.makedirs(storage_path)
            self.storage_file = os.path.join(
              storage_path, self.storage_file)
            logger.debug("storage_path: {}".format(storage_path))
            logger.debug("storage_file: {}".format(self.storage_file))
            return self.oauth2init()
          
        elif self.client_secrets is None:
            self.client_secrets = '{}.client_secrets.json'.format(__source_file__)
            storage_path = os.path.join(__source_dir__, '.credentials')
            if not os.path.exists(storage_path):
                os.makedirs(storage_path)
            self.client_secrets = os.path.join(
              storage_path, self.client_secrets)
            logger.debug("storage_path: {}".format(storage_path))
            logger.debug("client secrets: {}".format(self.client_secrets))
            return self.oauth2init()

        elif not os.path.exists(self.client_secrets):
            logger.info("Please setup a client_secrets.json")
            client_secret_fn = raw_input("Enter the name of the file containing your client secret: ")
            if not os.path.exists(client_secret_fn):
                return self.oauth2init()
            with open(client_secret_fn, 'r') as fid_in:  
                client_secret = fid_in.read()
                with open(self.client_secrets, 'w') as fid:
                    try:
                        fid.write(json.dumps(client_secret))
                    except:
                        fid.write(client_secret)
                    logger.debug("client_secret: {}".format(client_secret))      
            return self.oauth2init()
            
        elif self._credentials is None or self._credentials.invalid == True:
            # check storage for credentials
            storage = oa2file.Storage(self.storage_file)
            try:
                self._credentials = storage.get()
                logger.debug("credentials: {}".format(self._credentials))
                return self.oauth2init()

            except Exception as e:
                logger.debug(e)

            # run oauth2 flow to generate creds
            flow = oa2client.flow_from_clientsecrets(
                self.client_secrets,
                scope =  'https://www.googleapis.com/auth/fusiontables')
            flags = oa2tools.argparser.parse_args(['--noauth_local_webserver'])
            self._credentials = oa2tools.run_flow(flow, storage, flags)
            logger.debug("credentials: {}".format(self._credentials))
            return self.oauth2init()  
   
  
    def credentials(self):
        return self.oauth2init()     
   
  
    def authorize_http(self):
        http = httplib2.Http()
        self.credentials().authorize(http)
        http_request = http.request

        def _wrapper(uri, method="GET", body=None, headers=None, **kw):
            resp, content = http_request(uri, method, body, headers, **kw)
            logger.debug('Request\n{} {}\n{}\n\n{}'.format(
                method, uri, 
                pprint.pformat(headers) if headers else '', 
                pprint.pformat(body) if body else ''))
            logger.debug('Response\n{}\n\n{}'.format(
                    "200 OK" if resp.status == '200' else pprint.pformat(resp), 
                    pprint.pformat(json.loads(content))))
            return resp, content

        http.request = _wrapper
        return http

             
    def http(self):
        return self.authorize_http()

                    
                     

