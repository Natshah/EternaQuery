#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess as sp
import multiprocessing as mp
import json 
import inspect 
import os
import csv
import glob

import numpy as np
import pandas as pd


import pprint
pprint = pprint.PrettyPrinter(indent=2, depth=4)

import logging
logger = logging.getLogger( __name__ )

import eternadata.util as util
import eternadata.fusiontables.client as ft_client
import eternadata.fusiontables.util as ft_util

#exit()
###############################################################################
### globals
###############################################################################
__dir__ = os.path.dirname(inspect.getfile(inspect.currentframe()))


###############################################################################
### helpers
###############################################################################
def get_unique(s):
    if isinstance(s, pd.Series):
        return s.iloc[0]
    if isinstance(s, list):
        return s.pop(0)
    return s
    
def format_ID_columns(df):
    for c in df.columns:
        if not c.endswith('ID'):
            continue
        try:
            df[c] = df[c].astype(int)
        except Exception as e:
            logger.error("df[{}].astype(int) failed\n(error={})".format(c,e))
    return df

###############################################################################
### main
###############################################################################
class MetaDataFusionTableFlow():
    
    def __init__(self, options=None):

        self.__name__ = str(self.__class__)
        self.logger = logging.getLogger(self.__name__)
        
        self.options = self._validate_options(options)

        self.ft_client = ft_client.FTClientOAuth2()

        self.df, self.df_cache = None, {}

        self.fn_cache = {}
  
        self.table_name, self.table_map = None, {}
        self._tableFlow = None

        # inti source df
        source_df = util.load_dataframe(self.options.source, 
                                        na_filter=False)
        self.df_cache[self.options.source] = source_df
   
     
  

    def _validate_options(self, options):
        self.logger.debug('(options={})'.format(pprint.pformat(options)))
        return options


    def init_tables(self, table_map):
        # validate table map
        for attr in dir(self):
            if  'method' not in str(type(getattr(self, attr))):
                continue
            if not attr.lower().startswith('tableFlow_'):
                continue
            # warn if missing
            if attr not in table_map:
                self.logger.warn("Missing tableId for MetaData Table" + 
                    " (tableFlow={}, tableName={})"
                    .format(attr, attr.split('tableFlow_').pop()))
  
        self.logger.debug("{ #Normalized Meta/Meta TableName: TableID }\n" + 
            "{}".format(pprint.pformat(table_map)))
        self.table_map = table_map
        return self



    def run_flow(self, table_flow=None):
        """        
        """
        if table_flow is None:
            table_flow = self.table_map.keys()
        if type(table_flow) != list:
            table_flow = [table_flow]

        for table_name in table_flow:
            self.logger.info("@ Initializing MetaData Flow for Table:" + 
                "\t{}".format(table_name))
            self.logger.debug("(TableName={})".format(table_name))
            self.flow_runner(table_name).execute()

        return self


    def flow_from_table_name(self, table_name=None):
        # table flow
        if table_name:
            self.table_name = table_name
        _tableFlow = 'tableFlow_{TableName}'.format(
            TableName=self.table_name)
        self._tableFlow = getattr(self, _tableFlow)
        return self._tableFlow

    def execute(self, table_name=None):
        if table_name:
            self.flow_from_table_name(table_name)
        self.logger.info("@ Executing MetaData Flow")
        self.logger.debug("(TableFlow={})".format(self._tableFlow))
        self.df_cache[self.table_name] = self._tableFlow()
        self.logger.debug("(TableName={})"
            .format(pprint.pformat(self.table_name)))
        try:
            self.logger.debug("\n(DataFrame=\n{}\n)"
                .format(pprint.pformat(
                    self.df_cache[self.table_name].describe().head(1))))
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("\n(DataFrameInfo=\n{}\n)"
                    .format(pprint.pformat(
                        self.df_cache[self.table_name].info())))

        except:
            self.logger.debug("(DataFrame={})"
                .format(pprint.pformat(
                    self.df_cache[self.table_name])))
        return self

    def flow_runner(self, table_name=None):
        self.flow_from_table_name(table_name)
        return self


    def save_tables(self):
        for table_name, df in self.df_cache.iteritems():
            if table_name not in self.table_map:
                continue
            self.save_table(table_name=table_name, df=df)
        return self 

    def save_table(self, table_name=None, df=None):
        if table_name is None:
            table_name = self.table_name
        if df is None:
            df = self.df_cache[table_name]
        if df is None:
            self.logger.warning("DataFrame is NoneType (TableName={})"
                           .format(table_name))
            return self
        outfile = "MetaDataTableFlow_{}.csv".format(table_name)
        if self.options.outfile is not None:
            outfile = os.path.join(
                os.path.dirname(self.options.outfile),
                outfile.replace('.csv',
                    os.path.basename(self.options.outfile)))
        self.logger.info("@ Writing DataFrame to File for Table:"
            .format(table_name))
        self.logger.debug("(File={})".format(outfile))
        self.logger.debug("(TableName={})".format(table_name))
        self.logger.debug("\n(DataFrame=\n{}\n)"
                     .format(pprint.pformat(df.describe().head(1))))
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("\n(DataFrameInfo=\n{}\n)"
                     .format(pprint.pformat(df.info())))
        util.write_dataframe(df, outfile)
        self.fn_cache[table_name] = outfile
        return self

    
    def upload_tables(self):
        for table_name, table_id in self.table_map.iteritems():
            if table_name not in self.fn_cache:
                continue
            filename = self.fn_cache[table_name]
            self.upload_table(table_id, filename)
        return self

    def upload(self, table_id=None, filename=None):
        #self.ft_client
        ft_util.upload_csv(table_id, filename)
        return self


    def tableFlow_ProjectsPuzzles(self):
        """
        :Projects/Puzzles
           meta = {Project_ID: 
                    Project_Name
                    Puzzle_ID
                    Puzzle_Name,
                    Design_Count
                    State_Count}
        """
        table_name = self.table_name = 'ProjectsPuzzles'
        table_id = self.table_map[table_name]

        # init source df
        source_df = self.df_cache[self.options.source].copy()
        source_df = source_df.set_index('Puzzle_ID')

        table = self.ft_client.Table(table_id)
        columns = table.list_columns()
        self.logger.debug(pprint.pformat(columns))
        fields = [_.get('name') for _ in columns]
        meta = dict((_, []) for _ in fields)
        self.logger.debug(pprint.pformat(meta))
      
        for Puzzle_ID in set(source_df.index):  
            puzzles_df = source_df.ix[Puzzle_ID]
            meta['Puzzle_ID']    += [Puzzle_ID]
            meta['Project_ID']   += list(puzzles_df.Project_ID.unique())
            meta['Project_Name'] += list(puzzles_df.Project_Name.unique())
            meta['Puzzle_Name']  += list(puzzles_df.Puzzle_Name.unique())
            meta['Design_Count']  += [len(puzzles_df)] 
            meta['State_Count']  += ['2']
        
        self.logger.debug(pprint.pformat(zip(*map(meta.get, fields))))
        self.df = pd.DataFrame(zip(*map(meta.get, fields)), 
            columns=fields)
        self.df = format_ID_columns(self.df)
        return self.df



    def tableFlow_RoundsProjects(self):
        """
        :Rounds/Projects
              meta = {'Ready':'Y', 
                   'Synthesis_Round': int
                   'Project'
                   'Project Round': #,
                   'Project_plus_Round: project name + "Round #"
                   'Project_ID': }
        """
        table_name = self.table_name = 'RoundsProjects'
        table_id = self.table_map[table_name]
        
        # init source df
        source_df = self.df_cache[self.options.source]

        #
        table = self.ft_client.Table(table_id)
        columns = table.list_columns()
        self.logger.debug(pprint.pformat(columns))
        fields = [_.get('name') for _ in columns]
        meta = dict((_, []) for _ in fields)
        self.logger.debug(pprint.pformat(meta))
      
        done = []
        for index, df in source_df.iterrows():
            if df.Synthesis_Round in done:
                continue
            done += [df.Synthesis_Round]
            if "Round" in df.Project_Name:
                [Project_Name, Project_Round] = df.Project_Name.split('Round ')
                df.Project_Round = Project_Round[0]
                df.Project_Name = Project_Name.replace('(','').strip()
            try:
                df.Synthesis_Round = max(df.Synthesis_Round)
            except:
                pass

            meta['Ready'] += ['Y']
            meta['Project'] += ["{}".format(df.Project_Name)]
            meta['Project_Round'] += [df.Project_Round]
            meta['Project_plus_Round'] += ["{} Round {}".format(
                df.Project_Name, df.Project_Round)] # max?
            meta['Project_plus_Round_ID'] += [df.Project_ID]
            meta['Synthesis_Round'] += [df.Synthesis_Round]
            meta['Histogram_URL_Template'] += [
                "https://s3.amazonaws.com/eterna/labs/histograms_R{}/{}.png"
                .format(df.Synthesis_Round, "{Design_ID}")
            ]
          
        self.logger.debug(pprint.pformat(zip(*map(meta.get, fields))))
        self.df = pd.DataFrame(zip(*map(meta.get, fields)), columns=fields)
        self.df = format_ID_columns(self.df)
        return self.df




    def tableFlow_ProjectsColumns(self):
        """
        :Projects/Columns
            meta = {Ready: 'Y',
                    Project Name,
                    Puzzle_ID
                    Column_Name
                    Comments:''}
        """
        table_name = self.table_name = 'ProjectsColumns'        
        table_id = self.table_map[table_name]

        # init source df
        source_df = self.df_cache[self.options.source]

        #
        table = self.ft_client.Table(table_id)
        columns = table.list_columns()
        fields = [_.get('name') for _ in columns]
        meta = dict((_, []) for _ in fields)
        
        column_names = table.sql_select('Column_Name').get('rows')
        column_names = set([__ for _ in column_names for __ in _])
        self.logger.debug(pprint.pformat(column_names))
        
        for column_name in column_names:
            meta['Ready'] += ['Y']
            meta['Project_ID'] += list(source_df.Project_ID.unique())
            meta['Project_Name'] += list(source_df.Project_Name.unique())
            meta['Column_Name'] += [column_name]
            meta['Notes'] += ['']

        self.logger.debug(pprint.pformat(zip(*map(meta.get, fields))))
        self.df = pd.DataFrame(zip(*map(meta.get, fields)), columns=fields)
        self.df = format_ID_columns(self.df)
        return self.df



    def tableFlow_ColumnDefinitions(self):
        """
        :Column/Definitions
            meta = {'Column_Name': 
                    'Column_Label': 
                    'Sort_Order':
                    'Importance':
                    'Siqi_sub_1':
                    'Siqi_sub_2':
                    'Siqi_sub_3':
                    'Siqi_sub_4':
                    'Usage':
                    'Precision':
                    'Description':}
        """
        table_name = self.table_name = 'ColumnDefinitions'
        table_id = self.table_map[table_name]
       
        """
        ### Column/Definitions
        # this table is only needed for new columns inserted into the table
        # * not immediate need

         # init source df
        source_df = self.df_cache[self.options.source]
      
        table = self.ft_client.Table(table_id)
        columns = table.list_columns()
        fields = [_.get('name') for _ in columns]
        meta = dict((_, []) for _ in fields)

        print pprint.pprint(pprint.pformat(meta))

        table_data = table.sql_select('*')
        cols, rows = table_data.get('columns'), table_data.get('rows')
        #self.logger.info(pprint.pformat(table_data))

        table_data = dict(zip(cols, 
        [set([r[i] for r in rows]) for i,_ in enumerate(cols)]))
        self.logger.info(pprint.pformat(table_data))
        """
        return None


###############################################################################
### main script
###############################################################################
if __name__=="__main__":

    # script-specific imports
    import argparse
    
    ### TODO
    parser = argparse.ArgumentParser(
        description='Automated Eterna Fusion Tables Metadata Curation')
    parser.add_argument('-s', '--source', default=None)
    parser.add_argument('-o', '--outfile', default=None)
    parser.add_argument('-u', '--upload', default=False, action='store_true')
    parser.add_argument('-l', '--log', default='INFO')
    options = parser.parse_args()
  
    # init log levels
    util.configure_logging(level=options.log.upper())
    
    # script-specific imports
    """
        Generate the 4 sheets of metadata
        1. Rounds/Project
        2. Projects/Puzzles
        3. Project/Columns
        4. Column_Definitions:
    """
    mdflow = MetaDataFusionTableFlow(options)
    mdflow.init_tables({
        'ProjectsPuzzles':  '18ASX43DY99SCxwsU_I5B0A6EyqkJ4AMmuVGMkQed',
        'ProjectsColumns':  '1EQijhCi8GMYNraBGO2d8p_kyE-lNcxCzvasNAxiW',
        'RoundsProjects':    '1KgEJ7s-Aroey6G2MG_Z-bE5QZbVnCUfFnX7pfjWw',
        'ColumnDefinitions': '1RHRfMztSKXrGYC9HAuqepSQcTPqPFCY5fThs-iVJ'
        })
    mdflow.run_flow().save_tables()

    if options.upload is True:
        mdflow.upload_tables()
