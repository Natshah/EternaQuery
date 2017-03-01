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
pprint = pprint.PrettyPrinter(indent=2, depth=2)

import logging


import eternadata.util as util

import eternadata.fusiontables.client as ft_client
import eternadata.fusiontables.util as ft_util

###############################################################################
### globals
###############################################################################
__dir__ = os.path.dirname(inspect.getfile(inspect.currentframe()))
__file__ = os.path.basename(inspect.getfile(inspect.currentframe()))
logger= logging.getLogger( __name__ )


###############################################################################
### helpers
###############################################################################
def fixup_fusion_columns(df):
    
    # make replacements
    # TODO: allow user to specify list of replacements
    fn = os.path.join(__dir__, "johan_fusion_replacements.csv") 
    replacements = map(tuple, util.load_dataframe(fn).values) 
    for (key, val) in replacements:
        columns_replaced = dict(
            (c, c.replace(key, val)) for c in df.columns if key in c)
        df.rename(columns = columns_replaced, inplace=True)
    

    # remove trailing _
    df = df.rename(columns = dict(
        (c, c[:-1]) for c in df.columns if c.endswith('_')))
    df = df.rename(columns = dict(
        (c, c.replace('__', '_')) for c in df.columns if '__' in c))
    df = df.rename(columns = dict(
        (c, c.replace('err', 'sem')) for c in df.columns if 'err' in c))    

    # TODO: allow user to specify list of conversions
    # convert from CamelCase to Camel_Case
    fn = os.path.join(__dir__, "johan_fusion_conversions.csv") 
    conversions =  dict(map(tuple, util.load_dataframe(fn).values))
    df.rename(columns = conversions, inplace = True)
    
    return df


    

###############################################################################
### main
###############################################################################
class FusionUtil():

    def __init__(self, options):
        
        self.__name__ = str(self.__class__)
        self.logger = logging.getLogger(self.__name__)
        
        self.options = self._validate_options(options)

        self.df_map = {}
        self.df = None 

        self.ft_client = ft_client.FTClientOAuth2(self.options.tableID)


    def _validate_options(self, options):
        # user should specify eterna round (i.e. R104 or 104
        if options.cloud_round and options.cloud_round.startswith("R"):
            options.cloud_round = options.cloud_round.replace("R","")

        print ('[FusionUtil.options]' +
               '\n\t{}'.format('\n\t'.join(
                ['{:15}= {}'.format(*_) for _ in options._get_kwargs()])))
        return options


    def _authenticate_token2(self):
        n_attempts, max_attempts = 0, 3
        while self.options.auth_token is None:
            print("Please enter an access token" +
                 " (see: https://developers.google.com/oauthplayground/):")
            self.options.auth_token = raw_input()
            if len(self.options.auth_token) < 10:
                self.options.auth_token = None
            print '[token]', self.options.auth_token
            
            # only try n times
            n_attempts += 1
            if n_attempts >= max_attempts:
                return False
        return True



    def apply_mode(self):
        for method in dir(self):
            if method.startswith('_'):
                continue
            if self.options.mode not in method:
                continue
            print '[FusionUtil.apply_mode(mode={})]'.format(method)
            self.df_map[method] = getattr(self, method)()
            if self.options.verbose:
                logger.info("{}: {}".format(method, 
                        pprint.pformat(self.df_map[method].head())))
                self.df_map[method].info()
            return True
        return False



    def process_data(self):
        """
            load --source, write --outfile
        """
        self.df = util.load_dataframe(self.options.source)
        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df


    def get_columns(self):
        """
        """
        if self.options.column_file:
            self.df = util.load_dataframe(self.options.column_file)
        else:
            coldata = self.ft_client.Table().list_columns()
            columns = [_.get('name') for _ in coldata]
            self.df = pd.DataFrame([], columns=columns)

        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df
   

    def verify_columns(self):
        """
        verify columns ...
        """
        try:
            source_cols = util.load_dataframe(self.options.source).columns
            fusion_cols = self.get_columns().columns
            assert(len(source_cols) and len(fusion_cols))
        except Exception as e:
            print e
            return None

        def _check_column(c_name, columns):
            """
            """
            return bool(c_name in columns or '"{}"'.format(c_name) in columns)

        status_map = [(c, _check_column(c, fusion_cols)) for c in source_cols]
        missing_cols = [c for c, status in status_map if status]
        if self.options.verbose:
            status_map = [(c, '+') if _ else (c, '-') for c, _ in status_map]
            print "\n".join(["[{}]\t{}".format(_, c) for c, _ in  status_map])

        self.df = pd.DataFrame([], columns=missing_cols)
        return self.df


    def fixup_columns(self):
        """
        """
        columns_missing = self.verify_columns().columns
        columns_valid = self.get_columns().columns

        self.df = util.load_dataframe(self.options.source)
        if len(columns_missing):
            self.df = fixup_fusion_columns(self.df)
            if self.options.verbose:
                print ','.join(['"{}"'.format(_) for _ in columns_missing])
        # if this fails, make sure xlsx has Design_ID and Design_Name
        self.df = self.df.reindex(columns=columns_valid)
       
        # remove controls?
        Design_ID_strlen = self.df.Design_ID.astype(str).str.len()
        Design_ID_valid = Design_ID_strlen.eq(Design_ID_strlen.max())
        self.logger.info("Dropping controls from DataFrame: {}".format(
            pprint.pformat(self.df[~Design_ID_valid])))
        self.df = self.df[Design_ID_valid]
        
        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df


    def insert_columns(self):
        """
        """
        self.df = self.verify_columns()
        self.ft_client.Table().insert_columns(self.df.columns)
        return self.df


    #### TODO: Hacky... needs some refactoring 
    def transfer_columns(self):
 
        # input file stuff
        source_df = util.load_dataframe(self.options.source)
        target_df = util.load_dataframe(self.options.target)
    
        ### join
        self.df = pd.DataFrame()
        if len(target_df):
            self.df = target_df.copy()
            self.df.update(source_df)
        else:
            self.df = source_df.copy()
            self.df = self.df.reindex(columns=target_df.columns)
       
        ### fixups
        # ensure ids are ints
        int_cols = ["Project_ID", "Project_Round", 
                    "Puzzle_ID",  "Puzzle_Round", 
                    "Design_ID", "Designer_ID"]
        for int_col in int_cols:
            self.df[int_col] = self.df[int_col].map(
                lambda _: "{:.0f}".format(_) if '.' in str(_) else _)

        # column replacements
        self.df['Sequence_Length'] = self.df['Sequence'].map(
            lambda _: type(_) if type(_) != str else len(_.strip()))
        if self.options.cloud_round:
            self.df['Synthesis_Round'] = self.df['Synthesis_Round'].map(
                lambda _: _ if _.isdigit() else self.options.cloud_round)
 
        # replacements
        self.df.replace({'"': '&quot;', 'NULL': ''}, regex=False)
    
        if self.options.verbose:
            print self.df

        # write to csv
        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df



    def query_eterna_data(self):
        """
        """
        # generate url 
        col_names = ['Design_ID', 'Project_ID', 'Project_Round',
                     'Puzzle_ID', 'Puzzle_Round', 'Design_Description',
                     'Design_Comments', 'Designer_ID']
        sele_cols = ['n.nid', 'puz.field_puzzle_lab_project_nid',
                     '__EMPTY_FIELD__', 'puz.nid', '__EMPTY_FIELD__', 
                     'nr.body', '__EMPTY_FIELD__', 'u.uid'] 

        def _format_curl(nid, nid_type='solnid'):
            url = str("http://staging.eternagame.org/get/?type=solutions" + 
                  "&{nid_type}={nid}".format(nid_type=nid_type, nid=nid) +
                  "&fields=" + ','.join(sele_cols) + 
                  "&select_as=" + ','.join(col_names))
            curl_cmd = "curl '{}'".format(url)
            self.logger.debug(curl_cmd)
            return curl_cmd

        # read source file
        source_df = util.load_dataframe(self.options.source, 
                               usecols=['Design_ID', 'Puzzle_Name',
                                        'Synthesis_Round'],
                               na_filter=False, low_memory=True)
        def get_unique(s):
            try:
                return list(s.unique())
            except AttributeError:
                return list(set(s)) if type(s) == list else [s]
     

        ### get unique puzzle ids
        def get_puzzle_ids():
            puzzle_ids = []

            # curl commands
            cmd_list = []
            puz_df = source_df.copy()
            
            # query all designs when missing puzzle names
            mask_na = puz_df.Puzzle_Name.eq('') 
            if mask_na.sum() > 0:
                self.logger.debug(puz_df)
                self.logger.debug("Setting Puzzle_Name == '' to index, will sampling 10")
                #puz_df[puz_df.Puzzle_Name.eq('')] = puz_df[puz_df.Puzzle_Name.eq('')].sample(10)
                #puz_df.Puzzle_Name[puz_df.Puzzle_Name.eq('')] = puz_df.Puzzle_Name[puz_df.Puzzle_Name.eq('')].index
                puz_df = puz_df[~mask_na | mask_na.sample(10)]
                puz_df.Puzzle_Name[mask_na] = puz_df.index
                self.logger.debug(puz_df)
                self.logger.debug(list(set(puz_df.index)))
               
            puz_df = puz_df.set_index(['Puzzle_Name', 'Synthesis_Round'])
            self.logger.debug(list(set(puz_df.index)))
            for puzzle_name in list(set(puz_df.index)):
                cmd_list += [_format_curl(
                    get_unique(puz_df.ix[puzzle_name]['Design_ID']).pop(),
                    nid_type='solnid')]
                logger.debug("querying db for puzzle: {}".format(puzzle_name))
                logger.debug("query solnid: {}".format(
                    get_unique(puz_df.ix[puzzle_name]['Design_ID']).pop()))

            # apply commands async
            results_async = util.map_async(util.submit_command, cmd_list)
            for d in results_async.get():
                d = json.loads(d.replace("\\r", " "))
                try:
                    d = d['data']['solutions']
                    self.logger.debug(d)
                    for sol_data in d:
                        puzzle_ids +=[sol_data['Puzzle_ID']]
                except Exception, e:
                    self.logger.debug("[error]", e, "\n[error]", d)
                    continue
            return list(set(puzzle_ids))


        # get unique puzzle ids, generate curl commands
        nids = get_puzzle_ids()
        cmd_list = [_format_curl(nid, nid_type='puznid') for nid in nids]
        self.logger.debug("puzzle nids = {}".format(nids))
        self.logger.debug("\n" + pprint.pformat(cmd_list))

        # process results
        results_async = util.map_async(util.submit_command, cmd_list)
        data = util.load_json(results_async, async=True, 
            keys=['data','solutions'])
        self.logger.debug(pprint.pformat([__ for _ in data for __ in _][:2]))
        eterna_data = [map(_.get, col_names) for d in data for _ in d]
        #self.logger.debug(pprint.pformat(eterna_data))
        
        # write data to file, convert types for successful join 
        self.df = pd.DataFrame(eterna_data, columns=col_names) 
        self.df = self.df.convert_objects(convert_numeric=True)

        print self.df.dtypes
        print source_df.dtypes
        self.df = source_df.join(self.df.set_index('Design_ID'), 
                                  on='Design_ID', how='left')
        pprint.pprint(self.df)

        ### TODO: hacky... 
        def get_project_info(project_id):
            """
            """
            url = str("http://staging.eternagame.org/get/?type=project" + 
                      "&nid={}".format(project_id))
            curl_cmd = "curl '{}'".format(url)
            if self.options.verbose:
                print '[command]\t{}'.format(curl_cmd)

            o = util.submit_command(curl_cmd, verbose=False)
            d = json.loads(o, encoding='utf-8')
            project_name, project_round, puzzle_names = "", "", {}
            try:
                project = d['data']['lab']
                project_name = project['title'].encode('utf-8')
                project_round = project['puzzles'][0]['round']
                if "round" in project_name.lower():
                    project_round = project_name.lower().split('round ')[-1][0]
                    if 'Round' in project_name:
                        project_name = project_name.split('Round ')[0]
                    else: 
                        project_name = project_name.split('round ')[0]
                    # clean up project name
                    project_name = project_name.replace('(', '').strip()
                    if project_name.endswith('-'):
                        project_name = project_name[:-1].strip()
                
                puzzles = project['puzzles'][0]['puzzles']
                puzzle_names = dict(
                    (int(_['nid']), _['title']) for _ in puzzles)
                self.logger.debug("Project_Name: {}".format(project_name))
                self.logger.debug("Project_Round: {}".format(project_round))
                self.logger.debug("Project_Puzzles: {}".format(
                    pprint.pformat(puzzle_names)))

                try:
                    self.logger.debug(pprint.pformat(project['puzzles']))
                    for puzzles in project['puzzles']:
                        self.logger.debug("Puzzle Round =" + 
                            pprint.pformat(puzzles['round']))
                        for puzzle in puzzles['puzzles']:
                            self.logger.debug(pprint.pformat(puzzle))
                            n_states = puzzle['constraints'].count('SHAPE')
                            self.logger.debug("N States = " + str(n_states))
                except Exception as e:
                    self.logger.error(e)
                
            except Exception as e:
                self.logger.error(e)
            try:
                project_name, project_round
            except Exception as e:
                self.logger.error(e)

            self.logger.debug("Project_Info\n(" + 
                              "Project_ID: {},\nProject_Name: {}\nProject_Round: {}\nPuzzle_Names: | {} |)".format(
                              project_id, project_name, project_round, ' | '.join(map(str, puzzle_names.values()))))
            return (project_name, project_round, puzzle_names)
                  
      
        groups = self.df.groupby(['Project_ID', 'Project_Round']).groups
        logger.debug(groups.keys())

        self.df['Project_Name'] = self.df.Project_ID.map(lambda x: '')
        for idx, (gkey, groupindex) in enumerate(groups.iteritems()):
            groupmask = groupindex
            logger.debug("{}, {}, {}".format(idx,gkey,
                groupmask[:min(5, len(groupmask))]))
            project_info = get_project_info(gkey[0])
            logger.debug(project_info)
            logger.debug(len(self.df[self.df.index.isin(groupindex)]))
            try:
                logger.debug(self.df[self.df.index.isin(groupindex)].head())
                self.df.Project_Name[self.df.index.isin(groupindex)] = project_info[0]
                self.df.Project_Round[self.df.index.isin(groupindex)] = project_info[1]
                self.df.Puzzle_Round[self.df.index.isin(groupindex)] = project_info[1]
                logger.debug(self.df[self.df.index.isin(groupindex)].head())
            
            except Exception as e:
                logger.error(e)
                logger.debug('iterating groupindex: {}'.format(groupindex))
                for gidx in groupindex:
                    logger.debug("df[gidx={}]: {}"
                                 .format(gidx,pprint.pformat(self.df[gidx])))
                    self.df.Project_Name[gidx] = project_info[0] #self.df.Project_ID.map(lambda x: '')
                    self.df.Project_Round[gidx] = project_info[1]
                    self.df.Puzzle_Round[gidx] = project_info[1] 
            

        logger.debug(self.df.head())
        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df



    def collect_meta(self):
        """
        """
       
        ### TODO: hacky... 
        def get_nstates(project_id):
            """
            """
            url = str("http://staging.eternagame.org/get/?type=project" + 
                      "&nid={}".format(project_id))
            curl_cmd = "curl '{}'".format(url)
            self.logger.debug(curl_cmd)

            o = util.submit_command(curl_cmd, verbose=False)
            d = json.loads(o, encoding='utf-8')

            n_states = {}
            try:
                project = d['data']['lab']
                puzzles = project['puzzles'][0]['puzzles']
                for puz in puzzles:
                    n_states[puz['nid']] = puz['constraints'].count('SHAPE')
                self.logger.debug("N States = " + str(n_states))                
            except Exception as e:
                self.logger.error(e)
            return n_states
     
     


        source_df = util.load_dataframe(
            self.options.source).set_index('Puzzle_ID')
        puzzle_ids = list(set(source_df.index))

        n_states = get_nstates(get_unique(source_df['Project_ID']).pop())

        puzzles = []
        for puzzle_id in puzzle_ids:  
            puzzles_df = source_df.ix[puzzle_id]
            state_count = '2'
            if n_states:
                state_count = str(n_states[puzzle_id])

            def get_unique(s):
                if isinstance(s, pd.Series):
                    return s.iloc[0]
                if isinstance(s, list):
                    return s.pop(0)
                return s


            puzzles.append([get_unique(puzzles_df['Project_ID']).pop(),
                            get_unique(puzzles_df['Project_Name']).pop(),
                            get_unique(puzzles_df['Puzzle_Name']).pop(),
                            puzzle_id, len(puzzles_df), state_count])

        new_header = ['Project_ID',
                      'Project_Name',
                      'Puzzle_Name',
                      'Puzzle_ID',
                      'Design_Count',
                      'State_Count']
        self.df = pd.DataFrame(puzzles, columns=new_header) 

        # HACKY: ensure ids are ints
        int_cols = ["Project_ID", "Puzzle_ID"]
        for int_col in int_cols:
            self.df[int_col] = self.df[int_col].map(
                lambda _: "{:.0f}".format(_) if '.' in str(_) else _)

        if self.options.outfile:
            util.write_dataframe(self.df, self.options.outfile)
        return self.df 



    def upload_csv_files_to_fusion(self):
        ft_util.upload_csv(self.options.tableID, self.options.source)
        return True


###############################################################################
### main script
###############################################################################
if __name__=="__main__":

    # script-specific imports
    import argparse

    # define func_map
    func_map = {'PROCESS': 'process_data', 'GET': 'get_columns', 
                'VERIFY': 'verify_columns', 'FIXUP': 'fixup_columns', 
                'INSERT': 'insert_columns', 'TRANSFER': 'transfer_columns', 
                'QUERY': 'query_eterna_data', 'META': 'collect_meta', 
                'UPLOAD': 'upload_csv_files_to_fusion'}
    

    ### TODO
    parser = argparse.ArgumentParser(
        description='Automated Eterna Fusion Tables Data Curation')

    parser.add_argument('mode',
                        help="valid modes: {}".format(func_map.keys()))

    parser.add_argument('--source', nargs='*', default=None)
    parser.add_argument('--target', default=None)
    parser.add_argument('--outfile', default=None)

    parser.add_argument('--column-file', dest='column_file', default=None) 
    parser.add_argument('--cloud-round', dest='cloud_round', default=None)

    parser.add_argument('--tableID', default=None)
    parser.add_argument('--token', default=None)
 
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dry', action='store_true')
    
    parser.add_argument('-l', '--log', default='INFO')
    args = parser.parse_args()
    
    # init log levels
    util.configure_logging(level=args.log.upper())

    ### modes...
    args.mode = args.mode.lower()
    fusion_util = FusionUtil(args)
    status = fusion_util.apply_mode()
    if args.verbose:
        print status 


 
