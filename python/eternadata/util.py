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
import logging
logger = logging.getLogger(__name__)


###############################################################################
### globals
###############################################################################
__dir__ = os.path.dirname(inspect.getfile(inspect.currentframe()))


###############################################################################
### logging
###############################################################################
def configure_logging(**config):

    import pprint
    import logging

    # defaults
    loglevel = 'INFO'
    logformat = '%(name)s %(levelname)s %(message)s'
    if 'level' not in config:
        config['level'] = loglevel
    else:
        loglevel = config['level']
    if 'format' not in config:
        config['format'] = logformat
    else:
        logformat = config['format']

    # convert to numeric logging.LEVEL
    config['level'] = getattr(logging, config['level'].upper(), None)
    if not isinstance(config['level'], int):
        raise ValueError('Invalid log level: {}'.format(config['level']))
    
    # init config
    logging.basicConfig(**config)
    for level in range(0, config['level'], 10):
        logging.disable(level)

    logger = logging.getLogger( __name__ )
    getattr(logger, loglevel.lower())("initializing logging configuration")
    getattr(logger, loglevel.lower())("(config=\n{}".format(pprint.pformat(config)))

    

    try:
        import coloredlogs
        coloredlogs.DEFAULT_LOG_FORMAT = '%(programname)s %(name)s %(levelname)s %(message)s'
        for k,v in coloredlogs.DEFAULT_FIELD_STYLES.iteritems():
            coloredlogs.DEFAULT_FIELD_STYLES[k]['bold'] = True
        coloredlogs.install(level='DEBUG')
    except ImportError:
        logger.debug("for colored logging, run: pip install coloredlogs")


try:

    def colorize(logger, *extra): 
        '''
        :sweet coloring scheme
        @author calebgeniesse
        '''
        extra = map(list, extra)
        extra = [[_[0],_[1].format(*_[2])] if len(_) > 2 else _ for _ in extra]
    
        DEFAULT_FIELD_STYLES = coloredlogs.DEFAULT_FIELD_STYLES
        DEFAULT_LOG_FORMAT = coloredlogs.DEFAULT_LOG_FORMAT
        for i, (color, _) in enumerate(extra):
            key = extra[i][0] = '{}{}'.format(color, i)
            coloredlogs.DEFAULT_LOG_FORMAT += ' %({})s'.format(key)
            coloredlogs.DEFAULT_FIELD_STYLES[key] = {'color':color.lower(), 
                                                     'bold':color.isupper()}
        coloredlogs.install(level='DEBUG')
        logger('', extra=dict(extra))
        
        # reinstall defaults
        coloredlogs.DEFAULT_FIELD_STYLES = DEFAULT_FIELD_STYLES
        coloredlogs.DEFAULT_LOG_FORMAT = DEFAULT_LOG_FORMAT
        coloredlogs.install(level='DEBUG')
 
except:

    def colorize(logger, *extra):
        logger(' '.join([_[1] for _ in extra]))


###############################################################################
### utility helpers
###############################################################################
def dataframe_loader(fn, **params):
    """
    """
    # return generator of load_dataframe calls
    if not isinstance(fn, list):
        fn = [fn]
    for fn_i in fn:
        yield load_dataframe(fn_i, **params) 


def load_dataframe_from_files(fn, df=pd.DataFrame(), **params):
    """
    """ 
    # append dataframes to df, as loaded
    for df_i in dataframe_loader(fn, **params):
        df = df.append(df_i, ignore_index=True)
    return df


def load_dataframe(fn, sheet = 'Sheet1', **params):
    """
    Inputs: 
        + single file name, or list of file names
    
    Outputs:
        + pandas dataframe
    """
    if isinstance(fn, list):
        # return single combined dataframe 
        df = load_dataframe_from_files(fn, sheet=sheet, **params)
        
        # group duplicate columns resulting from rename
        logger.warning("grouping DataFrame by columns")
        logger.warning("# columns before grouping: {}".format(len(df.columns)))
        df = df.groupby(df.columns, axis=1).first()
        logger.warning("# columns after grouping: {}".format(len(df.columns)))
   
        return df


    elif '*' in fn:
        # glob for files if * in fn
        return load_dataframe_from_files(glob.glob(fn), sheet=sheet, **params)
         
    else:
        # load dataframe for a single file
        df = None
        if ':' in fn:
            [fn, sheet] = fn.split(':')
   
        if 'verbose' not in params:
            params['verbose'] = False
    
     
        if any(fn.endswith(_) for _ in ['.csv','.tsv']):
            if 'na_filter' not in params:
                params['na_filter'] = False
            
            if 'low_memory' not in params:
                params['low_memory'] = False

            if 'mangle_dupe_cols' not in params:
                params['mangle_dupe_cols'] = True #False
 
       
 
        if fn.endswith('.csv'):
            df = pd.read_csv(fn, **params)
        elif fn.endswith('.tsv'):
            df = pd.read_csv(fn, delimiter='\t', **params)
        elif fn.endswith('.xlsx') or fn.endswith('.xls'):
            df = pd.read_excel(fn, sheet=sheet, **params)

        return df



def write_dataframe(df, fn, inplace=True, index=False, quoting=csv.QUOTE_ALL):
    if index is False:
        df.reset_index(inplace=True, drop=True)
    #try:
    #    df.to_csv(fn, inplace=inplace, index=index, quoting=quoting)
    #except UnicodeEncodeError as e:
    df.to_csv(fn, inplace=inplace, index=index, quoting=quoting, encoding='utf-8')
    return True


def submit_command(cmd, verbose=False):
    logger.info("submitting command ...")
    logger.debug("{}".format(cmd))
    o, e = sp.Popen(
        cmd, shell=True, 
        stderr=sp.PIPE, stdout=sp.PIPE
    ).communicate()
    if verbose is True:
        logger.info(pprint.pformat(o), pprint.pformat(e))
    logger.info("comand finished.")
    return o


def map_async(func, args):
    """
    """
    ### init multiprocessing                 
    logger.info("mapping async ...")                                   
    mp_pool = mp.Pool(64)#mp.cpu_count() - 1)
    async_result = mp_pool.map_async(func, args)
    mp_pool.close()
    mp_pool.join()
    return async_result


def load_json(jsondata, async=False, keys=[]):
    """
    """
    data = []
    if async is True:   
        for d in jsondata.get():
            d = json.loads(d.replace('\\r', ''))
            try:
                for key in keys:
                    d = d[key]
            except:            
                pass
            data.append(d)
    else:
        for idx, d in enumerate(jsondata):
            d = json.loads(d.replace('\\r', ''))
            try:
                for key in keys:
                    d = d[key]
            except:            
                pass
            data.append(d)
    return data


    
###############################################################################
### scripting
###############################################################################
if __name__=='__main__':
    pass
