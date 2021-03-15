# Reading input file and return header
from resources.logger.MyLogger import logger
from resources.utilities.ExceptionLog import ExceptionLog
import pandas as pd
import data_util


# nan function
def is_nan(x):
    return (x != x)


# HeaderReader class

def getheadermap(sourceinfo):
    sourcetype = sourceinfo['sourcetype']
    if sourcetype == 'filesystem':
        return pd.read_csv(sourceinfo['filename'], delimiter=sourceinfo['separator'])
    elif sourcetype == 'snowflake':
        snf_conn = data_util.Snowflake(sourceinfo['username'], sourceinfo['password'], sourceinfo['host'])
        return snf_conn.read_header(sourceinfo['database'], sourceinfo['schema'], sourceinfo['table'])


class HeaderReader:

    # HeaderReader init
    def __init__(self, data):
        self.data = data

    # HeaderReader getheader function
    def getheader(self):
        try:
            header = []
            for column in self.data.columns:
                header.append(column.upper())
            logger.info(header)
            return header
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise