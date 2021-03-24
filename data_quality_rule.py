# main program, program will be start from this main program
from resources.utilities.HeaderReader import HeaderReader
from resources.utilities.RuleHeaderMapping import RuleHeaderMapping
from resources.utilities.RulesExcelReader1 import RulesExcelReader
from resources.DQ.FileDQ import FileDQ
from resources.logger.MyLogger import logger
import data_util
import pandas as pd
from resources.utilities.ExceptionLog import ExceptionLog

# DQ rules file
rulesexcel = "resources/rule3.csv"

# comma delimited file for DQ check
commafilepath: str = "resources/demodatacomma.csv"

#file delimiter
separator = ','

batch_info = {}

def dqmain(batch_id, postgres_conn):
    logger.info("dq main program started................")
    try:
        files = postgres_conn.get_config_info(batch_id)
        dq_files = pd.DataFrame(files)
        logger.debug(dq_files)
        for ind, dq_file in dq_files.iterrows():
            #logger.debug(dq_file)
            SOURCE_ID = dq_file[2]
            TABLE_FILE_NAME = dq_file[3]
            SOURCE_TYPE = dq_file[4]
            SOURCE_HOST = dq_file[5]
            SOURCE_DATABASE = dq_file[6]
            SOURCE_USERNAME = dq_file[7]
            SOURCE_PASSWORD = dq_file[8]
            schema = 'TPCDS_SF10TCL'

            batch_info['batch_id'] = batch_id
            batch_info['run_id'] = 1
            batch_info['TABLE_FILE_NAME'] = TABLE_FILE_NAME
            batch_info['postgres_conn'] = postgres_conn

            """
            rules ={
                "filename1": {
                    'BR1': ['CustomerID', 'NotNull AlphaNumeric', 'Invalid Customer ID Check', ''],
                    'BR2': ['Zipcode', 'zipCode', 'Zip Code Validation', '']
                },
                "filename2": {
                    'BR3': ['Phonenumber', 'phoneNumber', 'Phonenumber validation', '##########']
                }
            }
            """
            # DQ rules config reading
            rules = RulesExcelReader(rulesexcel, batch_id, postgres_conn).get_rules()
            #rules = rules['items']

            logger.info("dq main program config rules parsed successfully................")

            # all the header columns in the input file:
            # header = ['CustomerID', 'CustomerName', 'Addressline1', 'Addressline2', 'City', 'State', 'Zipcode', 'Phonenumber']
            snf_conn = data_util.Snowflake(SOURCE_USERNAME, SOURCE_PASSWORD, SOURCE_HOST)
            data = snf_conn.read_snowflake_str(SOURCE_DATABASE, schema, TABLE_FILE_NAME)
            header = HeaderReader(data).getheader()

            logger.info("main program header parsed successfully from file................")

            """
            rulecolumnindex = {
                'BR1': ['CUSTOMERID', 'NOTNULL ALPHANUMERIC', 'Invalid Customer ID Check', '', 'passed', 0], 
                'BR2': ['ZIPCODE', 'ZIPCODE', 'Zip Code Validation', '', 'passed', 6], 
                'BR3': ['PHONENUMBER', 'PHONENUMBER', 'Phonenumber validation', '##########', 'passed', 7]
            }
            """
            rulecolumnindex = RuleHeaderMapping(header, rules).getruleapplicableheaders()

            logger.info("dq main program rules and headers mapped successfully................")

            # start DQ check
            FileDQ(data, rulecolumnindex, separator, batch_info).applyDQ()

            logger.info("dq main program ended................")
    except:
        ExceptionLog().log()


if __name__ == "__main__":
    sourceinfo = {'sourcetype': 'snowflake', 'host': 'dt21316.us-central1.gcp', 'username': 'WNS123', 'password': 'Nq1dRuaV',  'database': 'SNOWFLAKE_SAMPLE_DATA', 'schema': 'TPCDS_SF10TCL', 'table': 'ITEM'}
    #sourceinfo = {'sourcetype': 'filesystem', 'filename': commafilepath, 'separator': separator}
    conn_type = 'postgresql'
    user = 'postgres'
    password = 'password'
    host = 'localhost'
    db = 'postgres'
    port = '5432'
    postgre_conn = data_util.SQLAlchemy(conn_type, user, password, host, db)

    dqmain(1, postgre_conn)