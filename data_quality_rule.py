# main program, program will be start from this main program
from resources.utilities.HeaderReader import HeaderReader
from resources.utilities.RuleHeaderMapping import RuleHeaderMapping
from resources.utilities.RulesExcelReader1 import RulesExcelReader
from resources.DQ.FileDQ import FileDQ
from resources.logger.MyLogger import logger
import data_util

# DQ rules file
rulesexcel = "resources/rule3.csv"

# comma delimited file for DQ check
commafilepath: str = "resources/demodatacomma.csv"

#file delimiter
separator = ','

def dqmain(sourceinfo):
    logger.info("dq main program started................")

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
    rules = RulesExcelReader(rulesexcel).getrules()
    rules = rules['items']

    logger.info("dq main program config rules parsed successfully................")

    # all the header columns in the input file:
    # header = ['CustomerID', 'CustomerName', 'Addressline1', 'Addressline2', 'City', 'State', 'Zipcode', 'Phonenumber']
    snf_conn = data_util.Snowflake(sourceinfo['username'], sourceinfo['password'], sourceinfo['host'])
    data = snf_conn.read_header(sourceinfo['database'], sourceinfo['schema'], sourceinfo['table'])
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
    FileDQ(data, rulecolumnindex, separator).applyDQ()

    logger.info("dq main program ended................")

if __name__ == "__main__":
    sourceinfo = {'sourcetype': 'snowflake', 'host': 'dt21316.us-central1.gcp', 'username': 'WNS123', 'password': 'Nq1dRuaV',  'database': 'SNOWFLAKE_SAMPLE_DATA', 'schema': 'TPCDS_SF10TCL', 'table': 'ITEM'}
    #sourceinfo = {'sourcetype': 'filesystem', 'filename': commafilepath, 'separator': separator}
    dqmain(sourceinfo)