# main program, program will be start from this main program
from resources.utilities.HeaderReader import HeaderReader
from resources.utilities.RuleHeaderMapping import RuleHeaderMapping
from resources.utilities.RulesExcelReader1 import RulesExcelReader
from resources.DQ.FileDQ import FileDQ
from resources.logger.MyLogger import logger


# DQ rules file
rulesexcel = "resources/rule3.csv"

# comma delimited file for DQ check
commafilepath: str = "resources/demodatacomma.csv"

#file delimiter
separator = ','

def dqmain():
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
    rules = rules['file2']

    logger.info("dq main program config rules parsed successfully................")

    # all the header columns in the input file:
    # header = ['CustomerID', 'CustomerName', 'Addressline1', 'Addressline2', 'City', 'State', 'Zipcode', 'Phonenumber']
    header = HeaderReader(commafilepath, separator).getheader()

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
    FileDQ(commafilepath, rulecolumnindex, separator).applyDQ()

    logger.info("dq main program ended................")

if __name__ == "__main__":
    dqmain()