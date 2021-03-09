# Data Quality Check
from resources.logger.MyLogger import logger
import csv
from resources.utilities.Rules import Rules
from resources.utilities.ErrorRecordWriter import ErrorRecordWriter
from resources.utilities.ExceptionLog import ExceptionLog
import pandas as pd
import numpy as np


outputFile = "resources/errorrecords.csv"
validationFile = "resources/status.csv"


# FileDQ class for data quality checking
class FileDQ:

    #FileDQ init function
    def __init__(self, filepath, rulelist, separator):
        self.filePath = filepath
        self.rules = rulelist
        self.separator = separator

    def applyDQ(self):

        # variable declaration and initialization
        recordcount = 0
        failedrecordcount = 0
        errorrecords = []
        header = True
        statistics = {}

        try:
            #inputfile = open(self.filePath, "r")
            df_chunk = pd.read_csv(self.filePath)
            df_chunk = df_chunk.replace(np.nan, '', regex=True)
            #csvreader = csv.reader(inputfile, delimiter=self.separator)
            if header:
                headerline = []
                for column in df_chunk.columns:
                    headerline.append(column.upper())
                errorrecords.append(headerline)

            # initializing the Rules class
            rule = Rules(self.rules)

            # looping all the records one by one from input file
            for line in df_chunk.values:
                # applying the rules
                print(line)
                recordcount = recordcount + 1
                result = rule.applyrules(line)
                logger.debug("rules result  ---> "+str(result))

                try:
                    # checking any of the applied rule is failed or not
                    if result[0].strip() != '':
                        failedrecordcount = failedrecordcount + 1
                        # appending the error message
                        line = np.append(line, result[0])
                        errorrecords.append(line)
                        for key in result[1].keys():
                            self.rules[key][4] = 'failed'
                except:
                    # logging the exception and return the exception
                    ExceptionLog().log()
                    raise

            statistics['totalRecords'] = recordcount
            statistics['errorRecordCount'] = failedrecordcount
            # writing the error records to a file
            errorrecwriter1 = ErrorRecordWriter(outputFile, self.separator)
            errorrecwriter1.writeerrorrecords(errorrecords)
            # writing the rules status
            errorrecwriter2 = ErrorRecordWriter(validationFile, self.separator)
            errorrecwriter2.writefailedrules(self.rules)
            logger.info(statistics)
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise