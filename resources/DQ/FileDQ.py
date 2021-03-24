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
    def __init__(self, dataframe, rulelist, separator, batch_info):
        self.dataframe = dataframe
        self.rules = rulelist
        self.separator = separator
        self.batch_info = batch_info

    def applyDQ(self):

        # variable declaration and initialization
        recordcount = 0
        failedrecordcount = 0
        header = True
        statistics = {}
        failed_rule_records = {}

        try:
            #inputfile = open(self.filePath, "r")
            #df_chunk = pd.read_csv(self.filePath)
            df_chunk = self.dataframe.replace(np.nan, '', regex=True)
            #csvreader = csv.reader(inputfile, delimiter=self.separator)
            if header:
                headerline = []
                for column in df_chunk.columns:
                    headerline.append(column.upper())

                for rul in self.rules:
                    failed_rule_records[rul] = [headerline]

            # initializing the Rules class
            rule = Rules(self.rules)
            #print(failed_rule_records)
            # looping all the records one by one from input file
            for line in df_chunk.values:
                # applying the rules
                #print(line)
                recordcount = recordcount + 1
                result = rule.applyrules(line)
                #logger.debug("rules result  ---> "+str(result))

                try:
                    # checking any of the applied rule is failed or not
                    if result[0].strip() != '':
                        failedrecordcount = failedrecordcount + 1
                        # appending the error message
                        line = np.append(line, result[0])

                        for key in result[1].keys():
                            self.rules[key][4] = 'failed'
                            temp = failed_rule_records[key]
                            temp.append(list(line))
                            failed_rule_records[key] = temp
                            #print(failed_rule_records)
                except:
                    # logging the exception and return the exception
                    ExceptionLog().log()
                    raise

            statistics['totalRecords'] = recordcount
            statistics['errorRecordCount'] = failedrecordcount
            # writing the error records to a file
            errorrecwriter1 = ErrorRecordWriter(outputFile, self.separator, self.batch_info)
            errorrecwriter1.writeerrorrecords(failed_rule_records)
            # writing the rules status
            errorrecwriter2 = ErrorRecordWriter(validationFile, self.separator, self.batch_info)
            errorrecwriter2.writefailedrules(self.rules)
            logger.info(statistics)
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise