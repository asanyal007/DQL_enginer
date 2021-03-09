# Reading input file and return header
from resources.logger.MyLogger import logger
from resources.utilities.ExceptionLog import ExceptionLog
import pandas as pd

# nan function
def is_nan(x):
    return (x != x)


# HeaderReader class
class HeaderReader:

    # HeaderReader init
    def __init__(self, filePath, separator):
        self.filePath = filePath
        self.separator = separator

    # HeaderReader getheader function
    def getheader(self):
        try:
            map_data = pd.read_csv(self.filePath, delimiter=self.separator)
            header = []
            for column in map_data.columns:
                header.append(column.upper())
            logger.info(header)
            return header
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise
