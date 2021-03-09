# logging the exception
from resources.logger.MyLogger import logger
import sys, os


# ExceptionLog class
class ExceptionLog:

    # Exception log
    def log(self):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        error_log = str(exc_type)+":"+str(fname)+":"+str(exc_tb.tb_lineno)+":"+str(exc_obj)
        logger.error(error_log)