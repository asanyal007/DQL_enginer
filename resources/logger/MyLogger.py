# MyLogger class to log the info, debug and errors
import logging

filePath = "resources/mylog.log"

logger = logging.getLogger("mylogger")
logger.setLevel(level=logging.INFO)
logger.setLevel(level=logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler(filePath)
fh.setLevel(level=logging.INFO)
fh.setLevel(level=logging.DEBUG)
fh.setFormatter(formatter)
# reate console handler for logger.
ch = logging.StreamHandler()
ch.setLevel(level=logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(fh)

logger.addHandler(ch)