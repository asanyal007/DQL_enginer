# Rule columns, Header columns mapping
from resources.logger.MyLogger import logger
from resources.utilities.ExceptionLog import ExceptionLog


# RuleHeaderMapping class
class RuleHeaderMapping:

    # RuleHeaderMapping init function
    def __init__(self, header, rules):
        self.header = header
        self.rules = rules

    # RuleHeaderMapping getruleapplicableheaders function
    def getruleapplicableheaders(self):
        try:
            keys = self.rules.keys()
            #dict_keys(['BR1', 'BR2', 'BR3'])
            for key in keys:
                columnname = self.rules[key][0]
                headerindex = self.header.index(columnname)
                self.rules[key].append(headerindex)
            logger.debug(self.rules)
            return self.rules
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise