# Reading configured rules from csv file using pandas
from resources.logger.MyLogger import logger
from resources.utilities.ExceptionLog import ExceptionLog
import pandas as pd
from resources.constants import constants


# is nan check
def is_nan(x):
    return (x != x)


# RulesExcelReader class
class RulesExcelReader:

    # RulesExcelReader class init
    def __init__(self, filepath, batch_id, postgre_alchemy):
        self.filepath = filepath
        self.batch_id = batch_id
        self.postgre_alchemy = postgre_alchemy

    # RulesExcelReader class getrules, parse the csv file and returning the dictionary of rules
    def get_rules(self):
        rules = self.postgre_alchemy.fetch_rules(self.batch_id)
        rules_result = {}
        rl_df = pd.DataFrame(rules)
        for index, val in rl_df.iterrows():
            ruleid = val[0]
            ruledesc = val[1]
            rule = val[3].upper()
            columnname = val[2].upper()
            rulepattern = val[5]
            try:
                rule = constants.supportedrules[rule]
                if is_nan(rulepattern):
                    pattern = ''
                else:
                    pattern = rulepattern.strip()
                rule1 = [columnname, rule, ruledesc, pattern, 'passed']
                rules_result[ruleid] = rule1
            except:
                logger.info('rule: ' + ruleid + '-->' + rule + ' not supported')
        logger.debug(rules_result)
        return rules_result

    def getrules(self):
        try:
            map_data = pd.read_csv(self.filepath)
            rules = {}
            catrules = {}
            for index, val in map_data.iterrows():
                ruleid = val['Rule']
                rule = val['Value_Check'].upper()
                columnname = val['Value_Field'].upper()
                ruledesc = val['Rule_Description']
                rulepattern = val['Pattern']
                cat = val['source_name']
                try:
                    rule = constants.supportedrules[rule]
                    if is_nan(rulepattern):
                        pattern = ''
                    else:
                        pattern = rulepattern.strip()
                    rule1 = [columnname, rule, ruledesc, pattern, 'passed']
                    try:
                        if len(catrules[cat]) != 0:
                            tempcatrule = catrules[cat]
                            tempcatrule[ruleid] = rule1
                    except:
                        rules[ruleid] = rule1
                        catrules[cat] = rules
                    rules = {}
                except:
                    logger.info('rule: '+ruleid+'-->'+rule+' not supported')
            logger.debug(catrules)
            return catrules
        except:
            # logging the exception and return the exception
            ExceptionLog().log()
            raise
