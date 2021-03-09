import json

def notNull(columnName, value):
    errorLog = ''
    if value == '' or value == None or len(value) == 0:
        errorLog = columnName + "  is "+value+"null"
    #print(errorLog)
    #print("================================22222222222222")
    return errorLog

def alphanumeric(columnName, value):
    errorLog = ''
    if value == '' or value == None or len(value) == 0:
        #print("================================22222222222222333")
        errorLog = columnName + "  is "+value+"null"
    return errorLog

def isnumeric(columnName, value):
    errorLog = ''
    if value == '' or value == None or len(value) == 0:
        #print("================================22222222222222444444444")
        errorLog = columnName + "  is "+value+"null"
    return errorLog

def lenghtcheck(columnName, value):
    errorLog = ''
    if (len(str(value)) == 5 or len(str(value)) == 10):
        print("valid")
    else:
        #print("================================22222222222222444444444")
        errorLog = "  invalid zipcode"
    return errorLog

def phopnenumercheck(columnName, value):
    errorLog = ''
    if len(str(value)) != 10:
        #print("================================22222222222222444444444")
        errorLog = "  invalid phone number"
    return errorLog

class Rules:

    def __init__(self, rules, ruleids, ruledesc):
        self.rules = rules
        self.ruleids = ruleids
        self.ruledescriptions = ruledesc
        self.errorLog = []

    def processRule(self, columnName, rule, value):
        #print(rule)
        rule = rule.upper()
        #print(rule)
        if rule == 'NOTNULL':
            #print("==============11111")
            return notNull(columnName, value)
        elif rule == 'ALPHANUMERIC':
            return alphanumeric(columnName, value)
        elif rule == 'DOUBLE':
            return isnumeric(columnName, value)
        elif rule == 'LENGTH':
            return lenghtcheck(columnName, value)
        elif rule == 'PHONENUMBER':
            return phopnenumercheck(columnName, value)

    def applyRules(self, listOfValues, ruleApplicableColumnNames):
        ruleResults = ''
        status = {}
        for i in range(len(self.rules)):
            #print(" i -----> "+str(i))
            ruleid = self.ruleids[i]
            ruledescription = self.ruledescriptions[i]
            rules = self.rules[i].split(',')
            #print(rules)
            value = listOfValues[i]
            columnName = ruleApplicableColumnNames[i]
            if len(rules) > 1:
                for rule in rules:
                    ruleResult = self.processRule(columnName, rule.strip(), value)
                    #print(value + " ===============444=================== " + columnName + ":" + str(ruleResult) + ":")
                    if len(str(ruleResult)) != 0:
                        #print(value + " " + columnName)
                        ruleResults = ruleResults + " " + ruleResult
                        status[ruleid] = 'failed'
            else:
                ruleResult = self.processRule(columnName, rules[0].strip(), value)
                #print(value + " ====================5555============== " + columnName + ":" + str(ruleResult) + ":")
                if len(ruleResult) != 0:
                    #print(value+" ====================6666============== "+columnName+":"+str(ruleResult)+":")
                    ruleResults = ruleResults + " " + ruleResult
                    status[ruleid] = 'failed'
        return [ruleResults, status]
