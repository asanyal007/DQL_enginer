# data validation by applying rules
import datetime
import re
from resources.logger.MyLogger import logger
from resources.utilities.ExceptionLog import ExceptionLog
from resources.constants import constants

# regular expressions
phoneregex = re.compile("^(\d{3})([-])(\d{3})([-])?(\d{4})?$")
zipregex = re.compile("^(\d{5})([- ])?(\d{4})?$")
ssnregex = re.compile("^(?!000|.+0{4})(?:\d{9}|\d{3}-\d{2}-\d{4})$")
piiregex = re.compile("^(?!000|.+0{4})(?:\d{9}|\d{3}-\d{2}-\d{4})$")
mailregex = re.compile("^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$")
dateregex = re.compile("^([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])(\.|-|/)([1-9]|0[1-9]|1[0-2])(\.|-|/)([0-9][0-9]|19[0-9][0-9]|20[0-9][0-9])$|^([0-9][0-9]|19[0-9][0-9]|20[0-9][0-9])(\.|-|/)([1-9]|0[1-9]|1[0-2])(\.|-|/)([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])$")
invalid = "Invalid "
errorlog = ''


# log function, logging messages
def log(message):
    #skip = ''
    logger.debug(message)


# pattern validation for phone numbers, zip code and ssn
def validatePattern(columnName, patterns, value):
    errorlog = ''
    flag = True
    for pattern in patterns:
        pattern1 = '^(pattern)$'
        nrv = ''
        for r in pattern:
            if r == '#':
                r = '\d'
            nrv = nrv + r
        nrv = pattern1.replace('pattern', nrv)
        m = re.search(re.compile(nrv), value)
        if m:
            flag = False
    if flag:
        errorlog = invalid+columnName

    return errorlog


# pattern validation for dates
def validatedatepattern(columnname, patterns, value):
    errorlog = ''
    flag = False
    for pattern in patterns:
        pattern = pattern.upper()
        pattern = pattern.replace('MMM', '%b').replace('MM', '%m').replace('DD', '%d').replace('YYYY', '%Y').replace('YY', '%y')
        try:
            datetime.datetime.strptime(str(value), pattern)
        except:
            flag = True
    if flag:
        errorlog = invalid+columnname
    return errorlog


# length function to validate pattern
def length(columnname, value, pattern):
    errorlog = ''
    if len(value) != int(pattern):
        errorlog = invalid+columnname
    return errorlog


# notNull function
def notNull(columnname, value):
    errorlog = ''
    if value == '' or value == None or len(value) == 0:
        errorlog = invalid+columnname
    return errorlog


# alpha numeric validation
def alphanumeric(columnname, value):
    if str(value).isalnum():
        errorlog = ''
    else:
        errorlog = invalid+columnname
    return errorlog


# numeric validation
def numeric(columnname, value):
    if str(value).isnumeric():
        errorlog = ''
    else:
        errorlog = invalid+columnname
    return errorlog


# decimal validation
def decimal(columnname, value):
    if str(value).isdecimal():
        errorlog = ''
    else:
        errorlog = invalid + columnname
    return errorlog


# email validation
def email(columnname, value):
    if re.search(mailregex, value):
        errorlog = ''
    else:
        errorlog = invalid+columnname

    return errorlog


# alphabates validation
def alphabates(columnname, value):
    if str(value).isalpha():
        errorlog = ''
    else:
        log("inside alphabates")
        errorlog = invalid+columnname
    return errorlog


# phone number validation
def phonenumber(columnname, value, pattern):
    if len(pattern) != 0:
        patterns = pattern.split(',')
        errorlog = validatePattern(columnname, patterns, value)
    else:
        if re.search(phoneregex, value):
            errorlog = ''
        else:
            errorlog = invalid+columnname

    return errorlog


# zipcode validation
def zipcode(columnname, value, pattern):
    if len(pattern) != 0:
        patterns = pattern.split(',')
        errorlog = validatePattern(columnname, patterns, value)
    else:
        if re.search(zipregex, value):
            errorlog = ''
        else:
            errorlog = invalid+columnname
    return errorlog


# ssn validation
def ssn(columnname, value, pattern):
    if len(pattern) != 0:
        patterns = pattern.split(',')
        errorlog = validatePattern(columnname, patterns, value)
    else:
        if re.search(ssnregex, value):
            errorlog = ''
        else:
            errorlog = invalid+columnname
    return errorlog


# date validation
def date(columnname, value, pattern):
    if len(pattern) != 0:
        patterns = pattern.split(',')
        errorlog = validatedatepattern(columnname, patterns, value)
    else:
        if re.search(dateregex, value):
            errorlog = ''
        else:
            errorlog = invalid+columnname
    return errorlog


# Rules class
class Rules:

    # Rules class inti function
    def __init__(self, rules):
        self.rules = rules
        self.errorLog = []

    #process rules
    def processrule(self, columnname, rule, value, pattern):
        #log(rule)
        try:
            rule = constants.supportedrules[rule]
        except:
            return ''
        if rule == 'PHONENUMBER':
            return phonenumber(columnname, value, pattern)
        elif rule == 'ZIPCODE':
            return zipcode(columnname, value, pattern)
        elif rule == 'SSN':
            return ssn(columnname, value, pattern)
        elif rule == 'DATE':
            return date(columnname, value, pattern)
        elif rule == 'NOTNULL':
            return notNull(columnname, value)
        elif rule == 'ALPHANUMERIC':
            return alphanumeric(columnname, value)
        elif rule == 'DECIMAL':
            return decimal(columnname, value)
        elif rule == 'EMAIL':
            return email(columnname, value)
        elif rule == 'NUMERIC':
            return numeric(columnname, value)
        elif rule == 'ALPHABATES':
            return alphabates(columnname, value)
        elif rule == 'LENGTH':
            return length(columnname, value, pattern)
        else:
            return ''

    # applying rules
    def applyrules(self, listofvalues):
        ruleresults = ''
        status = {}
        for key in self.rules.keys():
            rule = self.rules[key]
            columnname = rule[0]
            rulename = rule[1]
            pattern = rule[3]
            value = listofvalues[rule[5]]
            #log("rule: "+str(rule))
            try:
                ruleresult = self.processrule(columnname, rulename.strip(), value, pattern)
            except:
                # logging the exception and return the exception
                ExceptionLog().log()
                raise
            if len(ruleresult) != 0:
                    ruleresults = ruleresults + "" + ruleresult+"|"
                    status[key] = 'failed'
        return [ruleresults[:len(ruleresults)-1], status]
