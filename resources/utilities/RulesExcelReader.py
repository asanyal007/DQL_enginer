# Reading an excel file using Python
#import xlrd
#import openpyxl
from pathlib import Path

import pandas as pd
import numpy as np


class RulesExcelReader:

    def __init__(self, filePath):
        self.filePath = filePath

    def getRules(self):
        map_data = pd.read_csv(self.filePath)
        ruls = {}
        rule_list = []
        rule_number_list = []
        rule_desc_list = []
        total = ''
        for index, val in map_data.iterrows():
            ruls[val['Value_Field']] = val['Value_Check']
            rule_list.append(ruls)
            rule_number_list.append(val['Rule'])
            rule_desc_list.append(val['Rule_Description'])
            total = [rule_list, rule_number_list, rule_desc_list]
            ruls = {}

        return total
        # To open Workbook
        #wb = xlrd.open_workbook(self.filePath)
        #sheet = wb.sheet_by_index(0)

        # For row 0 and column 0
        #print(sheet.cell_value(0, 0))