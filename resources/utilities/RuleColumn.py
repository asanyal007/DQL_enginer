class RuleColumn:

    def __init__(self, header, rules):
        self.header = header
        self.rules = rules[0]

    def ruleApplicableHeaders(self):
        columnIndexs = []
        rulesList = []
        columnNames = []
        index = 0
        for x in self.header:
            #print(" rules -->"+str(self.rules))
            #print(x)
            for y in self.rules:
                #print(y)
                try:
                    #print(" specific --> " + str(y[x]))
                    if y[x] != '':
                        columnIndexs.append(index)
                        rulesList.append(y[x])
                        columnNames.append(x)
                        self.rules.remove(y)
                        break
                except:
                    print("skip")
            index = index + 1
        return [columnIndexs, rulesList, columnNames]