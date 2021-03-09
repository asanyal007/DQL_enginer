#configuration reader
import json

#config reader class
class configReader:
    def __init__(self, path, fileName):
        self.path = path
        self.fileName = fileName
        self.config = ""

    #read config function
    def readConfig(self):
        with open(self.fileName) as f:
            self.config = json.load(f)
        return self.config