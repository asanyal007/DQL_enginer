class HeaderReader:

    def __init__(self, filePath):
        self.filePath = filePath

    def readHeader(self):
        headerLine = 0
        header = True
        inputFile = open(self.filePath, "r")
        for line in inputFile:
            if header and headerLine == 0:
                headers = line[:len(line) - 1].split(",")
                break
            headerLine = headerLine + 1
        inputFile.close()
        return headers
