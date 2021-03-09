class HeaderReader1:

# init
    def __init__(self, filePath, separator):
        self.filePath = filePath
        self.separator = separator

# reading the header from the input file
    def readHeader(self):
        headerLine = 0
        header = True
        # establishing the connection to input file
        inputFile = open(self.filePath, "r")
        # parsing all the lines
        for line in inputFile:
            if header and headerLine == 0:
                headers = line[:len(line) - 1].split(self.separator)
                break
            headerLine = headerLine + 1
        inputFile.close()
        return headers
