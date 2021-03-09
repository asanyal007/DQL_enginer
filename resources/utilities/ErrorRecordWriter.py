# writing error records to a file

# ErrorRecordWriter class
class ErrorRecordWriter:

    # ErrorRecordWriter init
    def __init__(self, filePath, separator):
        self.filePath = filePath
        self.separator = separator

    # ErrorRecordWriter writeerrorrecords function
    def writeerrorrecords(self, errorrecords):
        file = open(self.filePath, "w")
        for line in errorrecords:
            str1 = ''
            for word in line:
                str1 = str1+'\''+str(word)+'\','
            file.write(str1[:len(str1)-1]+'\n')
        file.close()

    # ErrorRecordWriter writefailedrules function
    def writefailedrules(self, failedrules):
        file = open(self.filePath, "w+")
        for rule in failedrules:
            file.write(rule+','+failedrules[rule][2]+','+failedrules[rule][4]+'\n')
        file.close()