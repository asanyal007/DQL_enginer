# writing error records to a file

# ErrorRecordWriter class
class ErrorRecordWriter:

    # ErrorRecordWriter init
    def __init__(self, filePath, separator, batch_info):
        self.filePath = filePath
        self.separator = separator
        self.batch_info = batch_info

    # ErrorRecordWriter writeerrorrecords function
    def writeerrorrecords_file(self, errorrecords):
        #print(errorrecords)
        for rule in errorrecords:
            file = open(self.filePath+"_"+str(rule), "w")
            for line in errorrecords[rule]:
                str1 = ''
                for word in line:
                    str1 = str1+'\''+str(word)+'\','
                file.write(str1[:len(str1)-1]+'\n')
        file.close()

    def writeerrorrecords(self, error_records):
        #print(error_records)
        postgres_conn = self.batch_info['postgres_conn']
        for rule in error_records:
            failed_records = error_records[rule]
            if len(failed_records) > 1:
                self.batch_info['rule_id'] = rule
                postgres_conn.insert_error_records(self.batch_info, failed_records)

    # ErrorRecordWriter writefailedrules function
    def writefailedrules_file(self, failedrules):
        file = open(self.filePath, "w+")
        for rule in failedrules:
            file.write(str(rule)+','+failedrules[rule][2]+','+failedrules[rule][4]+'\n')
        file.close()

    def writefailedrules(self, rule_records):
        #print(error_records)
        postgres_conn = self.batch_info['postgres_conn']
        for rule in rule_records:
            rule_record = rule_records[rule]
            self.batch_info['rule_id'] = rule
            postgres_conn.insert_rule_records(self.batch_info, rule_record)