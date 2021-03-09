class connectors:

    def __init__(self, stype, connectionDetails):
        self.connectionDetails = connectionDetails
        self.stype = stype

    # source connectors function
    def getSourceConnectors(self):
        if self.stype == constants.MYSQL:
          return mysqlConnector(self.connectionDetails).getconnection()
        elif self.stype == constants.TEXTFILE:
          return fileConnector(self.connectionDetails).getconnection()
        elif self.stype == constants.API:
          return apiConnector(self.connectionDetails).getconnection()