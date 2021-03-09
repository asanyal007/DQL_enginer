# source connector interface
class sourceConnector(object):

    def __init__(self, connection):
        self.connection = connection

    # abstract get connection function
    def getconnection(self):
        pass