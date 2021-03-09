#MYSQL Connector which implements mysql connection

import mysql.connector
from mysql.connector import Error

#mysqlconnector class
class mysqlConnector(sourceConnector):

    #mysql connection
    def getconnection(self):
      try:
        username = self.connection['user']
        password = self.connection['password']
        host = self.connection['host']
        database = self.connection['database']
        connection = mysql.connector.connect(host=host, database=database,user=username,password=password)
        return connection
      except Error as e:
        print("Error while connecting to MySQL", e)

