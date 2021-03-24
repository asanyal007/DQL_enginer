from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import snowflake.connector
import psycopg2

class SQLAlchemy:
    def __init__(self, conn_type, user, password, host, db):
        conn_str = '{conn_type}://{user}:{password}@{host}/{db}'.format(conn_type=conn_type,
                                                                        user=user, password=password,
                                                                        host=host, db=db)
        self.engine = create_engine(conn_str, echo=False)


    def get_ddl(self, data, tablename):
        ddl = pd.io.sql.get_schema(data, tablename).replace('\n','')
        return ddl

    def to_sql(self, data, tablename, dtype, insert_typ):
        if dtype:
            data.to_sql(tablename, con=self.engine, if_exists=insert_typ, dtype= dtype, index=False)
        else:
            data.to_sql(tablename, con=self.engine, if_exists=insert_typ, index=False)

    def read_sql(self, table, chunksize):
        if chunksize:
            data = pd.read_sql_table(table, con=self.engine, chunksize=chunksize)
        else:
            data = pd.read_sql_table(table, con=self.engine)
        return data

    def create_tables(self, ddl):
        try:
            f = open(ddl, "r")
            ddl_txt = f.read()
        except Exception as e:
            print(e)
        try:
            self.engine.execute(ddl_txt)
        except Exception as e:
            print(e)


    def getbatchid(self):
        postgres_sequence = """ SELECT nextval('serial') """
        try:
            batch_id_result = self.engine.execute(postgres_sequence)
            result = batch_id_result.fetchone();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def get_batch_audit(self, batchid):
        postgres_batchinfo = """SELECT BATCH_ID, RUN_ID, SOURCE_ID, LOGGEDIN_USER_NAME, TABLE_FILE_NAME, 
        PROFILE_STATUS, DQ_STATUS, START_DATETIME, PROFILE_END_DATETIME, DQ_END_DATETIME 
        FROM BATCH_EXECUTION_AUDIT WHERE BATCH_ID =  %s"""
        try:
            batch_info_result = self.engine.execute(postgres_batchinfo, (batchid))
            result = batch_info_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def get_config_info(self, batchid):
        postgres_batchinfo = """SELECT BEA.BATCH_ID, BEA.RUN_ID, BEA.SOURCE_ID, BEA.TABLE_FILE_NAME, CONF.SOURCE_TYPE,
        CONF.SOURCE_HOST, CONF.SOURCE_DATABASE, CONF.SOURCE_USERNAME, CONF.SOURCE_PASSWORD  
        FROM BATCH_EXECUTION_AUDIT BEA, CONFIG CONF WHERE BEA.BATCH_ID =  %s AND BEA.SOURCE_ID = CONF.SOURCE_ID"""
        try:
            batch_info_result = self.engine.execute(postgres_batchinfo, (batchid))
            result = batch_info_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def fetch_rules(self, batch_id):
        postgres_rules = """SELECT RL.RULE_ID, RL.RULE_DESCRIPTION, RL.VALUE_FIELD, 
        RL.RULE_NAME, RL.CATEGORY, RL.PATTERN, RL.VERSION, RL.SOURCE_TYPE, RL.DB_PATH, RL.TABLE_FILE_NAME 
        FROM DQ_RULES RL, BATCH_EXECUTION_AUDIT BEA, CONFIG CONF
        WHERE BEA.BATCH_ID = %s AND BEA.SOURCE_ID = CONF.SOURCE_ID AND RL.DB_PATH = CONF.SOURCE_DATABASE 
        AND RL.TABLE_FILE_NAME = BEA.TABLE_FILE_NAME"""
        try:
            rules_result = self.engine.execute(postgres_rules, (batch_id,))
            result = rules_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def insert_batch(self, sourceinfo):
        try:
            for tables in sourceinfo['sources']:
                postgres_insert_query = """ INSERT INTO BATCH (BATCH_ID, SOURCE_ID,	LOGGEDIN_USER_NAME,	TABLE_FILE_NAME) VALUES ({}, {}, '{}', '{}')"""
                schema = tables['schema']
                tablelist = tables['tablelist']
                tablelist = ','.join([str(elem) for elem in tablelist])
                postgres_insert_query = postgres_insert_query.format(sourceinfo['batchid'], sourceinfo['sourceid'], sourceinfo['loginusername'], tablelist)
                self.engine.execute(postgres_insert_query)
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def insert_batch_audit(self, sourceinfo):
        try:
            for tables in sourceinfo['sources']:
                schema = tables['schema']
                tablelist = tables['tablelist']
                for table in tablelist:
                    postgres_insert_query = """ INSERT INTO BATCH_EXECUTION_AUDIT(BATCH_ID, RUN_ID, SOURCE_ID, 
                                LOGGEDIN_USER_NAME, TABLE_FILE_NAME, PROFILE_STATUS, DQ_STATUS) VALUES ({}, {}, {}, '{}',
                                '{}', 'started', 'started')"""
                    postgres_insert_query = postgres_insert_query.format(sourceinfo['batchid'], sourceinfo['runid'],
                                                                     sourceinfo['sourceid'],sourceinfo['loginusername'], table)

                    self.engine.execute(postgres_insert_query)
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch audit table", error)
            raise

    def insert_error_records(self, batch_info, error_records):
        try:
            postgres_insert_query = """ INSERT INTO DQ_ERROR_RECORDS(BATCH_ID, RUN_ID,
            TABLE_FILE_NAME, RECORD, RULE_ID) VALUES (%s, %s, %s, %s, %s)"""
            for record in error_records:
                record_to_insert = (batch_info['batch_id'], batch_info['run_id'], batch_info['TABLE_FILE_NAME'], str(record), batch_info['rule_id'])
                self.engine.execute(postgres_insert_query, record_to_insert)
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)

    def insert_rule_records(self, batch_info, rule_records):
        try:
            postgres_insert_query = """ INSERT INTO DQ_ERROR_RULES(BATCH_ID, RUN_ID, TABLE_FILE_NAME, RULE_ID, 
            RULE_DESCRIPTION, RULE_NAME, STATUS) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            record_to_insert = (batch_info['batch_id'], batch_info['run_id'], batch_info['TABLE_FILE_NAME'],
                                batch_info['rule_id'], rule_records[2], rule_records[1], rule_records[4])
            self.engine.execute(postgres_insert_query, record_to_insert)
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)

    def getfailedrules(self, batchid, runid):
        postgres_errorrules = """SELECT BATCH_ID, RUN_ID, TABLE_FILE_NAME, RULE_ID, RULE_DESCRIPTION, RULE_NAME, STATUS DQ_ERROR_RULES WHERE BATCH_ID =  %s AND RUN_ID = %s"""
        try:
            failed_rule_result = self.engine.execute(postgres_errorrules, (batchid, runid))
            result = failed_rule_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise


    def geterrorrecords(self, batchid, runid):
        postgres_errorrecords = """SELECT BATCH_ID, RUN_ID, TABLE_FILE_NAME, RECORD, DQ_ERROR_RECORDS WHERE BATCH_ID =  %s AND RUN_ID = %s"""
        try:
            error_record_result = self.engine.execute(postgres_errorrecords, (batchid, runid))
            result = error_record_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise

    def getconfig(self, batchid, runid):
        postgres_errorrecords = """SELECT BATCH_ID, RUN_ID, TABLE_FILE_NAME, RECORD, DQ_ERROR_RECORDS WHERE BATCH_ID =  %s AND RUN_ID = %s"""
        try:
            error_record_result = self.engine.execute(postgres_errorrecords, (batchid, runid))
            result = error_record_result.fetchall();
            return result
        except(Exception, psycopg2.Error) as error:
            print("Failed to insert record into batch table", error)
            raise



class Snowflake:
    def __init__(self, user, password, account):
        self.ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )

    def read_snowflake(self,databse, schema, table):
        cs = self.ctx.cursor()
        cs.execute('USE DATABASE {database};'.format(database=databse))
        cs.execute("SELECT * FROM {schema}.{table}".format(schema=schema, table=table))
        data = cs.fetch_pandas_all()
        data = data.where(pd.notnull(data), None)
        return data

    def read_snowflake_str(self,databse, schema, table):
        cs = self.ctx.cursor()
        cs.execute('USE DATABASE {database};'.format(database=databse))
        cs.execute("SELECT * FROM {schema}.{table}".format(schema=schema, table=table))
        data = cs.fetch_pandas_all()
        data = data.where(pd.notnull(data), None)
        return data

    def read_information_schema_snf(self, databse):

        cs = self.ctx.cursor()

        cs.execute('USE DATABASE {database};'.format(database=databse))

        sql_cols = """select table_schema,
               table_catalog,
               table_name
        from information_schema.tables 
        where table_type = 'BASE TABLE'    
        order by table_schema,
               table_name;"""

        cs.execute(sql_cols)
        data = cs.fetch_pandas_all()
        data = data.where(pd.notnull(data), None)
        return data

