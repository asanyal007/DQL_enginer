from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import snowflake.connector

class SQLAlchemy:
    def __init__(self, conn_type, user, password, host, db):
        conn_str = '{conn_type}://{user}:{password}@{host}/{db}'.format(conn_type=conn_type,
                                                                        user=user, password=password,
                                                                        host=host, db=db)
        self.engine = create_engine(conn_str, echo=False)

    def get_ddl(self, data, tablename):
        ddl = pd.io.sql.get_schema(data, tablename).replace('\n','')
        return ddl

    def to_sql(self, data, tablename, dtype):
        if dtype:
            data.to_sql(tablename, con=self.engine, if_exists='replace', dtype= dtype)
        else:
            data.to_sql(tablename, con=self.engine, if_exists='replace')

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

