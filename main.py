import profiler
import data_util
from datetime import datetime
import sqlalchemy

now = datetime.now()

conn_type = 'postgresql'
user = 'postgres'
password = 'password'
host = 'localhost'
db = 'postgres'

# Snowlake info
snf_user = 'WNS123'
snf_password = 'Nq1dRuaV'
snf_account = 'dt21316.us-central1.gcp'
snf_databse = 'SNOWFLAKE_SAMPLE_DATA'

snf_conn = data_util.Snowflake(snf_user, snf_password, snf_account)

conn = data_util.SQLAlchemy(conn_type, user, password, host, db)

conn.create_tables("ddl/sql_ddl_attribute.txt")
conn.create_tables("ddl/sql_ddl_corr.txt")
conn.create_tables("ddl/sql_ddl_table_level")

source_data = snf_conn.read_snowflake(snf_databse, 'TPCDS_SF10TCL', 'ITEM')

source_data.to_csv('sample.csv')


final_corr_data, table_level_profile, attribute_profile = profiler.Profile(source_data).create_profile('ITEM', now, 1, 'ITEMS')

dtype = {
                'character_counts': sqlalchemy.types.JSON,
                'category_alias_values': sqlalchemy.types.JSON,
                'block_alias_values': sqlalchemy.types.JSON,
                'block_alias_counts': sqlalchemy.types.JSON,
                'block_alias_char_counts': sqlalchemy.types.JSON,
                'script_counts': sqlalchemy.types.JSON,
                'script_char_counts': sqlalchemy.types.JSON,
                'category_alias_counts': sqlalchemy.types.JSON,
                'category_alias_char_counts': sqlalchemy.types.JSON,
                'word_counts': sqlalchemy.types.JSON,
                'chi_squared': sqlalchemy.types.JSON,
                'histogram': sqlalchemy.types.JSON
            }


conn.to_sql(final_corr_data, 'corr', dtype={})
conn.to_sql(table_level_profile, 'table_level_profile', dtype={})
conn.to_sql(attribute_profile, 'attribute_profile', dtype=dtype)

