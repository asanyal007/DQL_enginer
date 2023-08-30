# DQL_enginer

# Pandas Data Profile Generation Codebase

This repository contains Python code that utilizes the `profiler` module to create data profiles for a given dataset using the Pandas library. The data profiles include attribute-level profiles, table-level profiles, and correlation analyses. The resulting profiles are stored in a PostgreSQL database using SQLAlchemy for further analysis and visualization.

## Prerequisites

Before running the code in this repository, make sure you have the following:

- Python (>= 3.6)
- Pandas
- SQLAlchemy
- psycopg2 (for PostgreSQL database connection)
- Snowflake Connector (for Snowflake data source connection)

## Usage

1. **Configuration:**

   Update the connection parameters in the script according to your database configuration:

   ```python
   conn_type = 'postgresql'
   user = 'postgres'
   password = 'password'
   host = 'localhost'
   db = 'postgres'

   # Snowflake info
   snf_user = 'WNS123'
   snf_password = 'Nq1dRuaV'
   snf_account = 'dt21316.us-central1.gcp'
   snf_database = 'SNOWFLAKE_SAMPLE_DATA'
   ```

2. **Data Retrieval:**

   The code connects to a Snowflake database to retrieve source data. Update the Snowflake connection information as needed:

   ```python
   snf_conn = data_util.Snowflake(snf_user, snf_password, snf_account)
   source_data = snf_conn.read_snowflake(snf_database, 'TPCDS_SF10TCL', 'ITEM')
   ```

3. **Data Profiling:**

   Execute the profiling process on the source data to generate attribute-level profiles, table-level profiles, and correlation data:

   ```python
   final_corr_data, table_level_profile, attribute_profile = profiler.Profile(source_data).create_profile('ITEM', now, 1, 'ITEMS')
   ```

4. **Data Storage:**

   The generated profiles are stored in the PostgreSQL database using SQLAlchemy. Customize the data types as per your PostgreSQL database schema:

   ```python
   dtype = {
       'character_counts': sqlalchemy.types.JSON,
       'category_alias_values': sqlalchemy.types.JSON,
       # ... (customize the rest of the data types)
   }

   conn.to_sql(final_corr_data, 'corr', dtype={})
   conn.to_sql(table_level_profile, 'table_level_profile', dtype={})
   conn.to_sql(attribute_profile, 'attribute_profile', dtype=dtype)
   ```

5. **Execution:**

   Run the script to execute the data profiling process:

   ```bash
   python your_script_name.py
   ```

## Credits

This codebase was developed with contributions from [Your Name] and is part of [Project Name]. If you find this code useful, consider giving it a star or acknowledging the contributors.

## License

This code is provided under the [License Name] license. See the [LICENSE](LICENSE) file for details.

---

Feel free to modify this README to match your project's structure and requirements. Good luck with your data profiling endeavors!
