import psycopg2
from psycopg2 import Error
from psycopg2 import sql
import pandas as pd
import csv
import pandas


class PostgresSqlConnector:
    def __init__(self,credentials):
        self.credentials = credentials

    def get_connection(self):
        try:
            connection = psycopg2.connect(
                host = self.credentials.get('host'),
                database = self.credentials.get("database"),
                port = self.credentials.get("port"),
                user = self.credentials.get("username"),
                password = self.credentials.get("password"))
        except Exception as e:
            print(e)
            raise e
        return connection

   
    def get_sql_result(self,sql):
        try:
            connection = self.get_connection()
            cursor= connection.cursor()
            cursor.execute(sql)
        except Exception as e:
            print(e)
            raise e
        return cursor.fetchall()

    def export_sql_to_csv(self,sql,export_file_name):
        try:
            connection = self.get_connection()
            # cursor = connection.cursor()
            export_df = pandas.read_sql_query(sql, connection)
            export_df.to_csv('{}.csv'.format(export_file_name),header='true', index=False)
        except Exception as e:
            print(e)
            raise

    def copy_csv_to_staging(self, truncate_staging_sql, copy_to_staging_sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(truncate_staging_sql)
            cursor.execute(copy_to_staging_sql)
            cursor.execute("COMMIT")
        except Exception as e:
            print(e)
            raise
        
        print('copy to staging successful')

    def update_from_staging_to_redshift(self,delete_sql,insert_sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(delete_sql)
            cursor.execute(insert_sql)
        except Exception as e:
            print(e)
            raise

        print('update update_from_staging_to_redshift successful')
