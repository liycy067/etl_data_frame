import sys
import os
import json
from ac_shopping_crm_config import AcShoppingCrmConfig, PipelineConfig, TableConfig
from utils.postgressql_connector import PostgresSqlConnector
from utils.secrets import get_secret
from utils.tasks import BaseTask
from utils.s3_connector import S3Connector
BASE_PATH = "{}/load_ac_shopping_crm/config/".format(os.getcwd().replace("\\", "/"))

class LoadAcShoppingCrm(BaseTask):

    def __init__(self, config_path=None, table_name=None):
        self.config_path = config_path

    def extract(self,table_config):

        
    

        incremental_load_datetime = self.destination_connector.get_sql_result("""select coalesce(dateadd(days,-2,max(created_at)),'1900-01-01') from ac_shopping_crm.{}""".format(table_config.table))[0][0]

        incremental_sql = """ select {}
            from ac_shopping.{}
            where created_at>'{}' or updated_at>'{}';
            """.format(",".join(table_config.load_columns),table_config.table, incremental_load_datetime, incremental_load_datetime)

        self.source_connector.export_sql_to_csv(incremental_sql,table_config.export_file_name)

        s3 = S3Connector()
        s3.configure()
        s3.upload_file('{}.csv'.format(table_config.table), self.pipe_config.s3_bucket_name, object_name=None)

    def load(self,table_config):

        truncate_staging_sql = """truncate staging.{}""".format(table_config.table)
        copy_to_staging_sql = """copy staging.{}
        from 's3://{}/{}'
        credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
        CSV DELIMITER AS ','
        IGNOREHEADER 1
        timeformat 'auto'
        NULL as 'NULL';
        """.format(table_config.table,self.pipe_config.s3_bucket_name,table_config.s3_file_path)

        self.destination_connector.copy_csv_to_staging(truncate_staging_sql, copy_to_staging_sql)

        delete_sql = """delete from ac_shopping_crm.{}
                    where {} in (select {} from staging.{})
                    """.format(table_config.table,table_config.primary_key,table_config.primary_key,table_config.table)
        insert_sql = """insert into ac_shopping_crm.{}
                    select * from staging.{}
                    """.format(table_config.table,table_config.table)

        self.destination_connector.update_from_staging_to_redshift(delete_sql,insert_sql)


    def main(self):
        ac_shopping_config = AcShoppingCrmConfig(yaml_file = "{}{}.yml".format(BASE_PATH,self.config_path))
        self.pipe_config = ac_shopping_config.get_pipeline_config()

        source_credentials = get_secret(self.pipe_config.source_credentials)
        self.source_connector = PostgresSqlConnector(source_credentials)
        
    
        destination_credentials = get_secret(self.pipe_config.destination_credentials)
        self.destination_connector = PostgresSqlConnector(destination_credentials)

        table_config_list= ac_shopping_config.get_table_config()

        for table_config in table_config_list:
            self.extract(table_config)
            self.load(table_config)
        
        

if __name__ == "__main__":
    task = LoadAcShoppingCrm("ac_shopping_crm")
    task.main()

