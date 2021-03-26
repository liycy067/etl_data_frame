import yaml
import os
import sys
sys.path.append(os.getcwd())
from utils.postgressql_connector import PostgresSqlConnector
BASE_PATH = "{}/load_ac_shopping_crm/config/".format(os.getcwd().replace("\\", "/"))

class AcShoppingCrmConfig:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.yaml_string = self._get_yaml()
        

    def _get_yaml(self):
        with open(self.yaml_file) as config_file:
            return yaml.full_load(config_file)

    def get_yaml_attr(self, attribute_name):
        return self.yaml_string.get(attribute_name)
        '''
        (Dictionary) get() 函数返回指定键的值。
         (Dictionary) items() 函数以列表返回可遍历的(键, 值) 元组数组
        dict = {'Google': 'www.google.com', 'Runoob': 'www.runoob.com', 'taobao': 'www.taobao.com'}
        print (dict.items())
        [('Google', 'www.google.com'), ('taobao', 'www.taobao.com'), ('Runoob', 'www.runoob.com')]
        for key,values in  dict.items():
            print (key,values)
        Google www.google.com
        Runoob www.runoob.com
        taobao www.taobao.com'''

    def get_pipeline_config(self):
        return PipelineConfig(self)
    
    def get_table_config(self):
        table_config_list = []
        for table in self.yaml_string.get('tables'):
            '''
            {'customer': 
                {'destination_table': 'customer', 'export_file_name': 'customer.csv', 's3_file_path': 'ac_shopping_crm/customer.csv', 
                'load_columns': ['customer_id', 'first_name', 'last_name', 'gender', 'dob', 'registered_at', 'last_login_at', 'email_address', 'mobile', 'street_1', 'street_2', 'suburb', 'state', 'country', 'postcode', 'created_at', 'updated_at'], 
                'incremental_load_columns': ['created_at', 'updated_at'], 
                'copy_extra_params': "CSV DELIMITER AS ',' IGNOREHEADER 1 NULL AS 'NULL'"}
            }
            {'order': 
                {'destination_table': 'order', 'export_file_name': 'order.csv', 's3_file_path': 'ac_shopping_crm/order.csv', 
                'load_columns': ['order_id', 'order_code', 'order_datetime', 'customer_id', 'order_status', 'device', 'created_at', 'updated_at'], 
                'incremental_load_columns': ['created_at', 'updated_at'], 
                'copy_extra_params': "CSV DELIMITER AS ',' IGNOREHEADER 1 NULL AS 'NULL'"}
            }

            '''
            for table_name, table_config_attr in table.items():
                table_config_list.append(TableConfig(table_name, table_config_attr))
        return table_config_list


class PipelineConfig:
    def __init__(self, config):
        self.dag_name = config.get_yaml_attr("dag_name")
        self.s3_bucket_name = config.get_yaml_attr("s3_bucket_name")
        self.source_credentials = config.get_yaml_attr("source_credentials")
        self.source_schema = config.get_yaml_attr("source_schema")
        self.destination_schema = config.get_yaml_attr("destination_schema")
        self.destination_credentials = config.get_yaml_attr("destination_credentials")

    def _get_source_connection(self):
        return PostgresSqlConnector(self, self.source_credentials)
    
    def _get_destination_connection(self):
        return PostgresSqlConnector(self, self.destination_credentials)


class TableConfig:
    def __init__(self, table_name, table_config_attr):
        self._resolve_parameters(table_name, table_config_attr)

    def _resolve_parameters(self, table_name,table_config_attr):
        self.table = table_name
        self.export_file_name = table_config_attr.get('export_file_name')
        self.load_columns = table_config_attr.get('load_columns')
        self.incremental_load_columns = table_config_attr.get('incremental_load_columns')
        self.copy_extra_params = table_config_attr.get('copy_extra_params')
        self.s3_file_path = table_config_attr.get('s3_file_path')
        self.primary_key = table_config_attr.get('primary_key')

