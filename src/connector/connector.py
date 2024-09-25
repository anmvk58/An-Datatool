import pyodbc
import os
import pandas as pd
import src.common.encryptor as encryptor
from src.common.config_reader import Config_reader

class Connector:
    connection = {}
    # cursor = {}

    def __init__(self, src_name='WHR2', config_path=None):
        self.src_name = src_name
        self.config_path = config_path
        if src_name not in Connector.connection:
            Connector.connection[src_name] = self.create_connection()
        # if src_name not in Connector.cursor:
        #     Connector.cursor[src_name] = self.create_cursor()

    @property
    def decrypt_key(self) -> str:
        # golive se config trong ENV variable
        return os.getenv('FERNET_KEY', 'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o=')

    @property
    def config(self):
        return Config_reader(self.config_path).get_info_database(self.src_name)

    @property
    def driver(self) -> str:
        return self.config['driver']

    @property
    def database(self) -> str:
        return self.config['DB']

    @property
    def engine(self) -> str:
        return self.config['engine']

    @property
    def schema(self) -> str:
        return self.config['schema']

    @property
    def server(self) -> str:
        return self.config['server']

    @property
    def service_name(self) -> str:
        return self.config['service_name']

    @property
    def user(self) -> str:
        return self.config['user']

    @property
    def pw(self) -> str:
        return encryptor.decrypt(self.config['password'].encode(), self.decrypt_key.encode()).decode()

    @property
    def port(self) -> str:
        return self.config['port']

# method to override in subclass
    def create_connection(self):
        pass

    def create_cursor(self):
        return self.connection[self.src_name].cursor()

    def read_sql_query(self, query) -> pd.DataFrame:
        # return pd.read_sql_query(query, self.connection[self.src_name], coerce_float=False, chunksize=1)
        return pd.read_sql_query(query, self.connection[self.src_name], coerce_float=False)


    def read_sql_query_one_row(self, query) -> pd.DataFrame:
        limit_query = self.make_query_limit_1(query)
        return pd.read_sql_query(limit_query, self.connection[self.src_name], coerce_float=False)

    def make_query_limit_1(self, query) -> str:
        pass

    def get_mapping_df(self) -> pd.DataFrame:
        pass

    def get_meta_data_from_query(self, sql_query):
        cursor = self.create_cursor()
        cursor.execute(sql_query).fetchone()
        list_meta = cursor.description
        df_meta = pd.DataFrame(list_meta)
        headers = ["COL_NAME", "DATA_TYPE", "X", "LENGTH", "PRECISION", "SCALE", "Y"]
        df_meta.columns = headers
        df_meta["DATA_TYPE"] = df_meta.apply(
            lambda x: x["DATA_TYPE"].__name__, axis=1
        )
        total_row = df_meta.shape[0]
        print(df_meta)
        # if(cnn.engine == 'MSSQL'):
        cursor.close()
        return df_meta, total_row