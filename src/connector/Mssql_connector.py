import pyodbc
import pandas as pd
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert

class MssqlConnector(Connector):
    @property
    def driver(self) -> str:
        list_driver = pyodbc.drivers()
        for driver in list_driver:
            if 'for SQL Server' in driver and 'ODBC Driver' in driver:
                return driver
        raise Exception("Không có driver thích hợp !!!")

    # connSqlServer = pyodbc.connect('DRIVER={};SERVER=192.106.0.102,1443;DATABASE=master;UID=sql2008;PWD=password123')
    # connSqlServer = pyodbc.connect('DRIVER={};SERVER=192.106.0.102\instance1;DATABASE=master;UID=sql2008;PWD=password123')
    def create_connection(self):
        return pyodbc.connect('DRIVER={%s};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s' % (
            self.driver, self.server, self.port, self.schema, self.user, self.pw))

    def make_query_limit_1(self, query) -> str:
        return "SELECT TOP 1 * FROM " + query

    def get_mapping_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"SRC_DATA_TYPE": DictionaryConvert.dict_MSSQL.keys(),
             "PYARROW_DATATYPE": DictionaryConvert.dict_MSSQL.values()}
        )

