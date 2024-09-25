import re
import os
import jaydebeapi
import pandas as pd
from src.common.io_utils import IOUtils
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert


class MysqlJDBC(Connector):
    def create_connection(self):
        list_driver = IOUtils.get_src_path(os.getcwd())
        try:
            return jaydebeapi.connect("com.mysql.cj.jdbc.Driver",
                                      f"jdbc:mysql://{self.server}:{self.port}/{self.schema}",
                                      [self.user, self.pw],
                                      list_driver)
        except Exception as e:
            raise Exception(e.message)

    # in case we want connect a host which have many instances
    # connSqlServer = pyodbc.connect('DRIVER={};SERVER=192.106.0.102\instance1;DATABASE=master;UID=sql2008;PWD=password123')

    def make_query_limit_1(self, query) -> str:
        query = re.sub(' +', ' ', query.strip())
        first_word = query.split(" ")[0]
        second_word = query.split(" ")[-2]
        third_word = query.split(" ")[-1]
        if first_word.lower() == 'select' and second_word.lower() != 'limit':
            return "SELECT" + query[6:] + " limit 1"
        elif first_word.lower() == 'select' and second_word.lower() == 'limit':
            return query.replace(third_word, '1')
        else:
            return "SELECT * FROM " + query + " LIMIT 1"

    def get_mapping_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"SRC_DATA_TYPE": DictionaryConvert.dict_MSSQL.keys(),
             "PYARROW_DATATYPE": DictionaryConvert.dict_MSSQL.values()}
        )

    def get_data_type_correct(self, s):
        """
        :param s: chuoi can xu ly
        :type s: str
        :return: str
        """
        list_s = s.replace('DBAPITypeObject', '').split(', ')[0]
        return re.sub("\'|\(|\)", "", list_s)

    def get_meta_data_from_query(self, sql_query):
        cursor = self.create_cursor()
        cursor.execute(self.make_query_limit_1(sql_query))
        list_meta = cursor.description
        df_meta = pd.DataFrame(list_meta)
        headers = ["COL_NAME", "DATA_TYPE", "X", "LENGTH", "PRECISION", "SCALE", "Y"]
        df_meta.columns = headers
        df_meta["DATA_TYPE"] = df_meta.apply(
            lambda x: self.get_data_type_correct(str(x["DATA_TYPE"])), axis=1
        )
        total_row = df_meta.shape[0]
        cursor.close()
        return df_meta, total_row
