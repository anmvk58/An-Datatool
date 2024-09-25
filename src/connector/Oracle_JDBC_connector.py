import os
import re
import jaydebeapi
import pandas as pd
from src.common.io_utils import IOUtils
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert


class OracleJDBC(Connector):
    def create_connection(self):
        list_driver = IOUtils.get_src_path(os.getcwd())
        try:
            cnn = jaydebeapi.connect("oracle.jdbc.OracleDriver",
                                     f"""jdbc:oracle:thin:@{self.server}:{self.port}/{self.service_name}""",
                                     [self.user, self.pw],
                                     list_driver)
            return cnn
        except Exception:
            raise Exception("Errors in connection")

    def make_query_limit_1(self, query) -> str:
        query = re.sub(' +', ' ', query.strip())
        first_word = query.split(" ")[0]
        last_word = query.split(" ")[-1]
        near_last_word = query.split(" ")[-2]
        third_last_word = query.split(" ")[-3]
        if first_word.lower() == 'select' and last_word.lower() != 'only' and near_last_word.lower() != 'rows':
            return query + " FETCH FIRST 1 ROWS ONLY"
        elif first_word.lower() == 'select' and last_word.lower() == 'only' and near_last_word.lower() == 'rows':
            cmd = third_last_word + ' ' + near_last_word + ' ' + last_word
            return query.replace(cmd, '1 ROWS ONLY')
        else:
            return "SELECT * FROM " + query + " FETCH FIRST 1 ROWS ONLY"

    def get_mapping_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"SRC_DATA_TYPE": DictionaryConvert.dict_ORACLE.keys(),
             "PYARROW_DATATYPE": DictionaryConvert.dict_ORACLE.values()}
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
