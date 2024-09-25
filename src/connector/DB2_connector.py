import ibm_db_dbi as db2
import pandas as pd

from src.common.dictionary_convert import DictionaryConvert
from src.connector.connector import Connector

class DB2Connector(Connector):

    def create_connection(self):
        return db2.connect(
            "DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};".format(
                DATABASE=self.database, HOSTNAME=self.server, PORT=self.port, UID=self.user, PWD=self.pw)
            , "", "")

    def make_query_limit_1(self, query) -> str:
        return "SELECT * FROM " + query + " LIMIT 1"

    def get_mapping_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"SRC_DATA_TYPE": DictionaryConvert.dict_DB2.keys(),
             "PYARROW_DATATYPE": DictionaryConvert.dict_DB2.values()}
        )

    def get_meta_data_from_query(self, sql_query):
        cursor = self.create_cursor()
        q = cursor.execute(sql_query)
        list = cursor.description
        df_meta = pd.DataFrame(list)
        headers = ["COL_NAME", "DATA_TYPE", "X", "LENGTH", "PRECISION", "SCALE", "Y"]
        df_meta.columns = headers
        df_meta["DATA_TYPE"] = df_meta.apply(
            lambda x: x["DATA_TYPE"].col_types[0].lower(), axis=1
        )
        total_row = df_meta.shape[0]
        print(df_meta)
        cursor.close()
        return df_meta, total_row

