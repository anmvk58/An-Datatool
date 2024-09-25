import pyodbc
import pandas as pd
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert

class OracleConnector(Connector):
    @property
    def driver(self) -> str:
        list_driver = pyodbc.drivers()
        for driver in list_driver:
            if 'Oracle' in driver:
                return driver
        raise Exception("Không có driver thích hợp !!!")

    def create_connection(self):
        # DBQ la tns name
        conn =  pyodbc.connect(**{'DBQ': '%s:%s/%s' % (self.server, self.port, self.service_name), 'uid': self.user, 'pwd': self.pw,
                                'driver': self.driver})
        # conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf16')
        # conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf16')
        # conn.setencoding(encoding='utf8')
        return conn

    def make_query_limit_1(self, query) -> str:
        return "SELECT * FROM " + query + " WHERE ROWNUM = 1"

    def get_mapping_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"SRC_DATA_TYPE": DictionaryConvert.dict_ORACLE.keys(),
             "PYARROW_DATATYPE": DictionaryConvert.dict_ORACLE.values()}
        )

from src.common.default_var import DefaultVar
if __name__ == '__main__':
    cnn = OracleConnector('OCBADMIN_210428', config_path=DefaultVar.DEV_ENV)
    query = """
        Select ID , MODIFIER_ID , DATE_CHANGED , COLUMN_CHANGED ,  OLD_VALUE, NEW_VALUE , HISTORY_COMMENT , BRANCH
        FROM OCBADMIN_210428.HISTORY
        where to_char(DATE_CHANGED,'YYYY-MM-DD') = '2021-07-12' AND OLD_VALUE IN ('₫1,000.00', 'Một Nghìn Việt Nam Đồng') 
    """
    pd = cnn.read_sql_query(query)

    # cnn = OracleConnector('ORA_DIH', config_path=DefaultVar.DEV_ENV)
    # query = """
    #         Select ID , MODIFIER_ID , DATE_CHANGED, COLUMN_CHANGED , OLD_VALUE , NEW_VALUE , HISTORY_COMMENT , BRANCH
    #         FROM VPB_DIH_DB.OCB_HISTORY
    #     """
    pd = cnn.read_sql_query(query)

    print(pd)
    print("hello")


























