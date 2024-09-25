import re

from connector.connector_factory import Connector_factory
from common.default_var import DefaultVar
from compare_module.Compare_sql_cmd import Factory_Command
import src.compare_module.Compare_helpers as helper
import pandas as pd


class Compare_Object_Fast(object):

    def __init__(self, src_name, query):
        self.src_name = src_name
        self.connection = Connector_factory.create_connector(src_name, DefaultVar.DEV_ENV)
        self.query = re.sub(' +', ' ', query).strip().upper()
        self.command = Factory_Command.create_command(engine_type=self.connection.engine,
                                                      connection=self.connection,
                                                      table_name_input=self.query)


    def get_data_check_count(self):
        df = self.connection.read_sql_query(self.command.make_sql_select_count())
        return df

    def get_data_check_number(self):
        df = self.connection.read_sql_query(self.command.make_sql_compare_number())
        return df

    def get_data_check_not_number(self):
        df = self.connection.read_sql_query(self.command.make_sql_compare_not_number())
        return df

class Compare_Object_Query(object):
    def __init__(self, src_name, query):
        self.src_name = src_name
        self.cnn = Connector_factory.create_connector(src_name, config_path=DefaultVar.DEV_ENV)
        self.query = query

    def get_data_after_process(self):
        # get data from query and sort column index, rows
        try:
            df = self.cnn.read_sql_query(self.query)
        except Exception as e:
            raise Exception(e)

        df_table = df.reindex(sorted(df.columns), axis=1)

        # Tạm thời ko dùng hàm compare nên bỏ sort theo values và renew lại index của df
        # df_table = df_table.sort_values(by=list(df_table.columns), axis=0)
        # renew index
        # df_table = df_table.reset_index(drop=True)

        # Get meta data of data to fix data conversion date
        meta_data, total_col = self.cnn.get_meta_data_from_query(self.query)
        list_column_date = meta_data[meta_data['DATA_TYPE'].isin(['TIMESTAMP', 'DATE', 'DATETIME'])]['COL_NAME'].to_list()
        # print(list_column_date)

        # Get meta data of data to fix data conversion float & decimal
        list_column_number = meta_data[meta_data['DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'DECIMAL'])]['COL_NAME'].to_list()
        # Xu ly date -> datetime
        for col_need_fix in list_column_date:
            df_table[col_need_fix] = pd.to_datetime(df_table[col_need_fix])

        # Xu ly number -> numeric
        for col_need_fix in list_column_number:
            df_table[col_need_fix] = pd.to_numeric(df_table[col_need_fix])

        # Upper header:
        df_table.columns = map(str.upper, df_table.columns)
        return df_table, total_col

    def get_data_to_compare_file(self):
        # get data from query and sort column index, rows
        try:
            df = self.cnn.read_sql_query(self.query)
        except Exception as e:
            raise Exception(e)

        df_table = df.reindex(sorted(df.columns), axis=1)

        # Tạm thời ko dùng hàm compare nên bỏ sort theo values và renew lại index của df
        # df_table = df_table.sort_values(by=list(df_table.columns), axis=0)
        # renew index
        # df_table = df_table.reset_index(drop=True)

        # Get meta data of data to fix data conversion date
        meta_data, total_col = self.cnn.get_meta_data_from_query(self.query)
        list_column_date = meta_data[meta_data['DATA_TYPE'].isin(['TIMESTAMP', 'DATE', 'DATETIME'])]['COL_NAME'].to_list()
        # print(list_column_date)

        # Get meta data of data to fix data conversion float & decimal
        list_column_number = meta_data[meta_data['DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'DECIMAL'])]['COL_NAME'].to_list()
        # Xu ly date -> datetime
        for col_need_fix in list_column_date:
            df_table[col_need_fix] = pd.to_datetime(df_table[col_need_fix])

        # Xu ly number -> numeric
        for col_need_fix in list_column_number:
            # df_table[col_need_fix] = df_table[col_need_fix].map(str)
            df_table[col_need_fix] = pd.to_numeric(df_table[col_need_fix])

        # Upper header:
        df_table.columns = map(str.upper, df_table.columns)
        return df_table, total_col, meta_data


if __name__ == '__main__':
    src_name_1 = 'WHR2'
    query_1 = """
        SELECT CAST(BUSINESS_DATE As DATETIME) BUSINESS_DATE FROM UAT_WHR2.dbo.T24CRD
    """
    cof_1 = Compare_Object_Query(src_name_1, query_1)
    df = cof_1.get_data_after_process()
    print(df)

