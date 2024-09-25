import pandas as pd


class SQL_Command:
    def __init__(self, connection, table_name_input, number_of_rows = 100):
        self.connection = connection
        self.table_name_input = table_name_input
        self.number_of_rows = str(number_of_rows)

    @property
    def schema_table(self) -> str:
        start_index = self.table_name_input.find(" FROM ")
        if(start_index == -1):
            start_index = 0
        else:
            start_index += 6
        where_index = self.table_name_input.find(" WHERE ")
        if (where_index == -1):
            schema_table = self.table_name_input[start_index:]
        else:
            schema_table = self.table_name_input[start_index:where_index]
        # result = schema_table.strip().split(".")
        return schema_table.strip()

    @property
    def schema_name(self) -> str:
        schema_list = self.schema_table.split(".")
        return schema_list[0]

    @property
    def table_name(self) -> str:
        schema_list = self.schema_table.split(".")
        return schema_list[-1]

    @property
    def get_meta_data(self) -> pd.DataFrame:
        pass

    @property
    def list_columns_number(self) -> list:
        pass

    @property
    def list_columns_not_number(self) -> list:
        pass

    # Tao cau truy van sum cho number
    def make_sql_for_number_sum(self) -> str:
        pass

    # Tao cau truy van max cho number
    def make_sql_for_number_max(self) -> str:
        max_sql = 'SELECT '
        for i, col in enumerate(self.list_columns_number):
            max_sql += 'Max("' + col + '") AS "' + col + '", '
        max_sql += '\'MAX\' As Type_Check FROM ' + self.table_name_input
        return max_sql

    # Tao cau truy van min cho number
    def make_sql_for_number_min(self) -> str:
        min_sql = 'SELECT '
        for i, col in enumerate(self.list_columns_number):
            min_sql += 'Min("' + col + '") AS "' + col + '", '
        min_sql += '\'MIN\' As Type_Check FROM ' + self.table_name_input
        return min_sql

    # Tao cau truy van select count table:
    def make_sql_select_count(self) -> str:
        sql = "SELECT COUNT_BIG(1) as TOTAL_ROW FROM " + self.table_name_input
        return sql

    #Tao cau truy van compare number column
    def make_sql_compare_number(self) -> str:
        sql = self.make_sql_for_number_sum() + " UNION ALL " + \
              self.make_sql_for_number_max() + " UNION ALL " + \
              self.make_sql_for_number_min()
        return sql

    # Tao cau truy van lay ra 100 ban ghi string cho mssql
    def make_sql_compare_not_number(self) -> str:
        string_sql = 'SELECT '
        string_order = ''
        for i, col in enumerate(self.list_columns_not_number):
            string_sql += '"' + col + '"'
            string_order += '"' + col + '"'
            if i < self.list_columns_not_number.size - 1:
                string_sql += ', '
                string_order += ', '
        string_sql += ' FROM ' + self.table_name_input + ' ORDER BY ' + string_order + ' LIMIT ' + self.number_of_rows
        return string_sql

class MSSQL_Command(SQL_Command):
    @property
    def list_columns_number(self) -> list:
        df = self.get_meta_data()
        return df[df['SRC_DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'TINYINT', 'REAL'])]['COLUMN_NAME']

    @property
    def list_columns_not_number(self) -> list:
        df = self.get_meta_data()
        return df[~df['SRC_DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'TINYINT', 'REAL', 'NTEXT', 'TEXT', 'IMAGE'])]['COLUMN_NAME']

    def get_meta_data(self):
        query = """
                SELECT COLUMN_NAME, 
                        UPPER(org.DATA_TYPE) AS SRC_DATA_TYPE, 
                        COALESCE(org.NUMERIC_PRECISION, 0) AS NUMERIC_PRECISION, 
                        COALESCE(org.NUMERIC_SCALE, 10) AS NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS org
                WHERE org.TABLE_CATALOG = '{SRC_SCHEMA_NAME}' 
                AND org.TABLE_NAME = '{SRC_TABLE_NAME}'
        """.format(SRC_SCHEMA_NAME=self.schema_name, SRC_TABLE_NAME=self.table_name)
        df = self.connection.read_sql_query(query)
        return df

    def make_sql_for_number_sum(self) -> str:
        sum_sql = 'SELECT '
        for i, col in enumerate(self.list_columns_number):
            sum_sql += 'Sum(CAST("' + col + '" AS numeric(38, 10))) AS "' + col + '", '
        sum_sql += '\'SUM\' As Type_Check FROM ' + self.table_name_input
        return sum_sql

    def make_sql_compare_not_number(self) -> str:
        string_sql = 'SELECT TOP ' + str(self.number_of_rows) + ' '
        string_order = ''
        for i, col in enumerate(self.list_columns_not_number):
            string_sql += '"' + col + '"'
            string_order += '"' + col + '"'
            if i < self.list_columns_not_number.size - 1:
                string_sql += ', '
                string_order += ', '
        string_sql += ' FROM ' + self.table_name_input + ' ORDER BY ' + string_order
        return string_sql

class DB2_Command(SQL_Command):
    @property
    def list_columns_number(self) -> list:
        df = self.get_meta_data()
        return df[df['SRC_DATA_TYPE'].isin(['BIGINT', 'DECFLOAT', 'DECIMAL', 'DOUBLE', 'INTEGER', 'REAL', 'SMALLINT'])]['COLUMN_NAME']

    @property
    def list_columns_not_number(self) -> list:
        df = self.get_meta_data()
        return df[~df['SRC_DATA_TYPE'].isin(
            ['BIGINT', 'DECFLOAT', 'DECIMAL', 'DOUBLE', 'INTEGER', 'REAL', 'SMALLINT'])]['COLUMN_NAME']

    def get_meta_data(self):
        query = """
                SELECT COLNAME AS COLUMN_NAME, 
                    UPPER(org.TYPENAME) AS SRC_DATA_TYPE, 
                    COALESCE(org.LENGTH, 31) AS NUMERIC_PRECISION, 
                    COALESCE(org.SCALE, 10) AS NUMERIC_SCALE 
                FROM SYSCAT.COLUMNS org
                WHERE org.TABSCHEMA = '{SRC_SCHEMA_NAME}' 
                     AND org.TABNAME = '{SRC_TABLE_NAME}'
            """.format(SRC_SCHEMA_NAME=self.schema_name, SRC_TABLE_NAME=self.table_name)
        df = self.connection.read_sql_query(query)
        return df

    def make_sql_for_number_sum(self) -> str:
        sum_sql = 'SELECT '
        for i, col in enumerate(self.list_columns_number):
            sum_sql += 'Sum(' + col + ') AS "' + col + '", '
        sum_sql += '\'SUM\' As Type_Check FROM ' + self.table_name_input
        return sum_sql

class Oracle_Command(SQL_Command):
    def make_sql_for_number_sum(self) -> str:
        return super().make_sql_for_number_sum()

class Factory_Command():
    @staticmethod
    def create_command(engine_type, connection, table_name_input, number_of_rows = 100):
        if (engine_type == 'DB2'):
            return DB2_Command(connection, table_name_input, number_of_rows)
        elif (engine_type == 'ORACLE'):
            return Oracle_Command(connection, table_name_input, number_of_rows)
        elif (engine_type == 'MSSQL'):
            return MSSQL_Command(connection, table_name_input, number_of_rows)
        else:
            # logger.error("Not founded: engine %s" % engine)
            raise NotImplementedError("Not founded: engine %s" % engine_type)





from connector.connector_factory import Connector_factory
from common.default_var import DefaultVar
if __name__ == '__main__':
    whr2_cnn = Connector_factory.create_connector('WHR2', DefaultVar.DEV_ENV)
    table_name = 'UAT_WHR2.dbo.ACCT_BAL'
    cmd = MSSQL_Command(whr2_cnn, table_name)
    print(cmd.make_sql_for_number_max())