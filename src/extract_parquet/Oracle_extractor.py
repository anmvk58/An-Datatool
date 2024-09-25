from src.extract_parquet.extractor import Extractor

class OracleExtractor(Extractor):
    def get_sql_datatype_src(self) -> str:
        query_get_type_src = """
                            SELECT COLUMN_NAME, 
                                UPPER(org.DATA_TYPE) AS SRC_DATA_TYPE,
                                nvl(org.DATA_PRECISION,org.DATA_LENGTH) AS NUMERIC_PRECISION,
                                org.DATA_SCALE AS NUMERIC_SCALE
                            FROM ALL_TAB_COLUMNS org
                            WHERE org.OWNER = '{SRC_SCHEMA_NAME}'
                                 AND org.TABLE_NAME = '{SRC_TABLE_NAME}'
                                 AND org.COLUMN_NAME IN {SRC_FILTER_COL_CONDITION}
                            """.format(SRC_SCHEMA_NAME=self.schema_name,
                                               SRC_TABLE_NAME=self.table_name,
                                               SRC_FILTER_COL_CONDITION=self.list_column_select_sql)
        return query_get_type_src

if __name__ == '__main__':
    sql = "DIHDEV.T24_MOVEMENT_ENTRY"
    db2 = OracleExtractor(sql, 'ORA_DIH')
    db2.extract_data()



























































































































































