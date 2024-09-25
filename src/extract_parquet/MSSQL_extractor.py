from src.extract_parquet.extractor import Extractor

class MssqlExtractor(Extractor):
    def get_sql_datatype_src(self) -> str:
        query_get_type_src = """
                            SELECT COLUMN_NAME, 
                                    UPPER(org.DATA_TYPE) AS SRC_DATA_TYPE, 
                                    COALESCE(org.NUMERIC_PRECISION, 0) AS NUMERIC_PRECISION, 
                                    COALESCE(org.NUMERIC_SCALE, 10) AS NUMERIC_SCALE
                            FROM INFORMATION_SCHEMA.COLUMNS org
                            WHERE org.TABLE_CATALOG = '{SRC_SCHEMA_NAME}' 
                            AND org.TABLE_NAME = '{SRC_TABLE_NAME}'
                            AND org.COLUMN_NAME IN {SRC_FILTER_COL_CONDITION}
                            """.format(SRC_SCHEMA_NAME=self.schema_name,
                                               SRC_TABLE_NAME=self.table_name,
                                               SRC_FILTER_COL_CONDITION=self.list_column_select_sql)
        return query_get_type_src
