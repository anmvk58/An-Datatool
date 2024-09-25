from src.extract_parquet.extractor import Extractor
import pandas as pd

class DB2Extractor(Extractor):

    def get_sql_datatype_src(self) -> pd.DataFrame:
        query_get_type_src = """
                            SELECT COLNAME AS COLUMN_NAME, 
                                UPPER(org.TYPENAME) AS SRC_DATA_TYPE, 
                                COALESCE(org.LENGTH, 31) AS NUMERIC_PRECISION, 
                                COALESCE(org.SCALE, 10) AS NUMERIC_SCALE 
                            FROM SYSCAT.COLUMNS org
                            WHERE org.TABSCHEMA = '{SRC_SCHEMA_NAME}' 
                                 AND org.TABNAME = '{SRC_TABLE_NAME}'
                                 AND org.COLNAME IN {SRC_FILTER_COL_CONDITION}
                            """.format(SRC_SCHEMA_NAME=self.schema_name,
                                       SRC_TABLE_NAME=self.table_name,
                                       SRC_FILTER_COL_CONDITION=self.list_column_select_sql)
        return query_get_type_src



if __name__ == '__main__':
    sql = "UAT_VPB_C11_NEW.TBL_RPT_SAOKE_TINDUNG_WO_20220531"
    db2 = DB2Extractor(sql, 'VPB_STAG_OTHER')
    db2.extract_data()

