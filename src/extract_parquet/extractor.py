import re
from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar
import pyarrow.parquet as pq
import shutil
from pathlib import Path
from src.common.io_utils import IOUtils
import datetime
import pandas as pd
import pyarrow as pa

class Extractor:
    def __init__(self, query, src_name, is_Full_query = False):
        self.src_cnn = Connector_factory.create_connector(src_name, DefaultVar.DEV_ENV)
        # self.map_cnn = Connector_factory.create_connector(DefaultVar.SRC_MAP, DefaultVar.DEV_ENV)
        if(is_Full_query == True):
            self.query = re.sub(' +', ' ', query).strip().upper()
        else:
            self.query = self.get_full_querry(query).upper()

    def get_full_querry(self, query):
        df = self.src_cnn.read_sql_query_one_row(query)
        list_col = ""
        for col in df.head():
            list_col += col + ', '
        return "SELECT " + list_col[:-2] + " FROM " + query


    @property
    def list_column_select(self) -> str:
        end_index = self.query.find(" FROM ")
        return self.query[7:end_index]

    @property
    def list_column_select_sql(self) -> str:
        temp = self.list_column_select.split(", ")
        result = "("
        for col in temp:
            result += "'" + col + "', "
        return result[:-2] + ")"

    @property
    def schema_table(self) -> str:
        start_index = self.query.find(" FROM ") + 6
        where_index = self.query.find(" WHERE ")
        if (where_index == -1):
            # Khong co menh de where
            schema_table = self.query[start_index:]
        else:
            # co menh de where
            schema_table = self.query[start_index:where_index]
        result = schema_table.strip().split(".")
        return result

    @property
    def schema_name(self) -> str:
        return self.schema_table[0]

    @property
    def table_name(self) -> str:
        return self.schema_table[-1]

    @property
    def full_table_name(self) -> str:
        return self.schema_name + '_' + self.table_name

    def get_sql_datatype_src(self) -> str:
        pass

    def get_destination_schema(self):
        query_get_type_src = self.get_sql_datatype_src()
        query_get_mapping = """
                            SELECT map_tbl.SRC_DATATYPE AS SRC_DATA_TYPE
                                , PYARROW_DATATYPE
                            FROM DATATYPE_MAPPING_ATHENA_MASTER map_tbl
                            WHERE map_tbl.SQL_ENGINE ='{SQL_ENGINE}'
                            """.format(SQL_ENGINE=self.src_cnn.engine)
        df_type_src = self.src_cnn.read_sql_query(query_get_type_src)
        # df_mapping = self.map_cnn.read_sql_query(query_get_mapping)
        df_mapping = self.src_cnn.get_mapping_df()

        df = pd.merge(df_type_src, df_mapping, how='left', on='SRC_DATA_TYPE')
        df['PYARROW_DATATYPE'] = df.apply(
            lambda x: 'decimal128({PRE},{SCALE})' if (x['SRC_DATA_TYPE'] in ['NUMBER', 'DECIMAL', 'NUMERIC']) and (
                pd.notnull(x['NUMERIC_SCALE'])) and (pd.notnull(x['NUMERIC_PRECISION'])) else x['PYARROW_DATATYPE'],
            axis=1)
        df['PYARROW_DATATYPE'] = df.apply(
            lambda x: 'pa.' + x['PYARROW_DATATYPE'].format(PRE=x['NUMERIC_PRECISION'], SCALE=x['NUMERIC_SCALE']),
            axis=1)
        data_types = []

        for id, row in df.iterrows():
            data_types.append((row['COLUMN_NAME'], eval(row['PYARROW_DATATYPE'])))

        # Sort like source table
        sorted_cols = list(eval(self.list_column_select_sql))

        mapping = dict(data_types)
        data_types[:] = [(x, mapping[x]) for x in sorted_cols]
        destination_schema = pa.schema(data_types)
        return destination_schema

    def extract_data(self):
            now = datetime.datetime.today()
            business_date = now.strftime("%Y%m%d")

            # Xoa old folder neu da ton tai
            shutil.rmtree(IOUtils.get_extract_folder_by_partition(self.table_name.upper(), business_date),
                          ignore_errors=True)

            # Tạo folder mong muốn nếu chưa tồn tại
            folder_path = IOUtils.get_extract_folder_by_partition(self.table_name.upper(), business_date)
            Path(folder_path).mkdir(parents=True, exist_ok=True)

            dest_schema = self.get_destination_schema()

            number_of_parquet_files = 0
            total_record_count = 0
            index = 1
            # df = pd.read_sql(self.query, self.src_cnn.connection[self.src_cnn.src_name], chunksize=100000, coerce_float=False)

            for chunk in pd.read_sql(self.query, self.src_cnn.connection[self.src_cnn.src_name],
                                     chunksize=1000000,
                                     coerce_float=False):  # coerce_float=False dung de khong convert to float
                pa_chunk = pa.Table.from_pandas(chunk[dest_schema.names]).cast(dest_schema)
                file_name = f'{self.table_name}_{business_date}_{index}.parquet'
                exported_parquet_path = Path(f"{folder_path}/{file_name}")
                pq.write_table(pa_chunk, exported_parquet_path, compression='gzip')

                number_of_parquet_files += 1
                total_record_count += len(chunk)
                index = index + 1
            return True