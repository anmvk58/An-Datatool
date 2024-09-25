import os
import art
import pandas as pd
from pathlib import Path
import make_sql_number_unit as number_sql
import make_sql_string_unit as not_number_sql
from common.default_var import DefaultVar
import compare_table.helper as helper
from joblib import Parallel, delayed
from common.setup_logger import logger
from connector.connector_factory import Connector_factory
from extract_parquet.extractor_factory import Extractor_factory

batch_check_path = 'compare_folder_batch_266'
director_path_result = r"{CWD}\{file_path}".format(CWD=os.getcwd(),file_path=batch_check_path)

def write_to_excel(file_name, df_merge_count, df_merge_number, df_merge_string, df_diff_count, df_diff_number,
                   df_diff_string):
    director_path = r"{CWD}\{file_path}".format(CWD=os.getcwd(),file_path=batch_check_path)
    Path(director_path).mkdir(parents=True, exist_ok=True)
    file_path = r"{CWD}\{file_path}\{table_name}.xlsx".format(table_name=file_name, CWD=os.getcwd(),file_path=batch_check_path)
    writer = pd.ExcelWriter(file_path)
    df_merge_count.to_excel(writer, 'CHECK_COUNT', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_merge_number.to_excel(writer, 'CHECK_NUMBER', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_merge_string.to_excel(writer, 'CHECK_STRING', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_diff_count.to_excel(writer, 'COUNT_ERROR', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_diff_number.to_excel(writer, 'NUMBER_ERROR', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_diff_string.to_excel(writer, 'STRING_ERROR', header=True, float_format="%.8f", index=None, encoding="utf8")
    writer.close()

def write_to_excel2(file_name, df_merge_count,  df_diff_count):
    director_path = r"{CWD}\{file_path}".format(CWD=os.getcwd(),file_path=batch_check_path)
    Path(director_path).mkdir(parents=True, exist_ok=True)
    file_path = r"{CWD}\{file_path}\{table_name}.xlsx".format(table_name=file_name, CWD=os.getcwd(),file_path=batch_check_path)
    writer = pd.ExcelWriter(file_path)
    df_merge_count.to_excel(writer, 'CHECK_COUNT', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_diff_count.to_excel(writer, 'COUNT_ERROR', header=True, float_format="%.8f", index=None, encoding="utf8")
    writer.close()


def compare_2_table_count(table_a, extractor_a, connection_a, table_b, extractor_b, connection_b):
    logger.info('CHECK COUNT ------')
    sql_a = number_sql.make_sql_select_count(table_a)
    logger.info('SQL 1:  %s' % (sql_a))
    df_a = connection_a.read_sql_query(sql_a)

    sql_b = number_sql.make_sql_select_count(table_b)
    logger.info('SQL 2:  %s' % (sql_b))
    df_b = connection_b.read_sql_query(sql_b)

    df_diff = pd.concat([df_a, df_b]).drop_duplicates(keep=False)
    df_merge = pd.concat([helper.add_cloud_column(df_a, 'Location', extractor_a.full_table_name),
                          helper.add_cloud_column(df_b, 'Location', extractor_b.full_table_name)])

    temp_name = "{schema_name_a}_{table_name_a}_compare_to_{schema_name_b}_{table_name_b}".format(
        schema_name_a=extractor_a.schema_name,
        table_name_a=extractor_a.table_name,
        schema_name_b=extractor_b.schema_name,
        table_name_b=extractor_b.table_name)

    if (df_diff.size == 0):
        logger.info('CHECK COUNT PASS ------')
        return True, df_diff, df_merge, temp_name
    else:
        logger.info('CHECK COUNT FAIL ------')
        return False, df_diff, df_merge, temp_name


def compare_2_table_number(table_a, extractor_a, connection_a, table_b, extractor_b, connection_b):
    logger.info('CHECK NUMBER COLUMN ------')
    metadata_a = connection_a.read_sql_query(extractor_a.get_sql_datatype_src())
    list_columns_a = helper.get_list_columns_number(metadata_a)
    if (list_columns_a.size > 0):
        sql_a = number_sql.make_sql_select_number(list_columns_a, table_a)
        logger.info('SQL 1:  %s' % (sql_a))
        df_a = connection_a.read_sql_query(sql_a)
    else:
        df_a = pd.DataFrame({"Number_check": ['NOT HAVE COLUMN']})

    metadata_b = connection_b.read_sql_query(extractor_b.get_sql_datatype_src())
    list_columns_b = helper.get_list_columns_number(metadata_b)
    if (list_columns_b.size > 0):
        sql_b = number_sql.make_sql_select_number(list_columns_b, table_b)
        logger.info('SQL 2:  %s' % (sql_b))
        df_b = connection_b.read_sql_query(sql_b)
    else:
        df_b = pd.DataFrame({"Number_check": ['NOT HAVE COLUMN']})

    df_diff = pd.concat([df_a, df_b]).drop_duplicates(keep=False)
    df_merge = pd.concat([helper.add_cloud_column(df_a, 'Location', extractor_a.full_table_name),
                          helper.add_cloud_column(df_b, 'Location', extractor_b.full_table_name)])
    # temp_name = "{schema_name_a}_{table_name_a}_compare_to_{schema_name_b}_{table_name_b}".format(
    #     schema_name_a=extractor_a.schema_name,
    #     table_name_a=extractor_a.table_name,
    #     schema_name_b=extractor_b.schema_name,
    #     table_name_b=extractor_b.table_name)
    if (df_diff.size == 0):
        logger.info('CHECK COLUMN NUMBER PASS ------')
        return True, df_diff, df_merge
    else:
        logger.info('CHECK COLUMN NUMBER FAIL ------')
        return False, df_diff, df_merge


def compare_2_table_not_number(table_a, extractor_a, connection_a, table_b, extractor_b, connection_b):
    logger.info('CHECK NOT NUMBER COLUMN ------')
    metadata_a = connection_a.read_sql_query(extractor_a.get_sql_datatype_src())
    list_columns_a = helper.get_list_columns_not_number(metadata_a)
    if (list_columns_a.size > 0):
        sql_a = not_number_sql.make_sql_select_not_number(list_columns_a, table_a, 100)
        logger.info('SQL 1:  %s' % (sql_a))
        df_a = connection_a.read_sql_query(sql_a)
    else:
        df_a = pd.DataFrame({"Not_Number_check": ['NOT HAVE COLUMN']})

    metadata_b = connection_b.read_sql_query(extractor_b.get_sql_datatype_src())
    list_columns_b = helper.get_list_columns_not_number(metadata_b)
    if (list_columns_b.size > 0):
        sql_b = not_number_sql.make_sql_select_not_number(list_columns_b, table_b, 100)
        logger.info('SQL 2:  %s' % (sql_b))
        df_b = connection_b.read_sql_query(sql_b)
    else:
        df_b = pd.DataFrame({"Not_Number_check": ['NOT HAVE COLUMN']})

    df_diff = pd.concat([df_a, df_b]).drop_duplicates(keep=False)
    df_merge = pd.concat([helper.add_cloud_column(df_a, 'Location', extractor_a.full_table_name),
                          helper.add_cloud_column(df_b, 'Location', extractor_b.full_table_name)])
    if (df_diff.size == 0):
        logger.info('CHECK COLUMN STRING PASS ------')
        return True, df_diff, df_merge
    else:
        logger.info('CHECK COLUMN STRING FAIL ------')
        return False, df_diff, df_merge


def compare_2_table(src_a, table_a, src_b, table_b):
    connection_a = Connector_factory.create_connector(src_a, DefaultVar.DEV_ENV)
    extractor_a = Extractor_factory.create_extractor(connection_a.engine, table_a, src_a)
    extractor_b = Extractor_factory.create_extractor(DefaultVar.MSSQL, table_b, src_b)
    connection_b = Connector_factory.create_connector(src_b, DefaultVar.DEV_ENV)
    logger.info('-------------------------------------------------')
    logger.info('BEGIN COMPARE %s - %s ' % (extractor_a.full_table_name, extractor_b.full_table_name))

    count_check, df_diff_count, df_merge_count, compare_table_name = compare_2_table_count(table_a, extractor_a, connection_a, table_b,
                                                                       extractor_b, connection_b)
    if(count_check):
        number_check, df_diff_number, df_merge_number = compare_2_table_number(table_a, extractor_a,connection_a,
                                                                                table_b, extractor_b, connection_b)
        string_check, df_diff_string, df_merge_string = compare_2_table_not_number(table_a, extractor_a, connection_a,
                                                                                   table_b, extractor_b, connection_b)

        if (number_check and string_check):
            file_name = "PASS_" + compare_table_name
            helper.add_cloud_column(df_merge_count, 'Check', 'OK')
            helper.add_cloud_column(df_merge_number, 'Check', 'OK')
            helper.add_cloud_column(df_merge_string, 'Check', 'OK')
            logger.info('PASS: %s - %s ' % (extractor_a.full_table_name, extractor_b.full_table_name))
            logger.info('\n' + art.text2art('SUCCESS'))

        else:
            file_name = "FAIL_LOGIC_" + compare_table_name
            logger.info('FAIL: %s ' % (extractor_a.full_table_name))
            if (count_check):
                logger.info('FAIL: - Correct COUNT - %s ' % (extractor_a.full_table_name))
                helper.add_cloud_column(df_merge_count, 'Check', 'OK')
            if (number_check):
                logger.info('FAIL: - Correct NUMBER - %s ' % (extractor_a.full_table_name))
                helper.add_cloud_column(df_merge_number, 'Check', 'OK')
            if (string_check):
                logger.info('FAIL: - Correct STRING - %s ' % (extractor_a.full_table_name))
                helper.add_cloud_column(df_merge_string, 'Check', 'OK')
            logger.info('\n' + art.text2art('FAIL'))

        write_to_excel(file_name, df_merge_count, df_merge_number, df_merge_string, df_diff_count, df_diff_number,
                       df_diff_string)
    else:
        file_name = "FAIL_COUNT_" + compare_table_name
        logger.info('FAIL: %s ' % (extractor_a.full_table_name))
        write_to_excel2(file_name, df_merge_count, df_diff_count)

    logger.info('Write result %s finish !' % (file_name))


def run(list_task):
    # try:
    Parallel(n_jobs=10)(delayed(compare_2_table)(src_a, table_a, src_b, table_b) for
                       src_a, table_a, src_b, table_b in list_task)

    helper.make_result_file(director_path_result)
# except Exception as err:
#     print(err)

if __name__ == '__main__':
    logger.info('Started')
    df = pd.read_excel("C:\\Users\\anmv1\\Desktop\\input_check_sample.xlsx")
    run(df.to_records(index=False))
