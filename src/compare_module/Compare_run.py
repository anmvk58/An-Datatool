from datetime import datetime
import os
import pandas as pd

from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar
from src.compare_module.Compare_object import Compare_Object_Fast
from src.compare_module.Compare_object import Compare_Object_Query
import src.compare_module.Compare_core as compare_core
import src.compare_module.Compare_helpers as compare_helper


director_path_result = r"{CWD}\{file_path}".format(CWD=os.getcwd(), file_path=DefaultVar.BATCH_CHECK_PATH)

def compare_2_table_fast_mode(src_name_1, query_1, src_name_2, query_2):
    cof_1 = Compare_Object_Fast(src_name_1, query_1)
    cof_2 = Compare_Object_Fast(src_name_2, query_2)

    check_count, df_diff_count, df_merge_count = compare_core.compare_2_table_count(cof_1, cof_2)

    if(check_count):
        df_merge_count['Check'] = 'OK'
        # pass check count
        check_number, df_diff_number, df_merge_number = compare_core.compare_2_table_number(cof_1, cof_2)
        check_string, df_diff_string, df_merge_string = compare_core.compare_2_table_not_number(cof_1, cof_2)
        if(check_number and check_string):
            file_name = 'PASS_' + cof_1.command.schema_table + '_compare_to_' + cof_2.command.schema_table
            df_merge_number['Check'] = 'OK'
            df_merge_string['Check'] = 'OK'
        else:
            if(not check_number):
                file_name = 'FAIL_NUMBER_' + cof_1.command.schema_table + '_compare_to_' + cof_2.command.schema_table
                df_merge_number['Check'] = 'FAIL'
            else:
                df_merge_number['Check'] = 'OK'

            if(not check_string):
                file_name = 'FAIL_STRING_' + cof_1.command.schema_table + '_compare_to_' + cof_2.command.schema_table
                df_merge_string['Check'] = 'FAIL'
            else:
                df_merge_string['Check'] = 'OK'

        compare_helper.write_to_excel_all(batch_check_path=DefaultVar.BATCH_CHECK_PATH,
                                       file_name=file_name,
                                       df_merge_count=df_merge_count, df_merge_number=df_merge_number, df_merge_string=df_merge_string,
                                       df_diff_count=df_diff_count, df_diff_number=df_diff_number, df_diff_string=df_diff_string)
    else:
        # fail check count
        file_name = 'FAIL_COUNT_' + cof_1.command.schema_table + '_compare_to_' + cof_2.command.schema_table
        compare_helper.write_to_excel_count(batch_check_path=DefaultVar.BATCH_CHECK_PATH,
                                             file_name=file_name,
                                             df_merge_count=df_merge_count,
                                             df_diff_count=df_diff_count)

def compare_2_query_normal(src_name_1, query_1, src_name_2, query_2):
    coq_1 = Compare_Object_Query(src_name_1, query_1)
    coq_2 = Compare_Object_Query(src_name_2, query_2)

    try:
        check_query, df_diff, message = compare_core.compare_2_query(coq_1, coq_2)
    except Exception as e:
        raise Exception(e)

    full_path = ''

    if(check_query):
        return True, full_path, message
        print("Check Success !")
    else:
        if df_diff is not None:
            to_day = datetime.today().strftime('%Y%m%d')
            now = datetime.today().strftime('%H_%M_%S')
            file_name = f"{to_day}_{src_name_1}_{src_name_2}_{now}.csv"
            path = os.path.normpath(os.getcwd() + os.sep + os.pardir)
            full_path = path + '\compare_results\\' + file_name
            df_diff.to_csv(full_path, float_format='%f', encoding='utf-8', index=False)
            print("Check Fail, please check Result File !")
            return False, full_path, message
        else:
            return False, full_path, message

def compare_2_query_long_table(src_name_1, query_1, src_name_2, query_2, chunk_size, view_object):
    cnn_1 = Connector_factory.create_connector(src_name_1, config_path=DefaultVar.DEV_ENV)
    cnn_2 = Connector_factory.create_connector(src_name_2, config_path=DefaultVar.DEV_ENV)
    df_full_1 = pd.read_sql(query_1, cnn_1.connection[src_name_1], chunksize=chunk_size, coerce_float=False)
    df_full_2 = pd.read_sql(query_2, cnn_2.connection[src_name_2], chunksize=chunk_size, coerce_float=False)
    full_path = ''
    final_result = False
    batch = 0
    while True:
        # Get DF 1
        try:
            df_1 = next(df_full_1)
        except StopIteration:
            df_1 = pd.DataFrame()

        # Get DF 2
        try:
            df_2 = next(df_full_2)
        except StopIteration:
            df_2 = pd.DataFrame()

        # Check length of 2 DF:
        if(df_1.shape[0] == 0 and df_2.shape[0] == 0 ):
            break
        # Convert column Date:

        # Compare 2 DF:
        batch += 1
        print("Process Batch: " + str(batch))
        try:
            check_query, df_diff, message = compare_core.compare_2_data_frame(df_1, df_2, src_name_1, src_name_2)
        except Exception as e:
            raise Exception(e)
            view_object.write_log("Exception: " + e)

        if (check_query):
            final_result = True
            view_object.write_log("Process Batch: " + str(batch) + "| PASS")
            continue
        else:
            if df_diff is not None:
                to_day = datetime.today().strftime('%Y%m%d')
                now = datetime.today().strftime('%H_%M_%S')
                file_name = f"{to_day}_{src_name_1}_{src_name_2}_{now}.csv"
                path = os.path.normpath(os.getcwd() + os.sep + os.pardir)
                full_path = path + '\compare_results\\' + file_name
                df_diff.to_csv(full_path, float_format='%f', encoding='utf-8', index=False)
                print("Check Fail, please check Result File !")
                final_result = False
                view_object.write_log("Process Batch: " + str(batch) + " | FAIL")
                view_object.write_log("Stopped")
                break
            else:
                final_result = False
                view_object.write_log("Process Batch: " + str(batch) + " | FAIL")
                view_object.write_log("Stopped")
                break

    return final_result, full_path, message

def compare_file_and_database(file_path, file_type, src_name, query):
    """
    :param file_path: location of file that use to compare
    :type file_path: str
    :param src_name: definition of connection database
    :type src_name: str
    :param query: SQL query that use to get data from database to compare
    :type query: str
    :return: result of compare
    """
    coq = Compare_Object_Query(src_name, query)

    try:
        check_query, df_diff, message = compare_core.compare_data_frame_with_file(file_path, file_type, coq)
    except Exception as e:
        raise Exception(e)

    full_path = ''

    if (check_query):
        return True, full_path, message
        print("Check Success !")
    else:
        if df_diff is not None:
            to_day = datetime.today().strftime('%Y%m%d')
            now = datetime.today().strftime('%H_%M_%S')
            file_name = f"FILE_{file_type.upper()}_{to_day}_{src_name}_{now}.csv"
            path = os.path.normpath(os.getcwd() + os.sep + os.pardir)
            full_path = path + '\compare_results\\' + file_name
            df_diff.to_csv(full_path, float_format='%f', encoding='utf-8', index=False)
            print("Check Fail, please check Result File !")
            return False, full_path, message
        else:
            return False, full_path, message

if __name__ == '__main__':
    src_name_1 = 'EFICAZ'
    src_name_2 = 'VPB_COLLECTION_STAGING'
    query_1 = """
        SELECT NVL(a.VPB_OVD_DETAIL_OTHER,0) AS VPB_OVD_DETAIL_OTHER, a.OVERDRAFT_TYPE, a.END_DATE, b.M , b.S , b.DUE_DATE , b.GRACE_DATE 
        FROM EFZ_VPB_OVD_OTHER a
        JOIN EFZ_VPB_OVD_OTHER_DETAILS b ON a.VPB_OVD_DETAIL_OTHER = b.VPB_OVD_DETAIL_OTHER
        WHERE b.M = 1 AND b.S = 1
        ORDER BY 
        VPB_OVD_DETAIL_OTHER, OVERDRAFT_TYPE, END_DATE, M, S, DUE_DATE, GRACE_DATE
    """
    query_2 = """
        SELECT ISNULL(a.VPB_OVD_DETAIL_OTHER,0) AS VPB_OVD_DETAIL_OTHER, a.OVERDRAFT_TYPE, a.END_DATE, b.M , b.S , b.DUE_DATE , b.GRACE_DATE 
        FROM VPB_COLLECTION_STAGING.core.EFZ_VPB_OVD_OTHER a
        JOIN VPB_COLLECTION_STAGING.core.EFZ_VPB_OVD_OTHER_DETAILS b ON a.VPB_OVD_DETAIL_OTHER = b.VPB_OVD_DETAIL_OTHER
        WHERE b.M = 1 and b.S = 1
        ORDER BY 
        VPB_OVD_DETAIL_OTHER, OVERDRAFT_TYPE, END_DATE, M, S, DUE_DATE, GRACE_DATE
    """
    final_result, full_path, message = compare_2_query_long_table(src_name_1, query_1, src_name_2, query_2)
    print(final_result)
    print(full_path)
    print(message)

    # print(datetime.today().strftime('%Y%m%d_%H_%M_%S'))
