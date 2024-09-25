import os
from pathlib import Path
import pandas as pd
from src.connector.connector import Connector

# batch_check_path = 'compare_folder_batch_266'
# director_path_result = r"{CWD}\{file_path}".format(CWD=os.getcwd(),file_path=batch_check_path)

def write_to_excel_all(batch_check_path, file_name, df_merge_count, df_merge_number, df_merge_string, df_diff_count, df_diff_number,
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

def write_to_excel_count(batch_check_path, file_name, df_merge_count,  df_diff_count):
    director_path = r"{CWD}\{file_path}".format(CWD=os.getcwd(),file_path=batch_check_path)
    Path(director_path).mkdir(parents=True, exist_ok=True)
    file_path = r"{CWD}\{file_path}\{table_name}.xlsx".format(table_name=file_name, CWD=os.getcwd(),file_path=batch_check_path)
    writer = pd.ExcelWriter(file_path)
    df_merge_count.to_excel(writer, 'CHECK_COUNT', header=True, float_format="%.8f", index=None, encoding="utf8")
    df_diff_count.to_excel(writer, 'COUNT_ERROR', header=True, float_format="%.8f", index=None, encoding="utf8")
    writer.close()

def get_table_name(file_name):
    start_index = file_name.find("_compare_to_") + 12
    end_index = file_name.find(".xlsx")
    return file_name[start_index:end_index]

def make_final_result_file(file_path):
    arr_name = os.listdir(file_path)
    arr_table_name = []
    arr_check = []
    for name_file in arr_name:
        arr_table_name.append(get_table_name(name_file))
        if ("FAIL_COUNT" in name_file):
            arr_check.append("FAIL_COUNT")
        elif ("FAIL_LOGIC" in name_file):
            arr_check.append("FAIL_LOGIC")
        elif ("FAIL_STRING" in name_file):
            arr_check.append("FAIL_STRING")
        else:
            arr_check.append("PASS")

    df = pd.DataFrame({'File_name': arr_name, 'Table_name': arr_table_name, 'Result': arr_check})
    df.to_excel(f'{file_path}\Result_Final.xlsx'.format(file_path=file_path), index=False)


