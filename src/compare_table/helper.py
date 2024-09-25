import os
import pandas as pd

def get_list_columns_number(df):
    number_df = df[df['SRC_DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'TINYINT', 'REAL'])]['COLUMN_NAME']
    return number_df

def get_list_columns_not_number(df):
    number_df = df[~df['SRC_DATA_TYPE'].isin(['FLOAT', 'NUMERIC', 'INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'TINYINT', 'REAL', 'NTEXT', 'TEXT', 'IMAGE'])]['COLUMN_NAME']
    return number_df

def add_cloud_column(df, title, text):
    list = []
    for i in range(len(df.index)):
        list.append(text)
    df['{TITLE}'.format(TITLE = title)] = list
    return df

def get_table_name(file_name):
    start_index = file_name.find("UPG_WHR2_") + 9
    end_index = file_name.find(".xlsx")
    return file_name[start_index:end_index]

def make_result_file(file_path):
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













