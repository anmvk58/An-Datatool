import tkinter
import pandas as pd
from tkinter import *
from tkinter import scrolledtext
from tkinter import font

from common.config_reader import Config_reader
from compare_module.Compare_run import compare_2_table_fast_mode
from compare_module.Compare_run import compare_2_query_normal

config_reader = Config_reader()
# list_cnn_config = ["EFICAZ", "VPB_CTRL_OWNER", "LOSRPT_SME_OWNER", "VPB_STAG_OTHER", "WHR2", "JAVIS", "WHR2_OLD", "WHR2_NEW", "S19_DMC"]
list_cnn_config = config_reader.get_list_src_name()

def write_log(content, wrap_line=False):
    if(wrap_line):
        result.insert(END, content)
        result.insert(END, '\n')
    else:
        result.insert(END, content)

def compare_adhoc():
    write_log('*** St'
              'art compare ***', True)
    src_name_1 = config_reader.convert_to_src_name(list_cnn_name_a.get())
    src_name_2 = config_reader.convert_to_src_name(list_cnn_name_b.get())
    query_1 = txt_sql_query_table_a.get("1.0",END)
    query_2 = txt_sql_query_table_b.get("1.0",END)
    try:
        check, path = compare_2_query_normal(src_name_1, query_1.strip(), src_name_2, query_2.strip())
    except pd.io.sql.DatabaseError as e:
        write_log(e, True)

    write_log('----------- RESULTS -----------', True)
    if(check):
        write_log("Pass, 2 queries equals", True)
    else:
        write_log("Fail, please check more details in result file !", True)
        write_log("File Path: " + path , True)
    write_log('-----------------------------', True)
    write_log('*** Finished Compare ***', True)

def reset_form():
    list_cnn_name_a.set("Select Connection Name")
    list_cnn_name_b.set("Select Connection Name")
    txt_sql_query_table_a.delete(1.0, END)
    txt_sql_query_table_b.delete(1.0, END)
    result.delete(1.0, END)


root = Tk()
root.title('Compare Data Tools')
root.geometry("1200x800")
# root.resizable(width=False, height=False)

# Add menu
menu = Menu(root)
root.config(menu=menu)
filemenu = Menu(menu)
menu.add_cascade(label='File', menu=filemenu)
filemenu.add_command(label='New')
filemenu.add_command(label='Open...')
filemenu.add_separator()
filemenu.add_command(label='Exit', command=root.quit)
helpmenu = Menu(menu)
menu.add_cascade(label='Help', menu=helpmenu)
helpmenu.add_command(label='About')
# End add menu

# Chia bố cục
# sql_source_a = tkinter.Frame(master=root, width=800, height=200,  borderwidth = 1, relief=RIDGE)
sql_source_a = tkinter.Frame(master=root, width=1200, height=300)
sql_source_a.pack(pady=5, fill=tkinter.BOTH, expand=True)

sql_source_b = tkinter.Frame(master=root, width=1200, height=300)
sql_source_b.pack(pady=5, fill=tkinter.BOTH, expand=True)

run_area = tkinter.Frame(master=root, width=1200, height=300)
run_area.pack(pady=5, fill=tkinter.BOTH, expand=True)
# Done Chia bố cục

# add element in Source A
label_src_a = Label(sql_source_a, text="Source A", font=("Arial", 14), anchor='w').pack(padx=5, fill=tkinter.BOTH, expand=False)

frame_cnn_a = tkinter.Frame(master=sql_source_a)
frame_cnn_a.pack(fill=tkinter.BOTH, expand=False)
label_list_source_a = Label(frame_cnn_a, text="Connection", font=("Calibri", 12), width=12, anchor='e').pack(fill=tkinter.BOTH,side=tkinter.LEFT, expand=False)
list_cnn_name_a = StringVar(frame_cnn_a)
list_cnn_name_a.set("Select Connection Name") # default value
txt_drop_downlist_cnn_a = OptionMenu(frame_cnn_a, list_cnn_name_a, *list_cnn_config)
txt_drop_downlist_cnn_a.config(width = 40)
txt_drop_downlist_cnn_a.pack(fill=None, expand=False, anchor='w')

frame_sql_a = tkinter.Frame(master=sql_source_a)
frame_sql_a.pack(fill=tkinter.BOTH, expand=True)
label_sql_table_a = Label(frame_sql_a, text="Query/Table", font=("Calibri", 12), width=12, anchor='ne').pack(fill=tkinter.BOTH, side=tkinter.LEFT, expand=False)
txt_sql_query_table_a = scrolledtext.ScrolledText(frame_sql_a, height=5, width=75, bg="light yellow", wrap='word', font = ("Courier New", 12))
txt_sql_query_table_a.pack(padx=(2,0), fill=tkinter.BOTH, expand=True)

# add element in Source B
label_src_b = Label(sql_source_b, text="Source B", font=("Arial", 14), anchor='w').pack(padx=5, fill=tkinter.BOTH, expand=False)

frame_cnn_b = tkinter.Frame(master=sql_source_b)
frame_cnn_b.pack(fill=tkinter.BOTH, expand=False)
label_list_source_b = Label(frame_cnn_b, text="Connection", font=("Calibri", 12), width=12, anchor='e').pack(fill=tkinter.BOTH,side=tkinter.LEFT, expand=False)
list_cnn_name_b = StringVar(frame_cnn_b)
list_cnn_name_b.set("Select Connection Name") # default value
txt_drop_downlist_cnn_b = OptionMenu(frame_cnn_b, list_cnn_name_b, *list_cnn_config)
txt_drop_downlist_cnn_b.config(width = 40)
txt_drop_downlist_cnn_b.pack(fill=None, expand=False, anchor='w')

frame_sql_b = tkinter.Frame(master=sql_source_b)
frame_sql_b.pack(fill=tkinter.BOTH, expand=True)
label_sql_table_b = Label(frame_sql_b, text="Query/Table", font=("Calibri", 12), width=12, anchor='ne').pack(fill=tkinter.BOTH, side=tkinter.LEFT, expand=False)
txt_sql_query_table_b = scrolledtext.ScrolledText(frame_sql_b, height=5, width=75, bg="light yellow", wrap='word', font = ("Courier New", 12))
txt_sql_query_table_b.pack(padx=(2,0), fill=tkinter.BOTH, expand=True)


# add element in Run Area
lbl_log = Label(master=run_area, text="Log run: ", anchor='w').pack(padx=5, fill=tkinter.X, expand=False)
result = scrolledtext.ScrolledText(run_area, height=10, width=100, bg="white", wrap='word')
result.pack(padx=(5,0), pady=5, fill=tkinter.BOTH, expand=True)

btn_run = tkinter.Button(master=run_area, text ="Start Compare", anchor='se', command=compare_adhoc).pack(padx=5, pady=0, fill=tkinter.Y, side=tkinter.RIGHT, expand=False)
btn_run = tkinter.Button(master=run_area, text ="Reset", anchor='se', command=reset_form).pack(padx=5, pady=0, fill=tkinter.Y, side=tkinter.RIGHT, expand=False)
lbl_author = Label(master=run_area, text="AnMV1 - ©2022", anchor='w').pack(padx=5, side=tkinter.LEFT, expand=False)

# Button(root,text='get label font',command=lambda: print(font.nametofont(result['font']).configure()["family"])).pack()
mainloop()
