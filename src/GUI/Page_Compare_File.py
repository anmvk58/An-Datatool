import tkinter as tk
from tkinter import *
from tkinter import scrolledtext
from tkinter import filedialog
from src.GUI.Page_Page import Page
from common.config_reader import Config_reader
import src.compare_module.Compare_run as compare_run

config_reader = Config_reader()
list_cnn_config = config_reader.get_list_src_name()

class CompareFile(Page):
    def __init__(self, *args, **kwargs):
        self.filename = ''

        Page.__init__(self, *args, **kwargs)
        # file_source = tk.Frame(master=self, width=1000, height=150, bg="yellow")
        file_source = tk.Frame(master=self, width=1000, height=150)
        file_source.pack(pady=5, fill=tk.BOTH, expand=False)

        sql_source_b = tk.Frame(master=self, width=1000, height=500)
        sql_source_b.pack(pady=5, fill=tk.BOTH, expand=True)

        # run_mode = tk.Frame(master=self, width=1000, height=15)
        # run_mode.pack(pady=(10, 0), fill=tk.X, expand=False)

        self.config_area = tk.Frame(master=self, width=1000, height=21)
        self.config_area.pack(fill=tk.X, expand=False)

        run_area = tk.Frame(master=self, width=1000, height=200)
        run_area.pack(pady=(0, 5), fill=tk.BOTH, expand=True)
        # Done Chia bố cục

        # add element in Source A
        label_src_a = Label(file_source, text="Source A", font=("Arial", 14), anchor='w')
        label_src_a.pack(padx=5, fill=tk.BOTH, expand=False)
        frame_cnn_a = tk.Frame(master=file_source)
        frame_cnn_a.pack(fill=tk.BOTH, expand=False)
        label_list_source_a = Label(frame_cnn_a, text="Choose File", font=("Calibri", 12), width=12, anchor='e')
        label_list_source_a.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        icon_file = PhotoImage(file='../../resources/icons/note.png')
        button_explore = Button(frame_cnn_a, image=icon_file, relief=FLAT, command=lambda: self.browseFiles())
        button_explore.image = icon_file
        button_explore.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)

        frame_sql_a = tk.Frame(master=file_source)
        frame_sql_a.pack(fill=tk.BOTH, expand=False)
        label_sql_table_a = Label(frame_sql_a, text="", font=("Calibri", 12), width=12, anchor='ne')
        label_sql_table_a.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.label_file_explorer = Label(frame_sql_a, text="No File Chosen", font=("Calibri", 12), fg="blue")
        self.label_file_explorer.pack(fill=None, side=tk.LEFT, expand=False, anchor='nw')

        # add element in Source B
        label_src_b = Label(sql_source_b, text="Source B", font=("Arial", 14), anchor='w')
        label_src_b.pack(padx=5, fill=tk.BOTH, expand=False)
        frame_cnn_b = tk.Frame(master=sql_source_b)
        frame_cnn_b.pack(fill=tk.BOTH, expand=False)
        label_list_source_b = Label(frame_cnn_b, text="Connection", font=("Calibri", 12), width=12, anchor='e')
        label_list_source_b.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.list_cnn_name_b = StringVar(frame_cnn_b)
        self.list_cnn_name_b.set("Select Connection")  # default value
        self.txt_drop_downlist_cnn_b = OptionMenu(frame_cnn_b, self.list_cnn_name_b, *list_cnn_config)
        self.txt_drop_downlist_cnn_b.config(width=55)
        self.txt_drop_downlist_cnn_b.pack(fill=None, side=tk.LEFT, expand=False, anchor='w')
        icon = PhotoImage(file='../../resources/icons/refresh.png')
        btn_reload = tk.Button(master=frame_cnn_b, image=icon, relief=FLAT, command=lambda: self.reload_cnn())
        btn_reload.image = icon
        btn_reload.pack(fill=tk.Y, expand=True, anchor="w", padx=(5, 0))

        frame_sql_b = tk.Frame(master=sql_source_b)
        frame_sql_b.pack(fill=tk.BOTH, expand=True)
        label_sql_table_b = Label(frame_sql_b, text="Query/Table", font=("Calibri", 12), width=12, anchor='ne')
        label_sql_table_b.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.txt_sql_query_table_b = scrolledtext.ScrolledText(frame_sql_b, height=6, width=75, bg="white", wrap='word',
                                                               font=("Courier New", 12), undo=True)
        self.txt_sql_query_table_b.pack(padx=(2, 0), fill=tk.BOTH, expand=True)

        # add element in Run Area
        lbl_log = Label(master=self.config_area, text="Log run: ", anchor='w')
        lbl_log.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=False)
        self.result = scrolledtext.ScrolledText(run_area, height=5, width=100, bg="light yellow", wrap='word')
        self.result.configure(state="disabled")
        self.result.pack(padx=(5, 0), pady=5, fill=tk.BOTH, expand=True)

        btn_run = tk.Button(master=run_area, text="Start Compare", font=("Calibri", 12), bg='#C70039',
                            fg='#ffffff', command=lambda: self.compare_querry())
        btn_run.pack(padx=5, pady=0, fill=tk.Y, side=tk.RIGHT, expand=False)
        btn_clear_log = tk.Button(master=run_area, text="Clear Log", font=("Calibri", 12), bg='#0a75ad',
                                  fg='#ffffff', command=lambda: self.clear_log())
        btn_clear_log.pack(padx=5, pady=0, fill=tk.Y, side=tk.LEFT, expand=False)

        btn_reset = tk.Button(master=run_area, text="Reset", font=("Calibri", 12), bg='#0a75ad',
                              fg='#ffffff', command=lambda: self.reset_form())
        btn_reset.pack(padx=5, pady=0, fill=tk.Y, side=tk.LEFT, expand=False)

        # lbl_author = Label(master=run_area, text="©2022", anchor='w').pack(padx=5, side=tk.LEFT, expand=False)

    def browseFiles(self):
        self.filename = filedialog.askopenfilename(initialdir="/",
                                              title="Select a File",
                                              filetypes=(("CSV files", "*.csv*"),
                                                         ("Parquet files", "*.parquet*"),
                                                         ("all files","*.*")))

        # Change label contents
        if len(self.filename) > 0:
            self.label_file_explorer.configure(text="File Opened: " + self.filename)

    def reset_form(self):
        self.list_cnn_name_b.set("Select Connection")
        self.txt_sql_query_table_b.delete(1.0, END)
        self.result.config(state="normal")
        self.result.delete(1.0, END)
        self.result.configure(state="disabled")

    def reload_cnn(self, *args):
        list_cnn_config = config_reader.get_list_src_name()
        menu_b = self.txt_drop_downlist_cnn_b["menu"]
        menu_b.delete(0, "end")
        for string in list_cnn_config:
            menu_b.add_command(label=string, command=lambda value=string: self.list_cnn_name_b.set(value))
        self.write_log('Update connection successfully !')

    def clear_log(self):
        self.result.config(state="normal")
        self.result.delete(1.0, END)
        self.result.configure(state="disabled")

    def write_log(self, content, wrap_line=True):
        self.result.config(state="normal")
        if (wrap_line):
            self.result.insert(END, content)
            self.result.insert(END, '\n')
            self.result.see(END)
            self.result.update()
        else:
            self.result.insert(END, content)
            self.result.see(END)
            self.result.update()
        self.result.configure(state="disabled")

    def compare_querry(self):
        src_name_2 = config_reader.convert_to_src_name(self.list_cnn_name_b.get())
        query_2 = self.txt_sql_query_table_b.get("1.0", END).replace("\n", " ")


        if (src_name_2 != 'Select Connection' and query_2 != '' and len(self.filename) > 0):
            self.write_log('***** START COMPARE *****')
            try:
                self.write_log('----------- Compare File vs Database -----------')
                check, path, message = compare_run.compare_file_and_database(self.filename, 'csv', src_name_2, query_2)
            except Exception as e:
                self.write_log('EXCEPTION !!!')
                self.write_log(e)
                self.write_log('\n')
                raise Exception("Some thing went wrong, please send error to Developer!")

            self.write_log('----------- RESULTS -----------')
            self.write_log(message)
            # self.write_log('\n')
            if (path != ""):
                self.write_log("Fail, please check more details in result file !")
                self.write_log("File Path: " + path)
            self.write_log('-------------------------------')
            self.write_log('***** FINISHED COMPARE *****')
            self.write_log('\n')
        else:
            self.write_log('***** Empty input OR Unset Connection !!! *****')
            self.write_log('\n')