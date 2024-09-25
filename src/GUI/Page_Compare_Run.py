import tkinter as tk
from tkinter import *
from tkinter import scrolledtext
from src.GUI.Page_Page import Page
from common.config_reader import Config_reader
from compare_module.Compare_run import compare_2_table_fast_mode
import src.compare_module.Compare_run as compare_run

config_reader = Config_reader()
list_cnn_config = config_reader.get_list_src_name()


class Compare_Run(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        sql_source_a = tk.Frame(master=self, width=1000, height=300)
        sql_source_a.pack(pady=5, fill=tk.BOTH, expand=True)

        sql_source_b = tk.Frame(master=self, width=1000, height=300)
        sql_source_b.pack(pady=5, fill=tk.BOTH, expand=True)

        run_mode = tk.Frame(master=self, width=1000, height=15)
        run_mode.pack(pady=(10,0), fill=tk.X, expand=False)

        self.config_area = tk.Frame(master=self, width=1000, height=21)
        self.config_area.pack(fill=tk.X, expand=False)

        run_area = tk.Frame(master=self, width=1000, height=300)
        run_area.pack(pady=(0,5), fill=tk.BOTH, expand=True)
        # Done Chia bố cục

        # add element in run_mode
        self.var = IntVar(None, 1)
        normal_mode = Radiobutton(run_mode, text="Query Mode", variable=self.var, value=1, command=self.show_chunk_size)
        normal_mode.pack(side=tk.RIGHT, padx=(0,10))

        self.chunk_size = tk.StringVar()
        self.chunk_size.set("100000")
        self.text_chunk = Entry(self.config_area, width=10, textvariable=self.chunk_size)
        self.label_chunk = Label(self.config_area, text="Chunk Size:")
        # self.text_chunk.pack(side=tk.RIGHT, padx=(0, 15))
        # self.label_chunk.pack(side=tk.RIGHT, padx=(0, 5))
        # self.text_chunk.config(state="disabled")

        batch_mode = Radiobutton(run_mode, text="Batch Mode", variable=self.var, value=2, command=self.show_chunk_size)
        batch_mode.pack(side=tk.RIGHT, padx=(0,5))

        fast_mode = Radiobutton(run_mode, text="Fast Mode", variable=self.var, value=3, command=self.show_chunk_size)
        fast_mode.pack(side=tk.RIGHT, padx=(0,5))

        # add element in Source A
        label_src_a = Label(sql_source_a, text="Source A", font=("Arial", 14), anchor='w')
        label_src_a.pack(padx=5, fill=tk.BOTH, expand=False)
        frame_cnn_a = tk.Frame(master=sql_source_a)
        frame_cnn_a.pack(fill=tk.BOTH, expand=False)
        label_list_source_a = Label(frame_cnn_a, text="Connection", font=("Calibri", 12), width=12, anchor='e')
        label_list_source_a.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.list_cnn_name_a = StringVar(frame_cnn_a)
        self.list_cnn_name_a.set("Select Connection")  # default value
        self.txt_drop_downlist_cnn_a = OptionMenu(frame_cnn_a, self.list_cnn_name_a, *list_cnn_config)
        self.txt_drop_downlist_cnn_a.config(width=55)
        self.txt_drop_downlist_cnn_a.pack(fill=None, side=tk.LEFT, expand=False, anchor='w')
        icon = PhotoImage(file='../../resources/icons/refresh.png')
        btn_reload = tk.Button(master=frame_cnn_a, image=icon, relief=FLAT, command=lambda: self.reload_cnn())
        btn_reload.image = icon
        btn_reload.pack(fill=tk.Y, expand=True, anchor="w", padx=(5,0))

        frame_sql_a = tk.Frame(master=sql_source_a)
        frame_sql_a.pack(fill=tk.BOTH, expand=True)
        label_sql_table_a = Label(frame_sql_a, text="Query/Table", font=("Calibri", 12), width=12, anchor='ne')
        label_sql_table_a.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.txt_sql_query_table_a = scrolledtext.ScrolledText(frame_sql_a, height=5, width=75, bg="white", wrap='word',
                                                               font=("Courier New", 12), undo=True)
        self.txt_sql_query_table_a.pack(padx=(2, 0), fill=tk.BOTH, expand=True)

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
        self.txt_drop_downlist_cnn_b.pack(fill=None, expand=False, anchor='w')

        frame_sql_b = tk.Frame(master=sql_source_b)
        frame_sql_b.pack(fill=tk.BOTH, expand=True)
        label_sql_table_b = Label(frame_sql_b, text="Query/Table", font=("Calibri", 12), width=12, anchor='ne')
        label_sql_table_b.pack(fill=tk.BOTH, side=tk.LEFT, expand=False)
        self.txt_sql_query_table_b = scrolledtext.ScrolledText(frame_sql_b, height=5, width=75, bg="white", wrap='word',
                                                               font=("Courier New", 12), undo=True)
        self.txt_sql_query_table_b.pack(padx=(2, 0), fill=tk.BOTH, expand=True)

        # add element in Run Area
        lbl_log = Label(master=self.config_area, text="Log run: ", anchor='w')
        lbl_log.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=False)
        self.result = scrolledtext.ScrolledText(run_area, height=10, width=100, bg="light yellow", wrap='word')
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

    def show_chunk_size(self):
        choice = self.var.get()
        self.chunk_size = self.text_chunk.get().strip()
        # self.text_chunk.config(state= "disabled")
        self.text_chunk.pack_forget()
        self.label_chunk.pack_forget()
        # if choice == 2 and self.text_chunk["state"] == "disabled":
        if choice == 2:
            self.text_chunk.pack(side=tk.RIGHT, padx=(0, 15))
            self.label_chunk.pack(side=tk.RIGHT, padx=(0, 5))

    def reset_form(self):
        self.list_cnn_name_a.set("Select Connection")
        self.list_cnn_name_b.set("Select Connection")
        self.txt_sql_query_table_a.delete(1.0, END)
        self.txt_sql_query_table_b.delete(1.0, END)
        self.result.config(state="normal")
        self.result.delete(1.0, END)
        self.result.configure(state="disabled")

    def reload_cnn(self, *args):
        list_cnn_config = config_reader.get_list_src_name()
        menu_a = self.txt_drop_downlist_cnn_a["menu"]
        menu_b = self.txt_drop_downlist_cnn_b["menu"]
        menu_a.delete(0, "end")
        menu_b.delete(0, "end")
        for string in list_cnn_config:
            menu_a.add_command(label=string, command=lambda value=string: self.list_cnn_name_a.set(value))
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
        src_name_1 = config_reader.convert_to_src_name(self.list_cnn_name_a.get())
        src_name_2 = config_reader.convert_to_src_name(self.list_cnn_name_b.get())
        query_1 = self.txt_sql_query_table_a.get("1.0", END).replace("\n", " ")
        query_2 = self.txt_sql_query_table_b.get("1.0", END).replace("\n", " ")
        choice = self.var.get()

        if (src_name_1 != 'Select Connection' and src_name_2 != 'Select Connection' and query_1 != '' and query_2 != ''):
            self.write_log('***** START COMPARE *****')
            try:
                if choice == 1:
                    self.write_log('----------- Query Mode -----------')
                    check, path, message = compare_run.compare_2_query_normal(src_name_1, query_1.strip(), src_name_2,
                                                                              query_2.strip())
                elif choice == 2:
                    self.chunk_size = self.text_chunk.get().strip()
                    try:
                        chunk_sz = int(self.chunk_size)
                        if chunk_sz > 0:
                            self.write_log('----------- Batch Mode -----------')
                            check, path, message = compare_run.compare_2_query_long_table(src_name_1, query_1.strip(),
                                                                                          src_name_2, query_2.strip(),
                                                                                          chunk_sz, self)
                        else:
                            self.write_log('Chunk size phải là một số nguyên lớn hơn 0')
                    except ValueError as e:
                        self.write_log('Chunk size không đúng định dạng !!!')
                        self.write_log(e)
                        # raise Exception("Chunk size phải là một số nguyên lớn hơn 0")
                    except Exception as e:
                        self.write_log(e)

                elif choice == 3:
                    self.write_log('----------- Fast Mode -----------')
                    message = 'This Feature is comming !'
                    path = ''

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