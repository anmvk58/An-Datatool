import os
import tkinter as tk
from collections import deque
from tkinter import *
import json
from src.common.io_utils import IOUtils
from src.common.default_var import DefaultVar
import src.common.encryptor as encryptor
from common.config_reader import Config_reader
import pyodbc
from src.connector.connector_factory import Connector_factory

config_reader = Config_reader()

class CEntry(tk.Entry):
    def __init__(self, master, **kw):
        super().__init__(master=master, **kw)
        self._undo_stack = deque(maxlen=100)
        self._redo_stack = deque(maxlen=100)
        self.bind("<Control-z>", self.undo)
        self.bind("<Control-y>", self.redo)
        # traces whenever the Entry's contents are changed
        self.tkvar = tk.StringVar()
        self.config(textvariable=self.tkvar)
        self.trace_id = self.tkvar.trace("w", self.on_changes)
        self.reset_undo_stacks()
        # USE THESE TO TURN TRACE OFF THEN BACK ON AGAIN
        # self.tkvar.trace_vdelete("w", self.trace_id)
        # self.trace_id = self.tkvar.trace("w", self.on_changes)

    def undo(self, event=None):  # noqa
        if len(self._undo_stack) <= 1:
            return
        content = self._undo_stack.pop()
        self._redo_stack.append(content)
        content = self._undo_stack[-1]
        self.tkvar.trace_vdelete("w", self.trace_id)
        self.delete(0, tk.END)
        self.insert(0, content)
        self.trace_id = self.tkvar.trace("w", self.on_changes)

    def redo(self, event=None):  # noqa
        if not self._redo_stack:
            return
        content = self._redo_stack.pop()
        self._undo_stack.append(content)
        self.tkvar.trace_vdelete("w", self.trace_id)
        self.delete(0, tk.END)
        self.insert(0, content)
        self.trace_id = self.tkvar.trace("w", self.on_changes)

    def on_changes(self, a=None, b=None, c=None):  # noqa
        self._undo_stack.append(self.tkvar.get())
        self._redo_stack.clear()

    def reset_undo_stacks(self):
        self._undo_stack.clear()
        self._redo_stack.clear()
        self._undo_stack.append(self.tkvar.get())

class AddConnection:
    def __init__(self, parent):
        self.top = tk.Toplevel(parent)
        x = parent.winfo_x()
        y = parent.winfo_y()
        self.top.geometry("+%d+%d" % (x + 250, y + 200))
        self.frame = tk.Frame(self.top, width=285, height=500)
        # self.top.geometry('500x285')
        self.top.maxsize(500, 400)
        self.top.minsize(500, 285)
        self.top.title('Add Connection')
        self.top.attributes('-toolwindow', True)
        self.top.attributes('-topmost', True)
        self.top.focus()

        # Src_name
        label_src_name = Label(self.frame, text="Source Name", font=("Calibri", 12))
        label_src_name.grid(sticky="w", row=0, column=0, padx=(20, 0), pady=(20, 0))

        self.text_src_name = CEntry(self.frame, font=("Calibri", 12), width=45)
        self.text_src_name.grid(columnspan=3, row=0, column=1, padx=(0, 15), pady=(20, 0))

        # Engine
        label_engine = Label(self.frame, text="Engine", font=("Calibri", 12))
        label_engine.grid(sticky="w", row=1, column=0, padx=(20, 0), pady=2)

        list_engine_db = ["ORACLE", "MSSQL", "DB2", "REDSHIFT", "MYSQL"]
        self.engine = StringVar(self.frame)
        self.engine.set("Select Engine")
        self.engine.trace("w", self.option_changed)
        engine_drop_list = OptionMenu(self.frame, self.engine, *list_engine_db)
        engine_drop_list.config(width=27)
        engine_drop_list.grid(sticky="w", columnspan=3, row=1, column=1, padx=(0, 15), pady=2)

        # Host
        label_host = Label(self.frame, text="Host", font=("Calibri", 12))
        label_host.grid(sticky="w", row=2, column=0, padx=(20, 0), pady=2)

        self.text_host = CEntry(self.frame, font=("Calibri", 12), width=25)
        self.text_host.grid(sticky="w", row=2, column=1, padx=(0, 15), pady=2)

        # Port
        label_port = Label(self.frame, text="Port", font=("Calibri", 12))
        label_port.grid(sticky="e", row=2, column=2, padx=(15, 0), pady=2)

        self.text_port = CEntry(self.frame, font=("Calibri", 12), width=10)
        self.text_port.grid(sticky="e", row=2, column=3, padx=(10, 15), pady=2)

        # Service
        self.label_service_name_value = tk.StringVar()
        self.label_service_name = Label(self.frame, textvariable=self.label_service_name_value, font=("Calibri", 12))
        # .grid(sticky="w", row=5, column=0, padx=(15, 0), pady=2)
        self.text_service_name = CEntry(self.frame, font=("Calibri", 12), width=45)
        # .grid(columnspan=3, row=5, column=1, padx=(0, 15), pady=2)

        # Username
        label_username = Label(self.frame, text="Username", font=("Calibri", 12))
        label_username.grid(sticky="w", row=4, column=0, padx=(20, 0), pady=2)

        self.text_username = CEntry(self.frame, font=("Calibri", 12), width=45)
        self.text_username.grid(columnspan=3, row=4, column=1, padx=(0, 15), pady=2)

        # Password
        label_pass = Label(self.frame, text="Password", font=("Calibri", 12))
        label_pass.grid(sticky="w", row=5, column=0, padx=(20, 0), pady=2)

        self.text_pass = CEntry(self.frame, font=("Calibri", 12), width=45, show="•")
        self.text_pass.grid(columnspan=3, row=5, column=1, padx=(0, 15), pady=2)

        # Schema
        label_schema = Label(self.frame, text="Schema", font=("Calibri", 12))
        label_schema.grid(sticky="w", row=6, column=0, padx=(20, 0), pady=2)

        self.text_schema = CEntry(self.frame, font=("Calibri", 12), width=45)
        self.text_schema.grid(columnspan=3, row=6, column=1, padx=(0, 15), pady=2)

        # Label Notification
        self.label_noti = Label(self.frame, text="", fg="red", wraplength=350, font=("Calibri", 10))
        self.label_noti.grid(sticky="w", columnspan=3, row=8, column=0, padx=(20, 0), pady=(20, 10))

        # Button config
        self.btn_save = tk.Button(self.frame, font=("Calibri", 12), text="Save", width=8, bg='#0a75ad', fg='#ffffff',
                                  command=lambda: self.add_new_connection())
        self.btn_save.grid(sticky="e", row=8, column=3, padx=(0, 15), pady=(20, 10))

        self.frame.pack()

    def close(self):
        self.top.destroy()

    def option_changed(self, *args):
        if self.engine.get() == 'ORACLE':
            self.label_service_name.grid(sticky="w", row=3, column=0, padx=(20, 0), pady=2)
            self.text_service_name.grid(columnspan=3, row=3, column=1, padx=(0, 15), pady=2)
            self.label_service_name_value.set("Service name")
        elif self.engine.get() == 'DB2' or self.engine.get() == 'REDSHIFT' or self.engine.get() == 'MYSQL':
            self.label_service_name.grid(sticky="w", row=3, column=0, padx=(20, 0), pady=2)
            self.text_service_name.grid(columnspan=3, row=3, column=1, padx=(0, 15), pady=2)
            self.label_service_name_value.set("Database")
        else:
            self.label_service_name.grid_forget()
            self.text_service_name.grid_forget()

    def clear_input(self):
        self.text_src_name.delete(0, 'end')
        self.engine.set("Select Engine")
        self.text_host.delete(0, 'end')
        self.text_port.delete(0, 'end')
        self.text_username.delete(0, 'end')
        self.text_pass.delete(0, 'end')
        self.text_schema.delete(0, 'end')
        self.text_username.delete(0, 'end')

    def get_odbc_drivers(self, engine):
        list_driver = pyodbc.drivers()
        if engine == 'ORACLE':
            for driver in list_driver:
                if 'Oracle' in driver:
                    return driver
        elif engine == 'MSSQL':
            for driver in list_driver:
                if 'SQL Server' in driver:
                    return driver


    def add_new_connection(self):
        # Get data from input console
        input_src_name = self.text_src_name.get().strip()
        input_engine = self.engine.get()
        input_host = self.text_host.get()
        input_port = self.text_port.get()
        input_username = self.text_username.get()
        input_pass = self.text_pass.get()
        input_schema = self.text_schema.get()

        # Validate input from user
        if input_src_name == "" or input_engine == "Select Engine" or input_host == "" or input_port == "" or input_username == "" or input_pass == "":
            self.label_noti['text'] = 'Vui lòng nhập đầy đủ các thông tin !'
        else:
            #Check SrcName đã tồn tại hay chưa?
            list_cnn_config = config_reader.get_list_src_name_original()
            if input_src_name in list_cnn_config:
                self.label_noti['text'] = 'Tên kết nối đã tồn tại !'
            else:
                if(input_engine in ['ORACLE', 'DB2', 'REDSHIFT', 'MYSQL']):
                    input_service_name = self.text_service_name.get()
                else:
                    input_service_name = ''
                pass_encrypt = encryptor.encrypt(input_pass.encode(),encryptor.key.encode()).decode("utf-8")

                # Make new data:
                new_data = {
                    "schema": input_schema,
                    "server": input_host,
                    "port": input_port,
                    "user": input_username,
                    "password": pass_encrypt,
                    "engine": input_engine,
                    "service_name": input_service_name if input_engine == 'ORACLE' else '',
                    "DB": input_service_name
                }
                # Check validate connection:
                cnn_check, message = Connector_factory.validate_connection(new_data, input_engine, input_pass)
                if cnn_check:
                    file = open(IOUtils.get_absolute_path(DefaultVar.DEV_ENV), 'r+')
                    config = json.load(file)
                    config[input_src_name] = new_data
                    file.seek(0)
                    json.dump(config, file, indent=4)
                    self.label_noti['text'] = 'Thêm mới thành công !!!'
                    self.clear_input()
                else:
                    self.label_noti['text'] = message

# if __name__ == '__main__':
#     DB = 'bludb1'
#     server = '10.36.129.30'
#     port = '50000'
#     user = 'bid_anmv1'
#     password = 'langtuX25@49'
#
#     try:
#         cnn = db2.connect(
#             "DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};".format(
#                 DATABASE=DB, HOSTNAME=server, PORT=port, UID=user, PWD=password)
#             , "", "")
#     except Exception as e:
#         print(e.args[0])