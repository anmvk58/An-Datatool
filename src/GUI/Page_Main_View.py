import tkinter as tk
from tkinter import *
#Add Page Class
from src.GUI.Page_Add_Connection import AddConnection
from src.GUI.Page_About_Dialog import AboutDialog
from src.GUI.Page_Compare_Run import Compare_Run
from src.GUI.Page_Compare_File import CompareFile
from src.GUI.Page_Guide import Guide
from src.GUI.Page_Connection_Manager import ConnectionManager

def show_add_connection(root):
    add_connection = AddConnection(root)
    root.wait_window(add_connection.top)

def show_about(root):
    inputDialog = AboutDialog(root)
    root.wait_window(inputDialog.top)

class MainView(tk.Frame):
    def __init__(self, root, *args, **kwargs):
        tk.Frame.__init__(self, root, *args, **kwargs)
        page_compare = Compare_Run(self)
        page_compare_file = CompareFile(self)
        page_guide = Guide(self)
        page_cnn_manager = ConnectionManager(self)

        # Add menu
        menu = Menu(root)
        root.config(menu=menu)
        filemenu = Menu(menu)
        menu.add_cascade(label='Menu', menu=filemenu)
        self.run_icon = PhotoImage(file='..\\..\\resources\\icons\\play.png')
        filemenu.add_command(label='SQL compare', image=self.run_icon, compound='left', command=lambda: page_compare.show())
        self.run_file_icon = PhotoImage(file='..\\..\\resources\\icons\\book.png')
        filemenu.add_command(label='File compare', image=self.run_file_icon, compound='left', command=lambda: page_compare_file.show())
        self.light_icon = PhotoImage(file='..\\..\\resources\\icons\\light.png')
        filemenu.add_command(label='Add Connection', image=self.light_icon, compound='left', command=lambda: show_add_connection(root))
        self.connection = PhotoImage(file='..\\..\\resources\\icons\\connection.png')
        filemenu.add_command(label='Connection Manager', image=self.connection, compound='left', command=lambda: page_cnn_manager.show())
        filemenu.add_separator()
        filemenu.add_command(label='Exit', command=self.quit)
        helpmenu = Menu(menu)
        menu.add_cascade(label='Help', menu=helpmenu)
        self.help_icon = PhotoImage(file='..\\..\\resources\\icons\\map.png')
        helpmenu.add_command(label='Guide', image=self.help_icon, compound='left', command=lambda: page_guide.show())
        helpmenu.add_separator()
        self.about_icon = PhotoImage(file='..\\..\\resources\\icons\\heart.png')
        helpmenu.add_command(label='About', image=self.about_icon, compound='left', command=lambda: show_about(root))
        # End add menu

        container = tk.Frame(self)
        container.pack(side="top", fill="both", expand=True)

        page_compare.place(in_=container, x=0, y=0, relwidth=1, relheight=1)
        page_compare_file.place(in_=container, x=0, y=0, relwidth=1, relheight=1)
        page_guide.place(in_=container, x=0, y=0, relwidth=1, relheight=1)
        page_cnn_manager.place(in_=container, x=0, y=0, relwidth=1, relheight=1)

        page_compare.show()