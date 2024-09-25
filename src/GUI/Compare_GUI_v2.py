import sys
import os

path = os.path.normpath(os.getcwd() + os.sep + os.pardir)
parent_path = os.path.normpath(path + os.sep + os.pardir)
sys.path.extend([path, parent_path])

import tkinter as tk
from GUI.Page_Main_View import MainView

if __name__ == '__main__':
    root = tk.Tk()
    root.title('A-Data Tools')
    root.geometry("1000x700")
    root.minsize(1000, 700)
    main = MainView(root)
    main.pack(side="top", fill="both", expand=True)
    root.mainloop()