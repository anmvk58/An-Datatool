import art
import tkinter as tk
from src.GUI.Page_Page import Page

class ConnectionManager(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        label = tk.Label(self, text="HƯỚNG DẪN SỬ DỤNG", font=("Arial", 20, "bold"), fg='#C70039')
        label.pack(pady=(20, 10), side=tk.TOP, fill=tk.X, expand=False)