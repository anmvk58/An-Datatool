import tkinter as tk
from tkinter import *
from src.common.default_var import DefaultVar

class AboutDialog:
    def __init__(self, parent):
        top = self.top = tk.Toplevel(parent)
        x = parent.winfo_x()
        y = parent.winfo_y()
        self.top.geometry("+%d+%d" % (x + 250, y + 200))
        self.top.geometry('500x215')
        self.top.maxsize(500,215)
        self.top.minsize(500,215)
        self.top.title('About Tool')
        self.top.attributes('-toolwindow', True)
        self.top.attributes('-topmost', True)
        self.top.focus()

        # Devide layout
        frame1 = tk.Frame(master=self.top, width=150, height=215)
        frame1.pack(fill=tk.Y, side=tk.LEFT)
        frame2 = tk.Frame(master=self.top, width=350, height=215)
        frame2.pack(fill=tk.Y, side=tk.LEFT)

        # Frame 1: image
        # self.logo = ImageTk.PhotoImage(Image.open("..\\..\\resources\\images\\logo.jpg").resize((90,90), Image.ANTIALIAS))
        self.logo = PhotoImage(file="..\\..\\resources\\images\\logo.png")
        # Create a Label Widget to display the text or Image
        img_logo = Label(frame1, image=self.logo, width=100, height=100, anchor='center')
        img_logo.pack(padx=(20,30), pady=(20,0))

        # Frame 2: info
        tool_name = tk.Label(frame2, text=DefaultVar.TOOL_NAME, font=("Arial", 16, "bold"), width = 350, anchor="w")
        tool_name.pack(pady=(20,3), fill=tk.Y, expand=False)
        release = tk.Label(frame2, text=DefaultVar.RELEASE_DATE, font=("Arial", 12), width=350, anchor="w")
        release.pack(pady=(0,25), fill=tk.Y, expand=False)
        powered = tk.Label(frame2, text=DefaultVar.POWERED_BY, font=("Arial", 10, "italic"), width=350, anchor="w")
        powered.pack(pady=(0,3), fill=tk.Y, expand=False)
        copyright = tk.Label(frame2, text=DefaultVar.COPYRIGHT, font=("Arial", 10, "italic"), width=350, anchor="w")
        copyright.pack(pady=(0, 25), fill=tk.Y, expand=False)
        thanks = tk.Label(frame2, text=DefaultVar.THANK_TO, font=("Arial", 10, "italic"), width=350, anchor="w")
        thanks.pack(pady=(0, 25), fill=tk.Y, expand=False)

    def close(self):
        self.top.destroy()