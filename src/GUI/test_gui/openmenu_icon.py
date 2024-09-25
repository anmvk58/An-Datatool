import tkinter as tk
from tkinter import ttk

root = tk.Tk()

image1 = tk.PhotoImage(width=16, file='..\\..\\')
image2 = tk.PhotoImage(width=16); image2.put('green', to=(0,0,15,15))
image3 = tk.PhotoImage(width=16); image3.put('blue', to=(0,0,15,15))
images = {"red": image1, "green": image2, "blue": image3}

colorvar = tk.StringVar(value="red")
om = tk.OptionMenu(root, colorvar, "red", "green", "blue")
menu = om.nametowidget(om.menuname)
for label, image in images.items():
    print(f"configuring {label}")
    menu.entryconfigure(label, image=images[label], compound="left")

om.pack(padx=20, pady=20)

root.mainloop()