from tkinter import *


def focus_out_entry_box(widget, widget_text):
    if widget['fg'] == 'Black' and len(widget.get()) == 0:
        widget.delete(0, END)
        widget['fg'] = 'Grey'
        widget.insert(0, widget_text)


def focus_in_entry_box(widget):
    if widget['fg'] == 'Grey':
        widget['fg'] = 'Black'
        widget.delete(0, END)


entry_text = 'First name'
my_entry = Entry(font='Arial 18', fg='Grey')
my_entry.insert(0, entry_text)
my_entry.bind("<FocusIn>", lambda args: focus_in_entry_box(my_entry))
my_entry.bind("<FocusOut>", lambda args: focus_out_entry_box(my_entry, entry_text))
my_entry.pack()
mainloop()