import tkinter as tk
import os
root = tk.Tk ()

canvas1 = tk.Canvas (root, width=350, height=150)
canvas1.pack ()

def CallApplication():
    os.system ('python Processequences.py')


button1 = tk.Button (root, text='Start Application',font=('Helvetica', '15'), command=CallApplication)
canvas1.create_window (150, 130, window=button1)


root.mainloop ()