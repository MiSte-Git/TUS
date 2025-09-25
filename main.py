import tkinter as tk
from tkinter import ttk

from telegram_user_state.ui import App


def main_ui():
    root = tk.Tk()
    try:
        style = ttk.Style()
        if "clam" in style.theme_names():
            style.theme_use("clam")
    except Exception:
        pass
    app = App(root)
    root.mainloop()


if __name__ == "__main__":
    main_ui()
