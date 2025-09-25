import threading
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, simpledialog, messagebox
import asyncio
import os
import re
from datetime import datetime

from telethon.errors.rpcerrorlist import ApiIdInvalidError
from .i18n import tr, detect_lang_from_env
from .config import load_config, save_config
from .member_export import export_member_data, ExportSummary
from .chat_utils import NormalizedChat, normalize_chat_identifier, resolve_chat_entity
from telethon.tl.functions.help import GetConfigRequest


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.cfg = load_config()
        self.lang = self.cfg.get("lang") or detect_lang_from_env()
        self.t = lambda key, **kw: tr(self.lang, key, **kw)
        self.root.title(self.t("app.title"))

        # Defaults aus Umgebung/Config
        default_output = self.cfg.get("output_path", str(Path.cwd() / "members.csv"))
        default_api_id = os.getenv("TELEGRAM_API_ID", "")
        default_api_hash = os.getenv("TELEGRAM_API_HASH", "")
        default_phone = os.getenv("TELEGRAM_PHONE", "")
        default_chat = os.getenv("TELEGRAM_CHAT_USERNAME", "https://t.me/+q3dPCH4ulF85ZTg0")
        default_mode = self.cfg.get("export_mode", "member")
        default_history_limit = str(self.cfg.get("history_limit", "2000"))

        # UI: Felder
        frm = ttk.Frame(root, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)

        row = 0
        # Sprache
        self.lbl_lang = ttk.Label(frm, text=self.t("lang.label"))
        self.lbl_lang.grid(row=row, column=0, sticky="w")
        self.lang_var = tk.StringVar(value=self.lang)
        self.cmb_lang = ttk.Combobox(
            frm,
            textvariable=self.lang_var,
            values=["de", "en"],
            state="readonly",
            width=6,
        )
        self.cmb_lang.grid(row=row, column=1, sticky="w")
        self.cmb_lang.bind("<<ComboboxSelected>>", self.on_lang_change)
        row += 1

        self.lbl_phone = ttk.Label(frm, text=self.t("field.phone"))
        self.lbl_phone.grid(row=row, column=0, sticky="w")
        self.phone_var = tk.StringVar(value=default_phone)
        ttk.Entry(frm, textvariable=self.phone_var, width=40).grid(row=row, column=1, sticky="ew")
        row += 1

        self.lbl_api_id = ttk.Label(frm, text=self.t("field.api_id"))
        self.lbl_api_id.grid(row=row, column=0, sticky="w")
        self.api_id_var = tk.StringVar(value=default_api_id)
        ttk.Entry(frm, textvariable=self.api_id_var, width=40).grid(row=row, column=1, sticky="ew")
        row += 1

        self.lbl_api_hash = ttk.Label(frm, text=self.t("field.api_hash"))
        self.lbl_api_hash.grid(row=row, column=0, sticky="w")
        self.api_hash_var = tk.StringVar(value=default_api_hash)
        hash_row = ttk.Frame(frm)
        hash_row.grid(row=row, column=1, sticky="ew")
        hash_row.columnconfigure(0, weight=1)
        self.api_hash_entry = ttk.Entry(hash_row, textvariable=self.api_hash_var, width=40, show="*")
        self.api_hash_entry.grid(row=0, column=0, sticky="ew")
        self.show_hash_var = tk.BooleanVar(value=False)
        self.chk_show_hash = ttk.Checkbutton(hash_row, text=self.t("chk.show_api_hash"), variable=self.show_hash_var, command=self.on_toggle_hash)
        self.chk_show_hash.grid(row=0, column=1, padx=(6, 0))
        row += 1

        self.lbl_chat = ttk.Label(frm, text=self.t("field.chat"))
        self.lbl_chat.grid(row=row, column=0, sticky="w")
        self.recent_chats = self.cfg.get("recent_chats", [])
        initial_chat = default_chat or (self.recent_chats[0] if self.recent_chats else "")
        self.chat_var = tk.StringVar(value=initial_chat)
        chat_row = ttk.Frame(frm)
        chat_row.grid(row=row, column=1, sticky="ew")
        chat_row.columnconfigure(0, weight=1)
        self.cmb_chat = ttk.Combobox(chat_row, textvariable=self.chat_var, values=self.recent_chats, width=40)
        self.cmb_chat.grid(row=0, column=0, sticky="ew")
        self.cmb_chat.bind("<<ComboboxSelected>>", self.on_chat_selected)
        row += 1

        # Chat-Name Anzeige
        self.lbl_chat_name = ttk.Label(frm, text=self.t("field.chat_name"))
        self.lbl_chat_name.grid(row=row, column=0, sticky="w")
        self.chat_name_var = tk.StringVar(value="")
        self.chat_name_value = ttk.Label(frm, textvariable=self.chat_name_var)
        self.chat_name_value.grid(row=row, column=1, sticky="w")
        row += 1

        self.lbl_mode = ttk.Label(frm, text=self.t("field.mode"))
        self.lbl_mode.grid(row=row, column=0, sticky="w")
        self.mode_var = tk.StringVar(value=default_mode if default_mode in {"member", "admin"} else "member")
        mode_row = ttk.Frame(frm)
        mode_row.grid(row=row, column=1, sticky="w")
        mode_row.columnconfigure(0, weight=0)
        self.mode_buttons = {}
        for idx, value in enumerate(("member", "admin")):
            btn = ttk.Radiobutton(
                mode_row,
                text=self.t(f"mode.{value}"),
                value=value,
                variable=self.mode_var,
                command=self.on_mode_change,
            )
            btn.grid(row=0, column=idx, padx=(0, 12 if idx == 0 else 0), sticky="w")
            self.mode_buttons[value] = btn
        row += 1

        self.lbl_output = ttk.Label(frm, text=self.t("field.output_csv"))
        self.lbl_output.grid(row=row, column=0, sticky="w")
        self._suppress_output_trace = False
        self.output_user_modified = False
        self._suppress_output_trace = True
        self.output_var = tk.StringVar(value=default_output)
        self._suppress_output_trace = False
        self.output_var.trace_add("write", self.on_output_change)
        out_row = ttk.Frame(frm)
        out_row.grid(row=row, column=1, sticky="ew")
        out_row.columnconfigure(0, weight=1)
        ttk.Entry(out_row, textvariable=self.output_var).grid(row=0, column=0, sticky="ew")
        self.btn_browse = ttk.Button(out_row, text=self.t("btn.browse"), command=self.browse_output)
        self.btn_browse.grid(row=0, column=1, padx=(6, 0))
        row += 1

        # Optionale Session-Datei
        self.lbl_session = ttk.Label(frm, text=self.t("field.session"))
        self.lbl_session.grid(row=row, column=0, sticky="w")
        default_session = self.cfg.get("session_path", "")
        self.session_var = tk.StringVar(value=default_session)
        sess_row = ttk.Frame(frm)
        sess_row.grid(row=row, column=1, sticky="ew")
        sess_row.columnconfigure(0, weight=1)
        ttk.Entry(sess_row, textvariable=self.session_var).grid(row=0, column=0, sticky="ew")
        self.btn_browse_sess = ttk.Button(sess_row, text=self.t("btn.browse"), command=self.browse_session)
        self.btn_browse_sess.grid(row=0, column=1, padx=(6, 0))
        row += 1

        # Nachrichten-Limit für Aktivitäts-Scan
        self.limit_var = tk.StringVar(value=default_history_limit)
        limit_row = ttk.Frame(frm)
        limit_row.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        limit_row.columnconfigure(1, weight=1)
        self.limit_label = ttk.Label(limit_row, text=self.t("field.history_limit"))
        self.limit_label.grid(row=0, column=0, sticky="w")
        self.ent_limit = ttk.Entry(limit_row, textvariable=self.limit_var, width=8)
        self.ent_limit.grid(row=0, column=1, sticky="w", padx=(12, 0))
        row += 1

        # Hinweis zu Admin-Rechten
        self.admin_note = ttk.Label(frm, text=self.t("info.admin_note"), wraplength=420, justify="left")
        self.admin_note.grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        self.start_btn = ttk.Button(frm, text=self.t("btn.start"), command=self.on_start)
        self.start_btn.grid(row=row, column=0, pady=(8, 0), sticky="w")
        self.progress_lbl = ttk.Label(frm, text=self.t("status.ready"))
        self.progress_lbl.grid(row=row, column=1, pady=(8, 0), sticky="w")
        row += 1

        # Log-Ausgabe
        self.log_label = ttk.Label(frm, text=self.t("label.log"))
        self.log_label.grid(row=row, column=0, sticky="nw", pady=(8, 0))
        self.log = tk.Text(frm, height=12, width=60, state="disabled")
        self.log.grid(row=row, column=1, sticky="nsew", pady=(8, 0))
        frm.rowconfigure(row, weight=1)
        row += 1

        for c in range(2):
            frm.columnconfigure(c, weight=1)

        self.worker = None
        self.chat_resolve_job = None
        self.last_resolved_chat = None
        self.chat_var.trace_add("write", self.on_chat_var_change)
        self.api_id_var.trace_add("write", self.on_credentials_change)
        self.api_hash_var.trace_add("write", self.on_credentials_change)
        self.update_mode_dependent_state(adjust_path=True)
        self.schedule_chat_resolution(initial=True)

    def on_lang_change(self, event=None):
        self.lang = self.lang_var.get()
        self.cfg["lang"] = self.lang
        save_config(self.cfg)
        self.t = lambda key, **kw: tr(self.lang, key, **kw)
        self.refresh_texts()

    def on_chat_selected(self, event=None):
        self.schedule_chat_resolution()

    def on_chat_var_change(self, *args):
        self.schedule_chat_resolution()

    def on_credentials_change(self, *args):
        self.last_resolved_chat = None
        self.schedule_chat_resolution()

    def schedule_chat_resolution(self, delay: int = 700, *, initial: bool = False) -> None:
        if initial:
            delay = 200
        if self.chat_resolve_job is not None:
            try:
                self.root.after_cancel(self.chat_resolve_job)
            except Exception:
                pass
        self.chat_resolve_job = self.root.after(delay, self.on_resolve_chat)

    def on_output_change(self, *args) -> None:
        if not self._suppress_output_trace:
            self.output_user_modified = True

    def set_output_path(self, value: str, *, user: bool = False) -> None:
        self._suppress_output_trace = True
        try:
            self.output_var.set(value)
        finally:
            self._suppress_output_trace = False
        if user:
            self.output_user_modified = True

    def get_output_path_str(self) -> str:
        return (self.output_var.get() or "").strip()

    def apply_chat_resolution(self, chat_identifier: NormalizedChat, chat_name: str, force: bool | None = None) -> Path:
        if force is None:
            force = not self.output_user_modified
        self.chat_name_var.set(chat_name)
        self.last_resolved_chat = chat_identifier
        return self.suggest_output_from_chat(chat_name, force=force)

    def on_mode_change(self) -> None:
        mode = self.mode_var.get()
        if mode not in {"member", "admin"}:
            mode = "member"
            self.mode_var.set(mode)
        self.cfg["export_mode"] = mode
        save_config(self.cfg)
        self.update_mode_dependent_state(adjust_path=True)

    def current_output_suffix(self) -> str:
        return ".csv" if self.mode_var.get() in {"member", "admin"} else ".ods"

    def update_mode_dependent_state(self, *, adjust_path: bool = False) -> None:
        label_key = "field.output_csv" if self.current_output_suffix() == ".csv" else "field.output_ods"
        self.lbl_output.configure(text=self.t(label_key))
        for value, btn in getattr(self, "mode_buttons", {}).items():
            btn.configure(text=self.t(f"mode.{value}"))
        if adjust_path:
            current = self.output_var.get().strip()
            if current:
                try:
                    path = Path(current)
                    target = self.current_output_suffix()
                    if path.suffix.lower() != target:
                        self.set_output_path(str(path.with_suffix(target)))
                except Exception:
                    pass

    def localize_progress(self, msg: str) -> str:
        if msg == "Collecting participants...":
            return self.t("progress.collecting")
        if msg.startswith("Bots counted:"):
            count = msg.split(":", 1)[1].strip()
            return self.t("progress.bots", count=count)
        if msg.startswith("Unable to enumerate bots"):
            return self.t("progress.bots_missing")
        if msg.startswith("Participants collected:"):
            count = msg.split(":", 1)[1].strip()
            return self.t("progress.participants", count=count)
        if msg.startswith("Resolved chat title:"):
            name = msg.split(":", 1)[1].strip()
            return self.t("progress.chat_title", name=name)
        if msg.startswith("Recent participants counted:"):
            count = msg.split(":", 1)[1].strip()
            return self.t("progress.recent", count=count)
        if msg.startswith("Unable to enumerate recent members"):
            return self.t("progress.recent_missing")
        if msg == "No participants via API; scanning messages as fallback...":
            return self.t("progress.fallback")
        if msg == "Scanning message activity...":
            return self.t("progress.scanning")
        if msg.startswith("Messages scanned:"):
            count = msg.split(":", 1)[1].strip()
            return self.t("progress.messages_scanned", count=count)
        if msg.startswith("Activity scan finished after"):
            match = re.search(r"after\s+(\d+)\s+messages", msg)
            number = match.group(1) if match else "0"
            return self.t("progress.activity_done", count=number)
        if msg.startswith("Saved CSV:"):
            path = msg.split(":", 1)[1].strip()
            return self.t("progress.saved_csv", path=path)
        return msg

    def refresh_texts(self) -> None:
        self.root.title(self.t("app.title"))
        self.lbl_lang.configure(text=self.t("lang.label"))
        self.lbl_phone.configure(text=self.t("field.phone"))
        self.lbl_api_id.configure(text=self.t("field.api_id"))
        self.lbl_api_hash.configure(text=self.t("field.api_hash"))
        self.lbl_chat.configure(text=self.t("field.chat"))
        self.lbl_mode.configure(text=self.t("field.mode"))
        self.btn_browse.configure(text=self.t("btn.browse"))
        if hasattr(self, "lbl_session"):
            self.lbl_session.configure(text=self.t("field.session"))
            self.btn_browse_sess.configure(text=self.t("btn.browse"))
        self.limit_label.configure(text=self.t("field.history_limit"))
        if hasattr(self, "admin_note"):
            self.admin_note.configure(text=self.t("info.admin_note"))
        if hasattr(self, "lbl_chat_name"):
            self.lbl_chat_name.configure(text=self.t("field.chat_name"))
        self.start_btn.configure(text=self.t("btn.start"))
        self.progress_lbl.configure(text=self.t("status.ready"))
        self.log_label.configure(text=self.t("label.log"))
        if hasattr(self, "chk_show_hash"):
            self.chk_show_hash.configure(text=self.t("chk.show_api_hash"))
        self.update_mode_dependent_state()

    def on_toggle_hash(self) -> None:
        self.api_hash_entry.configure(show=("" if self.show_hash_var.get() else "*"))

    def suggest_output_from_chat(self, chat_name: str, *, force: bool = False) -> Path:
        # Bestimme Verzeichnis aus aktuellem Output oder CWD
        try:
            curr = Path(self.output_var.get().strip()) if self.output_var.get().strip() else (Path.cwd() / "members.csv")
        except Exception:
            curr = Path.cwd() / "members.csv"
        out_dir = curr.expanduser().resolve().parent
        # Slugify
        slug = re.sub(r"[^\w\-\.]+", "_", chat_name).strip("._") or "chat"
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")
        suffix = self.current_output_suffix() if hasattr(self, "mode_var") else ".csv"
        candidate = out_dir / f"{slug}_{ts}{suffix}"
        # Nur überschreiben, wenn bisher Standard war oder leer
        prev = self.get_output_path_str()
        if force or not prev or prev.endswith("usernames.ods") or prev.endswith("members.csv"):
            self.set_output_path(str(candidate))
        return candidate

    def on_resolve_chat(self) -> None:
        self.chat_resolve_job = None
        chat = self.chat_var.get().strip()
        api_id_raw = self.api_id_var.get().strip()
        api_hash = self.api_hash_var.get().strip()
        if not chat or not api_id_raw or not api_hash:
            return
        normalized_chat = normalize_chat_identifier(chat)
        if normalized_chat is None:
            return
        if normalized_chat == self.last_resolved_chat:
            return
        try:
            api_id = int(api_id_raw)
        except Exception:
            return
        self.set_status(self.t("status.resolving"))
        self.append_log(self.t("status.resolving"))
        def worker():
            try:
                async def run():
                    from telethon import TelegramClient
                    client = TelegramClient("resolve_preview", api_id, api_hash)
                    try:
                        await client.connect()
                        ent = await resolve_chat_entity(client, normalized_chat)
                        name = getattr(ent, "title", None) or getattr(ent, "username", None) or normalized_chat.raw
                        self.call_in_ui(lambda: self.apply_chat_resolution(normalized_chat, name))
                        self.call_in_ui(self.set_status, self.t("status.resolved"))
                    finally:
                        await client.disconnect()
                        try:
                            Path("resolve_preview.session").unlink(missing_ok=True)
                        except Exception:
                            pass
                asyncio.run(run())
            except ValueError:
                # Invite could not be resolved yet; show raw identifier but allow future retry
                self.call_in_ui(self.chat_name_var.set, normalized_chat.raw)
                self.call_in_ui(self.suggest_output_from_chat, normalized_chat.raw)
                self.last_resolved_chat = None
                self.call_in_ui(self.append_log, self.t("info.invite_deferred"))
                self.call_in_ui(self.set_status, self.t("status.ready"))
            except Exception as e:
                self.last_resolved_chat = None
                self.call_in_ui(self.chat_name_var.set, self.t("status.error_short"))
                self.call_in_ui(self.append_log, f"{self.t('status.error_title')}: {e}")
                self.call_in_ui(self.set_status, self.t("status.error_short"))
        threading.Thread(target=worker, daemon=True).start()

    def browse_output(self) -> None:
        initial = self.output_var.get() or str(Path.cwd() / "members.csv")
        suffix = self.current_output_suffix()
        if suffix == ".csv":
            filetypes = [
                (self.t("filetype.csv"), "*.csv"),
                (self.t("filetype.all"), "*.*"),
            ]
        else:
            filetypes = [
                (self.t("filetype.ods"), "*.ods"),
                (self.t("filetype.all"), "*.*"),
            ]
        path = filedialog.asksaveasfilename(
            title=self.t("dialog.save_title"),
            defaultextension=suffix,
            filetypes=filetypes,
            initialfile=Path(initial).name,
            initialdir=str(Path(initial).expanduser().resolve().parent),
        )
        if path:
            self.set_output_path(path, user=True)

    def browse_session(self) -> None:
        session_dir_cfg = self.cfg.get("session_dir")
        if session_dir_cfg:
            try:
                session_dir = Path(session_dir_cfg).expanduser().resolve()
            except Exception:
                session_dir = Path.cwd()
        else:
            session_dir = Path.cwd()
        initial = self.session_var.get() or str(session_dir)
        path = filedialog.askopenfilename(
            title=self.t("dialog.session_title"),
            filetypes=[(self.t("filetype.session"), "*.session"), (self.t("filetype.all"), "*.*")],
            initialdir=str(Path(initial).expanduser().resolve()),
        )
        if path:
            self.session_var.set(path)
            self.cfg["session_path"] = path
            self.cfg["session_dir"] = str(Path(path).expanduser().resolve().parent)
            save_config(self.cfg)

    def set_running(self, running: bool) -> None:
        for widget in (self.start_btn,):
            widget.configure(state=("disabled" if running else "normal"))

    def append_log(self, msg: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def set_status(self, msg: str) -> None:
        self.progress_lbl.configure(text=msg)

    # Thread-sichere UI-Aufrufe aus dem Worker
    def call_in_ui(self, fn, *args, **kwargs):
        ev = threading.Event()
        res = {}

        def wrapper():
            try:
                res["value"] = fn(*args, **kwargs)
            finally:
                ev.set()

        self.root.after(0, wrapper)
        ev.wait()
        return res.get("value")

    def request_code(self) -> str:
        return self.call_in_ui(
            simpledialog.askstring,
            self.t("login.code_title"),
            self.t("login.code_body"),
            parent=self.root,
        ) or ""

    def request_password(self) -> str:
        return self.call_in_ui(
            simpledialog.askstring,
            self.t("login.password_title"),
            self.t("login.password_body"),
            parent=self.root,
            show="*",
        ) or ""

    def on_start(self) -> None:
        # Validierung
        phone = self.phone_var.get().strip()
        api_id_raw = self.api_id_var.get().strip()
        api_hash = self.api_hash_var.get().strip()
        chat = self.chat_var.get().strip()
        output = self.output_var.get().strip()

        if not phone or not api_id_raw or not api_hash or not chat or not output:
            messagebox.showerror(self.t("error.missing_title"), self.t("error.missing_body"))
            return
        try:
            api_id = int(api_id_raw)
        except ValueError:
            messagebox.showerror(self.t("error.api_id_title"), self.t("error.api_id_body"))
            return

        # api_hash plausibilisieren (32-stelliger Hex-String)
        if not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
            messagebox.showerror(self.t("error.api_hash_title"), self.t("error.api_hash_body"))
            return

        # Ausgabepfad speichern
        out_path = Path(output).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self.cfg["output_path"] = str(out_path)

        try:
            history_limit = int(self.limit_var.get().strip() or "2000")
        except Exception:
            history_limit = 2000
        history_limit = max(100, min(history_limit, 20000))
        self.cfg["history_limit"] = history_limit
        self.limit_var.set(str(history_limit))

        normalized_chat = normalize_chat_identifier(chat)
        if normalized_chat is None:
            messagebox.showerror(self.t("error.chat_invalid_title"), self.t("error.chat_invalid_body"))
            return
        self.chat_var.set(normalized_chat.raw)

        sess_raw = self.session_var.get().strip()
        if sess_raw:
            sess_path = Path(sess_raw).expanduser()
            self.cfg["session_path"] = str(sess_path)
            try:
                self.cfg["session_dir"] = str(sess_path.resolve().parent)
            except Exception:
                self.cfg["session_dir"] = str(Path.cwd())
        else:
            self.cfg.pop("session_path", None)

        # Chat-Historie aktualisieren (max 10)
        recent_chat_entry = normalized_chat.raw
        recent = [recent_chat_entry] + [c for c in self.cfg.get("recent_chats", []) if c != recent_chat_entry]
        self.cfg["recent_chats"] = recent[:10]
        save_config(self.cfg)
        # Combobox-Werte aktualisieren
        if hasattr(self, "cmb_chat"):
            self.cmb_chat.configure(values=self.cfg["recent_chats"]) 

        # Start
        self.set_running(True)
        self.append_log(self.t("status.starting"))
        self.set_status(self.t("status.logging_in"))

        def worker():
            output_path_local = out_path
            try:
                # Preflight: API Credentials validieren ohne Codeversand
                self.call_in_ui(self.set_status, self.t("status.validating"))
                # Maskierte Logs zur Verifikation
                hash_mask = (api_hash[:4] + "…" + api_hash[-4:]) if len(api_hash) >= 8 else "(short)"
                phone_mask = phone[:4] + "…" + phone[-2:] if len(phone) >= 7 else phone
                self.call_in_ui(self.append_log, self.t("log.using_creds", api_id=api_id, api_hash=hash_mask, phone=phone_mask))
                async def preflight():
                    from telethon import TelegramClient
                    client = TelegramClient("preflight", api_id, api_hash)
                    try:
                        await client.connect()
                        await client(GetConfigRequest())
                    finally:
                        await client.disconnect()
                        try:
                            Path("preflight.session").unlink(missing_ok=True)
                        except Exception:
                            pass
                asyncio.run(preflight())
                self.call_in_ui(self.append_log, self.t("status.valid_ok"))

                # Export
                sess = sess_raw or None
                mode = self.mode_var.get()
                if mode not in {"member", "admin"}:
                    mode = "member"
                # Falls noch kein Chat-Name bekannt ist, für Dateiname fallback aus Eingabe ableiten
                if not self.chat_name_var.get().strip():
                    self.call_in_ui(self.suggest_output_from_chat, normalized_chat.raw)
                self.call_in_ui(self.set_status, self.t("status.exporting_members"))

                def progress(msg: str) -> None:
                    localized = self.localize_progress(msg)
                    self.call_in_ui(self.append_log, localized)

                summary: ExportSummary = asyncio.run(
                    export_member_data(
                        api_id=api_id,
                        api_hash=api_hash,
                        phone=phone,
                        chat_username=normalized_chat,
                        output_path=output_path_local,
                        mode=mode,
                        history_limit=history_limit,
                        session_path=sess,
                        progress_callback=progress,
                        code_callback=self.request_code,
                        password_callback=self.request_password,
                    )
                )
                _, new_output_str = self.call_in_ui(
                    lambda: (
                        self.apply_chat_resolution(normalized_chat, summary.chat_title),
                        self.get_output_path_str(),
                    )
                )
                try:
                    new_output = Path(new_output_str).expanduser().resolve()
                except Exception:
                    new_output = output_path_local
                if new_output != output_path_local:
                    try:
                        if new_output.exists():
                            raise FileExistsError(new_output)
                        new_output.parent.mkdir(parents=True, exist_ok=True)
                        output_path_local.replace(new_output)
                        output_path_local = new_output
                        self.cfg["output_path"] = str(new_output)
                        save_config(self.cfg)
                        self.call_in_ui(self.append_log, self.t("log.renamed_output", path=new_output))
                    except Exception as rename_err:
                        self.call_in_ui(
                            self.append_log,
                            self.t("log.rename_failed", error=str(rename_err)),
                        )
                self.call_in_ui(
                    self.append_log,
                    self.t(
                        "log.member_summary",
                        total=summary.total_members,
                        bots=summary.bot_count,
                        recent=summary.recent_count,
                    ),
                )
                self.call_in_ui(self.append_log, f"{self.t('status.done')} {output_path_local}")
                self.call_in_ui(self.set_status, self.t("status.done"))
            except ApiIdInvalidError:
                self.call_in_ui(self.append_log, self.t("error.api_creds_body"))
                self.call_in_ui(messagebox.showerror, self.t("error.api_creds_title"), self.t("error.api_creds_body"))
                self.call_in_ui(self.set_status, self.t("status.error_short"))
            except Exception as e:
                self.call_in_ui(self.append_log, f"{self.t('status.error_title')}: {e}")
                self.call_in_ui(messagebox.showerror, self.t("status.error_title"), str(e))
                self.call_in_ui(self.set_status, self.t("status.error_short"))
            finally:
                self.call_in_ui(self.set_running, False)

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()
