import asyncio
import os
import time
import shutil
from pathlib import Path
from telethon.sync import TelegramClient
from telethon.tl.types import UserStatusOnline, UserStatusOffline, UserStatusRecently
from telethon.errors import ChatAdminRequiredError, RPCError
from odf import opendocument, table, text


async def export_task(api_id: int, api_hash: str, phone: str, chat_username: str, output_path: Path, t, code_callback, password_callback, session_path: str | None = None, mode: str = "export", limit: int = 10000) -> dict:
    # Telegram-Client initialisieren und anmelden (Session ggf. klonen)
    tmp_session = None
    if session_path:
        src = Path(session_path).expanduser()
        if src.exists():
            cache_dir = Path.home() / ".cache" / "telegram_user_state" / "sessions"
            cache_dir.mkdir(parents=True, exist_ok=True)
            tmp_session = cache_dir / f"{src.stem}_{os.getpid()}_{int(time.time())}.session"
            try:
                shutil.copy2(str(src), str(tmp_session))
            except Exception:
                tmp_session = None
    session_id = str(tmp_session) if tmp_session else (session_path or "session")
    client = TelegramClient(session_id, api_id, api_hash)
    await client.start(phone=phone, code_callback=code_callback, password=password_callback)

    # Chat-Entity holen
    entity = await client.get_entity(chat_username)

    bots = 0
    total = 0

    # ODS vorbereiten je nach Modus
    doc = opendocument.OpenDocumentSpreadsheet()
    tbl = table.Table(name="UserData")
    doc.spreadsheet.addElement(tbl)

    if mode == "count_bots":
        # Header für Summary
        header_row = table.TableRow()
        for header in [t("header.metric"), t("header.value")]:
            cell = table.TableCell()
            cell.addElement(text.P(text=header))
            header_row.addElement(cell)
        tbl.addElement(header_row)
    else:
        header_row = table.TableRow()
        for header in [t("header.username"), t("header.is_bot"), t("header.last_seen")]:
            cell = table.TableCell()
            cell.addElement(text.P(text=header))
            header_row.addElement(cell)
        tbl.addElement(header_row)

    # Mitglieder iterieren (mit Limit)
    try:
        async for user in client.iter_participants(entity, limit=limit, aggressive=True):
            total += 1
            if getattr(user, "bot", False):
                bots += 1
            if mode == "export":
                if user.username:
                    is_bot = t("bot.yes") if user.bot else t("bot.no")
                    last_seen = t("seen.unknown")
                    if user.status:
                        if isinstance(user.status, UserStatusOnline):
                            last_seen = t("seen.online")
                        elif isinstance(user.status, UserStatusOffline):
                            last_seen = (
                                user.status.was_online.strftime("%Y-%m-%d %H:%M:%S") if user.status.was_online else t("seen.unknown")
                            )
                        elif isinstance(user.status, UserStatusRecently):
                            last_seen = t("seen.recently")
                        else:
                            last_seen = t("seen.hidden")

                    row = table.TableRow()
                    for value in [user.username, is_bot, last_seen]:
                        cell = table.TableCell()
                        cell.addElement(text.P(text=str(value)))
                        row.addElement(cell)
                    tbl.addElement(row)
    except ChatAdminRequiredError:
        # Kein Admin: Fallback nur für Bot-Zählen über Nachrichten-Historie
        if mode == "count_bots":
            seen = set()
            # Über Nachrichten eine Annäherung der sichtbaren Sender gewinnen
            async for msg in client.iter_messages(entity, limit=min(limit, 5000)):
                sender = await msg.get_sender()
                if sender and getattr(sender, 'id', None) is not None:
                    if sender.id not in seen:
                        seen.add(sender.id)
                        if getattr(sender, 'bot', False):
                            bots += 1
            total = len(seen)
            # Markiere Fallback via Rückgabewert
            fallback_used = True
        else:
            raise
    except RPCError:
        # Andere RPC-Fehler: ohne Admin oft nicht erlaubt
        if mode == "count_bots":
            seen = set()
            async for msg in client.iter_messages(entity, limit=min(limit, 5000)):
                sender = await msg.get_sender()
                if sender and getattr(sender, 'id', None) is not None:
                    if sender.id not in seen:
                        seen.add(sender.id)
                        if getattr(sender, 'bot', False):
                            bots += 1
            total = len(seen)
            fallback_used = True
        else:
            raise
    else:
        fallback_used = False

    # Falls count_bots: Summary-Zeilen schreiben
    if mode == "count_bots":
        for key, value in [(t("summary.total"), total), (t("summary.bots"), bots)]:
            row = table.TableRow()
            for v in [key, str(value)]:
                cell = table.TableCell()
                cell.addElement(text.P(text=str(v)))
                row.addElement(cell)
            tbl.addElement(row)

    # Speichern
    doc.save(str(output_path))
    await client.disconnect()
    # temporäre Session wieder entfernen
    if tmp_session:
        try:
            Path(str(tmp_session)).unlink(missing_ok=True)
        except Exception:
            pass
    return {"bots": bots, "total": total, "fallback": locals().get("fallback_used", False)}
