"""Advanced member export utilities for Telegram chats using Telethon."""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Union

from telethon import TelegramClient
from telethon.errors import ChatAdminRequiredError, RPCError
try:
    from telethon.tl.functions.messages import GetMessageReactionsList
except ImportError:  # Telethon < 1.26
    from telethon.tl.functions.messages import GetMessageReactionsListRequest as GetMessageReactionsList
from telethon.tl.types import (
    ChannelParticipant,
    ChannelParticipantAdmin,
    ChannelParticipantBanned,
    ChannelParticipantCreator,
    ChannelParticipantLeft,
    ChannelParticipantSelf,
    ChannelParticipantsBots,
    ChannelParticipantsRecent,
    Message,
    PeerUser,
)

try:  # Telethon >= 1.25
    from telethon.tl.types import ReactionTypeEmoji  # type: ignore
except ImportError:  # Older Telethon without ReactionTypeEmoji
    ReactionTypeEmoji = None  # type: ignore

from .chat_utils import NormalizedChat, normalize_chat_identifier, resolve_chat_entity

ProgressCallback = Callable[[str], None]


@dataclass(slots=True)
class ExportSummary:
    total_members: int
    bot_count: int
    recent_count: int
    csv_path: Path
    chat_title: str


def _format_dt(value: Optional[datetime]) -> str:
    if not value:
        return ""
    # Ensure timezone-aware datetimes are represented consistently
    if value.tzinfo:
        return value.isoformat()
    return value.replace(tzinfo=None).isoformat() + "Z"


def _format_reaction(reaction: Optional[tuple[Optional[str], Optional[datetime]]]) -> str:
    if not reaction:
        return ""
    emoji, date = reaction
    ts = _format_dt(date)
    if emoji and ts:
        return f"{emoji} @ {ts}"
    if emoji:
        return emoji
    return ts


def _status_from_participant(participant: Optional[ChannelParticipant]) -> str:
    if isinstance(participant, (ChannelParticipantAdmin, ChannelParticipantCreator)):
        return "admin"
    if isinstance(participant, ChannelParticipantBanned):
        return "restricted"
    if isinstance(participant, ChannelParticipantLeft):
        # Treat left users as restricted so they stand out in the export
        return "restricted"
    # Default to member (covers ChannelParticipant, ChannelParticipantSelf, etc.)
    return "member"


async def _collect_bots_and_recent(
    client: TelegramClient,
    entity,
    progress: Optional[ProgressCallback],
) -> tuple[int, Set[int]]:
    bot_count = 0
    recent_ids: Set[int] = set()

    try:
        async for user in client.iter_participants(entity, filter=ChannelParticipantsBots(), aggressive=True):
            bot_count += 1
    except (ChatAdminRequiredError, RPCError):
        if progress:
            progress("Unable to enumerate bots without sufficient rights; continuing with 0.")
        bot_count = 0
    else:
        if progress:
            progress(f"Bots counted: {bot_count}")

    try:
        async for user in client.iter_participants(entity, filter=ChannelParticipantsRecent(), aggressive=False):
            recent_ids.add(user.id)
    except (ChatAdminRequiredError, RPCError):
        if progress:
            progress("Unable to enumerate recent members without sufficient rights.")
    else:
        if progress:
            progress(f"Recent participants counted: {len(recent_ids)}")

    return bot_count, recent_ids


async def _collect_activity_maps(
    client: TelegramClient,
    entity,
    participant_ids: Set[int],
    history_limit: int,
    progress: Optional[ProgressCallback],
) -> tuple[Dict[int, datetime], Dict[int, tuple[Optional[str], Optional[datetime]]]]:
    last_post: Dict[int, datetime] = {}
    last_reaction: Dict[int, tuple[Optional[str], Optional[datetime]]] = {}

    scanned = 0
    async for message in client.iter_messages(entity, limit=history_limit):
        scanned += 1
        sender_id = getattr(message, "sender_id", None)
        if sender_id in participant_ids and sender_id not in last_post:
            last_post[sender_id] = message.date

        if message.reactions and getattr(message.reactions, "results", None):
            reactions = await _fetch_reactions_for_message(client, entity, message)
            for user_id, emoji, reaction_date in reactions:
                if user_id in participant_ids and user_id not in last_reaction:
                    last_reaction[user_id] = (emoji, reaction_date)

        if progress and scanned % 250 == 0:
            progress(f"Messages scanned: {scanned}")

        if len(last_post) == len(participant_ids) and len(last_reaction) == len(participant_ids):
            break

    if progress:
        progress(f"Activity scan finished after {scanned} messages")

    return last_post, last_reaction


async def _fetch_reactions_for_message(
    client: TelegramClient,
    entity,
    message: Message,
    page_size: int = 100,
) -> List[tuple[Optional[int], Optional[str], Optional[datetime]]]:
    reactions: List[tuple[Optional[int], Optional[str], Optional[datetime]]] = []
    try:
        response = await client(
            GetMessageReactionsList(
                peer=entity,
                id=message.id,
                limit=page_size,
                reaction=None,
                offset_peer=None,
                offset_id=0,
                offset_date=0,
            )
        )
    except RPCError:
        return reactions

    for reaction in response.reactions:
        peer = getattr(reaction, "peer_id", None)
        if isinstance(peer, PeerUser):
            user_id = peer.user_id
        else:
            user_id = None
        emoji = None
        reaction_obj = getattr(reaction, "reaction", None)
        if ReactionTypeEmoji is not None and isinstance(reaction_obj, ReactionTypeEmoji):
            emoji = reaction_obj.emoticon
        elif reaction_obj is not None and getattr(reaction_obj, "emoticon", None):
            emoji = getattr(reaction_obj, "emoticon")
        elif reaction_obj is not None:
            emoji = str(reaction_obj)
        reactions.append((user_id, emoji, getattr(reaction, "date", None)))

    return reactions


async def _clone_session(session_path: Optional[str]) -> tuple[Optional[Path], str]:
    if not session_path:
        return None, "member_export"

    src = Path(session_path).expanduser()
    if not src.exists():
        return None, session_path

    cache_dir = Path.home() / ".cache" / "telegram_user_state" / "sessions"
    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp_session = cache_dir / f"{src.stem}_{os.getpid()}_{int(time.time())}.session"
    try:
        shutil.copy2(str(src), str(tmp_session))
        return tmp_session, str(tmp_session)
    except OSError:
        return None, session_path


async def export_member_data(
    api_id: int,
    api_hash: str,
    phone: str,
    chat_username: Union[NormalizedChat, str, int],
    output_path: Path,
    *,
    mode: str = "member",
    history_limit: int = 2000,
    session_path: Optional[str] = None,
    progress_callback: Optional[ProgressCallback] = None,
    code_callback: Optional[Callable[[], str]] = None,
    password_callback: Optional[Callable[[], str]] = None,
) -> ExportSummary:
    if mode not in {"member", "admin"}:
        raise ValueError("mode must be 'member' or 'admin'")

    tmp_session, session_id = await _clone_session(session_path)

    normalized_chat = normalize_chat_identifier(chat_username)
    if normalized_chat is None:
        raise ValueError("Could not parse chat identifier.")

    client = TelegramClient(session_id, api_id, api_hash)
    await client.start(phone=phone, code_callback=code_callback, password=password_callback)

    try:
        entity = await resolve_chat_entity(client, normalized_chat)
        chat_title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or getattr(entity, "first_name", None)
            or normalized_chat.raw
        )
        if progress_callback:
            progress_callback(f"Resolved chat title: {chat_title}")

        if progress_callback:
            progress_callback("Collecting participants...")

        bot_count, recent_ids = await _collect_bots_and_recent(client, entity, progress_callback)

        participants: List = []
        aggressive = True if mode == "admin" else False
        async for user in client.iter_participants(entity, aggressive=aggressive):
            participants.append(user)
            if progress_callback and len(participants) % 200 == 0:
                progress_callback(f"Participants collected: {len(participants)}")

        if not participants and mode == "member":
            # As a fallback, try to iterate messages to discover at least some senders
            if progress_callback:
                progress_callback("No participants via API; scanning messages as fallback...")
            seen_ids: Set[int] = set()
            async for message in client.iter_messages(entity, limit=history_limit):
                sender_id = getattr(message, "sender_id", None)
                if sender_id and sender_id not in seen_ids:
                    user = await client.get_entity(sender_id)
                    participants.append(user)
                    seen_ids.add(sender_id)
                    if len(participants) >= history_limit:
                        break

        participant_ids = {u.id for u in participants}

        if progress_callback:
            progress_callback("Scanning message activity...")

        last_post_map, last_reaction_map = await _collect_activity_maps(
            client,
            entity,
            participant_ids,
            history_limit,
            progress_callback,
        )

        fieldnames = [
            "user_id",
            "username",
            "is_bot",
            "is_recent",
            "last_post",
            "last_reaction",
        ]

        include_admin_fields = mode == "admin"
        if include_admin_fields:
            fieldnames.extend(["join_date", "status"])

        rows: List[Dict[str, str]] = []
        for user in participants:
            username = user.username
            if not username:
                parts = [user.first_name or "", user.last_name or ""]
                username = " ".join(p for p in parts if p).strip() or "(no username)"

            row = {
                "user_id": str(user.id),
                "username": username,
                "is_bot": "true" if user.bot else "false",
                "is_recent": "true" if user.id in recent_ids else "false",
                "last_post": _format_dt(last_post_map.get(user.id)),
                "last_reaction": _format_reaction(last_reaction_map.get(user.id)),
            }

            if include_admin_fields:
                participant_info = getattr(user, "participant", None)
                join_date = getattr(participant_info, "date", None)
                row["join_date"] = _format_dt(join_date)
                row["status"] = _status_from_participant(participant_info)

            rows.append(row)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        if progress_callback:
            progress_callback(f"Saved CSV: {output_path}")

        summary = ExportSummary(
            total_members=len(participants),
            bot_count=bot_count,
            recent_count=len(recent_ids),
            csv_path=output_path,
            chat_title=chat_title,
        )
        return summary
    finally:
        await client.disconnect()
        if tmp_session:
            try:
                tmp_session.unlink(missing_ok=True)
            except OSError:
                pass


def _prompt_code() -> str:
    return input("Telegram Code: ")


def _prompt_password() -> str:
    return input("Telegram Passwort (leer lassen, falls nicht gesetzt): ")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Telegram member data to CSV")
    parser.add_argument("--api-id", type=int, required=True)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--phone", required=True)
    parser.add_argument("--chat", required=True, help="Username, invite link or chat id")
    parser.add_argument("--output", required=True, help="Pfad zur CSV-Datei")
    parser.add_argument("--mode", choices=["member", "admin"], default="member")
    parser.add_argument("--session", help="Pfad zu bestehender Telethon-Session")
    parser.add_argument(
        "--history-limit",
        type=int,
        default=2000,
        help="Anzahl Nachrichten, die für Aktivitätsdaten gescannt werden",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    output_path = Path(args.output).expanduser()

    asyncio.run(
        export_member_data(
            api_id=args.api_id,
            api_hash=args.api_hash,
            phone=args.phone,
            chat_username=args.chat,
            output_path=output_path,
            mode=args.mode,
            history_limit=args.history_limit,
            session_path=args.session,
            code_callback=_prompt_code,
            password_callback=_prompt_password,
            progress_callback=lambda msg: print(msg),
        )
    )


if __name__ == "__main__":
    main()
