"""Helpers for normalizing and resolving Telegram chat identifiers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union
from urllib.parse import urlparse
import re

from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import ChatInviteAlready, ChatInvite


@dataclass(frozen=True)
class NormalizedChat:
    raw: str
    target: Union[str, int]
    invite_hash: Optional[str] = None


def normalize_chat_identifier(chat: Union[str, int, NormalizedChat]) -> Optional[NormalizedChat]:
    if isinstance(chat, NormalizedChat):
        return chat
    if isinstance(chat, int):
        return NormalizedChat(raw=str(chat), target=chat)

    if not isinstance(chat, str):
        return None

    raw = chat.strip()
    if not raw:
        return None

    # Direct channel/group id (e.g. -1001234567890)
    if re.fullmatch(r"-?100\d{5,}", raw):
        try:
            target = int(raw)
        except Exception:
            target = raw
        return NormalizedChat(raw=raw, target=target)

    lower = raw.lower()
    domain_aliases = {
        "t.me",
        "telegram.me",
        "telegram.dog",
        "www.t.me",
        "www.telegram.me",
        "www.telegram.dog",
    }

    invite_hash: Optional[str] = None
    target: Union[str, int] = raw

    def _from_invite(hash_value: str) -> NormalizedChat:
        return NormalizedChat(raw=raw, target=raw, invite_hash=hash_value)

    if lower.startswith("http://") or lower.startswith("https://"):
        try:
            parsed = urlparse(raw)
        except Exception:
            parsed = None
        if parsed and parsed.netloc.lower() in domain_aliases:
            path = parsed.path.strip("/")
            parts = path.split("/") if path else []
            if parts:
                first = parts[0]
                if first == "c" and len(parts) >= 2:
                    numeric = re.sub(r"[^0-9-]", "", parts[1])
                    if numeric:
                        if numeric.startswith("-100"):
                            try:
                                target = int(numeric)
                            except Exception:
                                target = numeric
                        else:
                            full_id = f"-100{numeric.lstrip('-')}"
                            try:
                                target = int(full_id)
                            except Exception:
                                target = full_id
                        return NormalizedChat(raw=raw, target=target)
                if first in {"joinchat", "addstickers"} and len(parts) >= 2:
                    return _from_invite(parts[1])
                if first.startswith("+"):
                    return _from_invite(first.lstrip("+"))
                # Regular username link
                return NormalizedChat(raw=raw, target=first)

    if raw.startswith("+"):
        return _from_invite(raw.lstrip("+"))

    # Default: treat as username or phone link
    return NormalizedChat(raw=raw, target=raw)


async def resolve_chat_entity(client, chat: NormalizedChat):
    if chat.invite_hash:
        invite_hash = chat.invite_hash
        try:
            invite = await client(CheckChatInviteRequest(invite_hash))
        except UserAlreadyParticipantError:
            invite = await client(CheckChatInviteRequest(invite_hash))
        if isinstance(invite, ChatInviteAlready):
            return await client.get_entity(invite.chat)
        if isinstance(invite, ChatInvite):
            raise ValueError("Invite link could not be resolved. Join the chat first or check permissions.")
        raise ValueError("Invite link could not be resolved. Join the chat first or check permissions.")
    return await client.get_entity(chat.target)
