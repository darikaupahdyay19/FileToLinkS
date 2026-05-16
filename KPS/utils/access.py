# KPS/utils/access.py
"""
Public-mode access control.

When public mode is ON   -> anyone may use the bot (default behaviour).
When public mode is OFF  -> only the OWNER and explicitly authorized users
                            (added via /authorize) may use the bot. Every
                            other user is *silently* ignored: the bot will
                            not reply, will not show "processing", will not
                            even acknowledge that it received the message.

The setting is persisted in the `bot_settings` collection so it survives
restarts. The first time the bot runs, the value is seeded from the
PUBLIC_MODE environment variable (default: True / public).
"""

from typing import Optional

from KPS.utils.database import db
from KPS.utils.logger import logger
from KPS.utils.tokens import allowed
from KPS.vars import Var

_SETTING_KEY = "public_mode"

# In-process cache so we don't hit Mongo on every incoming update.
_cache: Optional[bool] = None


async def _seed_if_needed() -> bool:
    """Seed the DB value from env on first run, return current value."""
    global _cache
    stored = await db.get_setting(_SETTING_KEY, default=None)
    if stored is None:
        await db.set_setting(_SETTING_KEY, bool(Var.PUBLIC_MODE), updated_by=None)
        stored = bool(Var.PUBLIC_MODE)
    _cache = bool(stored)
    return _cache


async def is_public_mode() -> bool:
    """Return current public-mode state (cached)."""
    global _cache
    if _cache is None:
        try:
            return await _seed_if_needed()
        except Exception as e:
            logger.error(f"is_public_mode: failed to load setting, defaulting to env: {e}", exc_info=True)
            return bool(Var.PUBLIC_MODE)
    return _cache


async def set_public_mode(value: bool, updated_by: Optional[int] = None) -> bool:
    """Persist new public-mode state and refresh cache."""
    global _cache
    await db.set_setting(_SETTING_KEY, bool(value), updated_by=updated_by)
    _cache = bool(value)
    logger.info(f"Public mode set to {_cache} by {updated_by}.")
    return _cache


async def is_owner(user_id: Optional[int]) -> bool:
    if user_id is None:
        return False
    owner = Var.OWNER_ID
    if isinstance(owner, (list, tuple, set)):
        return user_id in owner
    return user_id == owner


async def is_user_allowed(user_id: Optional[int]) -> bool:
    """
    Whitelist check used when public mode is OFF.
    Owner and users added via /authorize are allowed; everyone else is not.
    """
    if user_id is None:
        return False
    if await is_owner(user_id):
        return True
    try:
        return await allowed(user_id)
    except Exception as e:
        logger.error(f"is_user_allowed: error checking auth for {user_id}: {e}", exc_info=True)
        return False


async def can_use_bot(user_id: Optional[int]) -> bool:
    """
    Top-level gate: True if this user is allowed to interact with the bot
    given the current public-mode setting.
    """
    if await is_public_mode():
        return True
    return await is_user_allowed(user_id)
