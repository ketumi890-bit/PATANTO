#!/usr/bin/env python3
"""
BCF MUSIC BOT
- BCF MUSIC API ADDED
- Developed By @BcS_RaiFuL
"""
import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream
from pytgcalls.exceptions import NoActiveGroupCall, NotInCallError

load_dotenv()

API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)

# ==== PulseStream API (PRIMARY) ====
PULSE_API_BASE = os.getenv("PULSE_API_BASE", "https://bcfmusic-apis.ai.studio")
PULSE_API_KEY = os.getenv(
    "PULSE_API_KEY",
    "BCF_HERE_YOUR_KEY",
)

if not all([API_ID, API_HASH, BOT_TOKEN, SESSION_STRING, OWNER_ID]):
    print("❌ Missing env vars", flush=True)
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("BCF")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)
logging.getLogger("yt_dlp").setLevel(logging.ERROR)

COOKIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")


# ============ DB ============
conn = sqlite3.connect("bcf_bot.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
conn.executescript("""
    CREATE TABLE IF NOT EXISTS groups (
        chat_id INTEGER PRIMARY KEY, title TEXT,
        play_mode TEXT DEFAULT 'admin', volume INTEGER DEFAULT 100,
        loop_mode TEXT DEFAULT 'off', muted INTEGER DEFAULT 0,
        banned INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS auth_users (
        chat_id INTEGER, user_id INTEGER, PRIMARY KEY (chat_id, user_id));
    CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER);
""")
conn.commit()


def get_group(cid):
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM groups WHERE chat_id=?", (cid,)).fetchone()
    if not row:
        conn.execute("INSERT OR IGNORE INTO groups(chat_id) VALUES(?)", (cid,))
        conn.commit()
        row = conn.execute("SELECT * FROM groups WHERE chat_id=?", (cid,)).fetchone()
    return row


def set_group(cid, **kw):
    keys = ", ".join(f"{k}=?" for k in kw)
    conn.execute(f"UPDATE groups SET {keys} WHERE chat_id=?", list(kw.values()) + [cid])
    conn.commit()


def is_auth(cid, uid):
    if not uid:
        return False
    return conn.execute(
        "SELECT 1 FROM auth_users WHERE chat_id=? AND user_id=?", (cid, uid)
    ).fetchone() is not None


def auth_add(cid, uid):
    conn.execute("INSERT OR IGNORE INTO auth_users(chat_id,user_id) VALUES(?,?)", (cid, uid))
    conn.commit()


def auth_remove(cid, uid):
    conn.execute("DELETE FROM auth_users WHERE chat_id=? AND user_id=?", (cid, uid))
    conn.commit()


def incr_stat(k):
    conn.execute(
        "INSERT INTO stats(key,value) VALUES(?,1) ON CONFLICT(key) DO UPDATE SET value=value+1",
        (k,),
    )
    conn.commit()


def get_stat(k):
    r = conn.execute("SELECT value FROM stats WHERE key=?", (k,)).fetchone()
    return r[0] if r else 0


# ============ API STATE TRACKERS ============
pulse_available = True


# ============ HELPERS ============
def _extract_video_id(query):
    if not query:
        return None
    m = re.search(r"(?:v=|/)([0-9A-Za-z_-]{11})(?:[?&/]|$)", query)
    if m:
        return m.group(1)
    m = re.search(r"youtu\.be/([0-9A-Za-z_-]{11})", query)
    if m:
        return m.group(1)
    if re.match(r"^[0-9A-Za-z_-]{11}$", query):
        return query
    return None


async def _yt_search(query):
    """Search YouTube for video metadata."""
    try:
        import yt_dlp
        opts = {
            "quiet": True, "no_warnings": True, "noplaylist": True,
            "skip_download": True, "socket_timeout": 15,
            "nocheckcertificate": True, "extract_flat": True,
        }
        loop = asyncio.get_event_loop()
        def _do():
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(f"ytsearch1:{query}", download=False)
        info = await loop.run_in_executor(None, _do)
        if info and "entries" in info and info["entries"]:
            entry = info["entries"][0]
            return {
                "video_id": entry.get("id"),
                "title": entry.get("title", query),
                "duration": entry.get("duration") or 0,
            }
    except Exception as e:
        log.debug("[search] %s", str(e)[:100])
    return None


# ============ PULSESTREAM API (PRIMARY) ============
def _pulse_headers():
    return {"x-api-key": PULSE_API_KEY, "User-Agent": "BCF-Music-Bot/34"}


async def _pulse_health():
    """Check PulseStream API health. Returns dict or None."""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(f"{PULSE_API_BASE}/api/v1/health") as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        log.debug("[Pulse-health] %s", str(e)[:100])
    return None


async def _pulse_search(query: str, limit: int = 5):
    """Search PulseStream. Returns list of track dicts."""
    global pulse_available
    if not pulse_available:
        return []

    try:
        params = {"q": query, "limit": limit, "api_key": PULSE_API_KEY}
        timeout = aiohttp.ClientTimeout(total=20, sock_connect=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(
                f"{PULSE_API_BASE}/api/v1/search",
                params=params,
                headers=_pulse_headers(),
            ) as r:
                if r.status == 429:
                    log.warning("[Pulse] ⚠️ Rate-limited")
                    return []
                if r.status != 200:
                    log.debug("[Pulse] search %s: HTTP %s", query[:30], r.status)
                    return []
                data = await r.json()
                tracks = data.get("tracks") or data.get("results") or []
                if not tracks and isinstance(data, list):
                    tracks = data
                return tracks
    except Exception as e:
        log.debug("[Pulse-search] %s: %s", query[:30], str(e)[:120])
        return []


async def _pulse_play(query: str, quality: int = 320):
    """One-shot search+stream URL from PulseStream."""
    global pulse_available
    if not pulse_available:
        return None

    try:
        params = {
            "q": query,
            "quality": quality,
            "format": "mp3",
            "redirect": 1,  # redirect to audio CDN (PyTgCalls friendly)
            "api_key": PULSE_API_KEY,
        }
        timeout = aiohttp.ClientTimeout(total=25, sock_connect=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            # Follow the redirect ourselves to grab the final CDN URL
            async with s.get(
                f"{PULSE_API_BASE}/api/v1/play",
                params=params,
                headers=_pulse_headers(),
                allow_redirects=True,
            ) as r:
                if r.status == 429:
                    log.warning("[Pulse] ⚠️ Rate-limited on /play")
                    return None
                if r.status not in (200, 206):
                    log.debug("[Pulse] /play HTTP %s", r.status)
                    return None
                final_url = str(r.url)
                ct = r.headers.get("Content-Type", "").lower()

                # If redirected to CDN audio
                if "/api/v1/play" not in final_url and (
                    "audio" in ct or "mpeg" in ct or "octet" in ct or "mp4" in ct
                ):
                    log.info("[Pulse-play] ✅ %s", final_url[:80])
                    return final_url
                # If raw bytes are returned (no redirect)
                if "audio" in ct or "mpeg" in ct or "octet" in ct:
                    return (
                        f"{PULSE_API_BASE}/api/v1/play"
                        f"?q={query}&quality={quality}&format=mp3"
                        f"&api_key={PULSE_API_KEY}"
                    )
    except Exception as e:
        log.debug("[Pulse-play] %s: %s", query[:30], str(e)[:120])
    return None


async def _pulse_stream_url(track_id: str, quality: int = 320):
    """Build a /stream/:id URL for a known PulseStream track ID."""
    if not track_id:
        return None
    return (
        f"{PULSE_API_BASE}/api/v1/stream/{track_id}"
        f"?quality={quality}&format=mp3&api_key={PULSE_API_KEY}"
    )


async def _pulse_resolve(query: str):
    """
    High-level PulseStream resolver.
    Returns (title, stream_url, duration) or None.
    Strategy:
      1) /search → pick top track → build /stream/:id URL
      2) /play  → one-shot redirect URL
    """
    global pulse_available

    if not pulse_available:
        return None

    # --- 1) Search first (gives metadata + ID) ---
    tracks = await _pulse_search(query, limit=3)
    if tracks:
        # Prefer tracks with stream_url available
        best = None
        for t in tracks:
            if t.get("stream_url"):
                best = t
                break
        if not best:
            best = tracks[0]

        title = best.get("title") or query
        duration = int(best.get("duration") or 0)
        # duration sometimes comes as milliseconds
        if duration > 100000:
            duration //= 1000

        # Prefer direct stream_url if present
        if best.get("stream_url"):
            su = best["stream_url"]
            if "api_key=" not in su:
                su += ("&" if "?" in su else "?") + f"api_key={PULSE_API_KEY}"
            log.info("[Pulse-search] ✅ %s", title[:60])
            return (title, su, duration)

        # Fall back to /stream/:id
        tid = best.get("id")
        if tid:
            su = await _pulse_stream_url(tid)
            if su:
                log.info("[Pulse-stream] ✅ %s", title[:60])
                return (title, su, duration)

    # --- 2) /play one-shot ---
    play_url = await _pulse_play(query, quality=320)
    if play_url:
        log.info("[Pulse-play] ✅ %s", query[:60])
        return (query.title(), play_url, 0)

    return None


# ============ YT-DLP FALLBACK ============
def _ytdlp_extract(query):
    """yt-dlp multi-format fallback."""
    try:
        import yt_dlp
    except ImportError:
        return None

    is_url = query.startswith("http://") or query.startswith("https://")
    has_cookies = os.path.isfile(COOKIES_PATH)

    def _try_target(target, opts_extra=None):
        opts = {
            "quiet": True, "no_warnings": True, "noplaylist": True,
            "skip_download": True, "socket_timeout": 20,
            "nocheckcertificate": True, "prefer_free_formats": True,
        }
        if opts_extra:
            opts.update(opts_extra)
        if has_cookies:
            opts["cookiefile"] = COOKIES_PATH

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(target, download=False)
            if not info:
                return None
            if "entries" in info:
                entries = [e for e in info["entries"] if e]
                if not entries:
                    return None
                info = entries[0]

            title = info.get("title", "Unknown")
            duration = info.get("duration") or 0
            url = info.get("url") or ""

            if "m3u8" in url or "manifest" in url:
                url = ""

            if not url:
                best_audio = None
                best_any = None
                for f in (info.get("formats") or []):
                    fu = f.get("url") or ""
                    proto = (f.get("protocol") or "").lower()
                    if not fu.startswith("http"):
                        continue
                    if "m3u8" in proto or "manifest" in fu:
                        continue
                    if f.get("acodec") in (None, "none"):
                        continue
                    abr = f.get("abr") or f.get("tbr") or 0
                    is_audio = f.get("vcodec") in (None, "none")
                    if is_audio:
                        if best_audio is None or abr > best_audio[1]:
                            best_audio = (fu, abr)
                    else:
                        if best_any is None or abr > best_any[1]:
                            best_any = (fu, abr)
                if best_audio:
                    url = best_audio[0]
                elif best_any:
                    url = best_any[0]

            if not url:
                return None

            return (title, url, int(duration))
        except Exception as e:
            log.debug("[yt-dlp] %s: %s", target[:30], str(e)[:80])
            return None

    if is_url:
        r = _try_target(query)
        if r:
            log.info("[yt-dlp-URL] ✅ %s", r[0][:50])
            return r

    log.info("[SC] SoundCloud search for %r", query)
    r = _try_target(f"scsearch1:{query}", {
        "format": "bestaudio/best",
        "http_headers": {"User-Agent": "Mozilla/5.0"},
    })
    if r:
        log.info("[SC] ✅ %s", r[0][:50])
        return r

    log.info("[YT] YouTube search for %r", query)
    r = _try_target(f"ytsearch1:{query}", {
        "format": "bestaudio/best",
        "extractor_args": {"youtube": {"player_client": ["android", "ios", "mweb"]}},
    })
    if r:
        log.info("[YT] ✅ %s", r[0][:50])
        return r

    return None


async def _ytdlp_extract_async(query):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ytdlp_extract, query)


# ============ MASTER RESOLVER ============
async def resolve_audio(query):
    """
    Priority:
      1) Direct MP3/M4A URL
      2) PulseStream  (primary — unlimited, best quality)
      3) yt-dlp       (SoundCloud → YouTube fallback)
    """
    is_url = query.startswith("http://") or query.startswith("https://")

    # ---- 1) Direct MP3/M4A URL ----
    if is_url and any(query.lower().endswith(ext) for ext in [".mp3", ".m4a", ".webm", ".ogg"]):
        title = query.rsplit("/", 1)[-1].split("?")[0][:60]
        log.info("Direct URL: %s", title)
        return (title, query, 0)

    # ---- 2) PulseStream (primary) ----
    #    Works for plain search queries and non-YouTube URLs
    if not (is_url and _extract_video_id(query)):
        pulse_res = await _pulse_resolve(query)
        if pulse_res:
            log.info("[Pulse] ✅ %s", pulse_res[0][:60])
            return pulse_res

    # ---- 3) YouTube URL path (Pulse → yt-dlp) ----
    if is_url and _extract_video_id(query):
        vid = _extract_video_id(query)
        title = f"YouTube {vid}"
        duration = 0
        try:
            import yt_dlp
            opts = {"quiet": True, "no_warnings": True, "skip_download": True,
                    "socket_timeout": 10, "extract_flat": True}
            loop = asyncio.get_event_loop()
            def _info():
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(f"https://youtube.com/watch?v={vid}", download=False)
            info = await loop.run_in_executor(None, _info)
            if info:
                title = info.get("title", title)
                duration = info.get("duration") or 0
        except Exception:
            pass

        # Try PulseStream with the title
        pulse_res = await _pulse_resolve(title)
        if pulse_res:
            return pulse_res

        # Then yt-dlp
        r = await _ytdlp_extract_async(query)
        if r:
            return r

    # ---- 4) Plain search: Pulse → YouTube search → Pulse(title) → yt-dlp ----
    log.info("[Search] %r", query)
    sr = await _yt_search(query)
    if sr:
        vid = sr["video_id"]
        title = sr["title"]
        duration = sr["duration"]
        log.info("[Search] ✅ %s → %s", vid, title[:50])

        # Try Pulse with the resolved title (more accurate than raw query)
        pulse_res = await _pulse_resolve(title)
        if pulse_res:
            return pulse_res

        r = await _ytdlp_extract_async(f"https://youtube.com/watch?v={vid}")
        if r:
            return r

    # ---- 5) Absolute final fallback ----
    log.info("[Fallback] yt-dlp for %r", query)
    r = await _ytdlp_extract_async(query)
    if r:
        return r

    # Last-ditch Pulse attempt
    pulse_res = await _pulse_resolve(query)
    if pulse_res:
        return pulse_res

    return None


# ============ STATE ============
@dataclass
class Track:
    title: str
    stream_url: str
    duration: int = 0
    requester: int = 0
    video_id: str = ""


@dataclass
class GS:
    chat_id: int
    queue: List[Track] = field(default_factory=list)
    current: Optional[Track] = None
    paused: bool = False
    volume: int = 100
    muted: bool = False
    loop_mode: str = "off"
    player_msg_id: Optional[int] = None
    started_at: float = 0.0

    def next_track(self):
        if self.loop_mode == "one" and self.current:
            return self.current
        if not self.queue:
            return None
        if self.loop_mode == "all" and self.current:
            self.queue.append(self.current)
        return self.queue.pop(0)


states: Dict[int, GS] = {}


def st_for(cid):
    if cid not in states:
        row = get_group(cid)
        states[cid] = GS(
            chat_id=cid,
            volume=row["volume"] if row else 100,
            loop_mode=row["loop_mode"] if row else "off",
            muted=bool(row["muted"]) if row else False,
        )
    return states[cid]


# ============ CLIENTS ============
bot = Client("bcf_bot", api_id=API_ID, api_hash=API_HASH,
             bot_token=BOT_TOKEN, parse_mode=ParseMode.HTML, workers=50)

assistant = Client("bcf_assistant", api_id=API_ID, api_hash=API_HASH,
                   session_string=SESSION_STRING, workers=50)

call = None
START_TS = time.time()


# ============ HELPERS ============
def fmt_dur(s):
    if not s or s <= 0:
        return "—:—"
    m, s = divmod(int(s), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _safe_uid(message: Message) -> int:
    try:
        if message.from_user:
            return message.from_user.id
        if message.sender_chat:
            return message.sender_chat.id
        if message.chat:
            return message.chat.id
    except Exception:
        pass
    return 0


def _safe_name(message: Message) -> str:
    try:
        if message.from_user:
            return message.from_user.first_name or "User"
        if message.sender_chat:
            return message.sender_chat.title or "Chat"
        if message.chat:
            return message.chat.title or "Chat"
    except Exception:
        pass
    return "Unknown"


# ============ PLAYER UI ============
def np_text(st: GS) -> str:
    t = st.current
    if not t:
        return "🎵 <b>BCF BOT</b>\n\n<i>Nothing playing.</i>"

    status_icon = "⏸" if st.paused else "▶️"
    status_text = "Paused" if st.paused else "Playing"
    vol = "🔇" if st.muted else f"🔊 {st.volume}%"

    if st.started_at and t.duration > 0:
        elapsed = int(time.time() - st.started_at)
        elapsed = min(elapsed, t.duration)
        progress_pct = (elapsed / t.duration) * 100
        bar_length = 16
        filled = int(bar_length * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_length - filled)
        time_str = f"{fmt_dur(elapsed)} / {fmt_dur(t.duration)}"
        remaining = t.duration - elapsed
        remain_str = f"⏳ {fmt_dur(remaining)} left"
    else:
        bar = "░" * 16
        time_str = "—:— / —:—"
        remain_str = "⏳ —:—"

    loop_icon = {"off": "➡️", "one": "🔂", "all": "🔁"}.get(st.loop_mode, "➡️")

    full_title = t.title
    if len(full_title) > 80:
        full_title = full_title[:77] + "…"

    return (
        f"🎧 <b>Now Streaming</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎵 <b>{full_title}</b>\n\n"
        f"<code>{bar}</code>\n"
        f"<code>{time_str}</code>  ·  {remain_str}\n\n"
        f"{status_icon} <b>{status_text}</b>  ·  {vol}  ·  {loop_icon} <code>{st.loop_mode}</code>\n"
        f"📃 Queue: <b>{len(st.queue)}</b>  ·  👤 <code>{t.requester}</code>"
    )


def player_kb(st: GS) -> InlineKeyboardMarkup:
    pause_btn = (
        InlineKeyboardButton("▶️ Resume", callback_data="resume")
        if st.paused else
        InlineKeyboardButton("⏸ Pause", callback_data="pause")
    )
    mute_btn = (
        InlineKeyboardButton("🔈 Unmute", callback_data="unmute")
        if st.muted else
        InlineKeyboardButton("🔇 Mute", callback_data="mute")
    )
    return InlineKeyboardMarkup([
        [pause_btn, InlineKeyboardButton("⏹ Stop", callback_data="stop")],
        [mute_btn,
         InlineKeyboardButton("📃 Queue", callback_data="queue"),
         InlineKeyboardButton("ℹ️ Help", callback_data="help")],
    ])


async def edit_player(cid, st):
    if not st.player_msg_id:
        return
    try:
        await bot.edit_message_text(cid, st.player_msg_id, np_text(st),
                                    reply_markup=player_kb(st))
    except Exception:
        pass


async def is_admin(cid, uid):
    if not uid:
        return False
    if uid == OWNER_ID:
        return True
    try:
        m = await bot.get_chat_member(cid, uid)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False


async def can_control(cid, uid):
    if not uid:
        return False
    if uid == OWNER_ID:
        return True
    if is_auth(cid, uid):
        return True
    if await is_admin(cid, uid):
        return True
    row = get_group(cid)
    return bool(row and row["play_mode"] == "all")


# ============ STREAMING ============
async def stream_track(cid, track: Track):
    st = st_for(cid)
    try:
        if call is None:
            await bot.send_message(cid, "❌ PyTgCalls not ready.")
            return False

        stream = MediaStream(track.stream_url)
        await call.play(cid, stream)
        st.current = track
        st.paused = False
        st.started_at = time.time()
        with suppress(Exception):
            await call.change_volume_call(cid, st.volume or 100)
        incr_stat("tracks_played")
        await edit_player(cid, st)
        log.info("▶️ Playing: %s in %s", track.title[:60], cid)
        return True
    except NoActiveGroupCall:
        await bot.send_message(cid, "❌ No active voice chat. Start one first.")
        return False
    except Exception as e:
        log.exception("stream_track: %s", e)
        await bot.send_message(cid, f"❌ {type(e).__name__}: {str(e)[:150]}")
        return False


async def play_next(cid):
    st = st_for(cid)
    nxt = st.next_track()
    if nxt is None:
        st.current = None
        st.started_at = 0
        await edit_player(cid, st)
        with suppress(Exception):
            await call.leave_group_call(cid)
        return
    if not await stream_track(cid, nxt):
        st.current = None


async def force_leave(cid):
    if call is None:
        return False
    for attempt in range(10):
        try:
            await call.leave_group_call(cid)
            log.info("✅ Left %s (attempt %d)", cid, attempt + 1)
            return True
        except Exception as e:
            err = str(e)[:80]
            log.debug("leave %d: %s", attempt + 1, err)
            if "not in" in err.lower() or "no active" in err.lower():
                return True
            try:
                if hasattr(call, '_wait_connect'):
                    call._wait_connect.pop(cid, None)
                if hasattr(call, '_binding') and hasattr(call._binding, '_calls'):
                    call._binding._calls.pop(cid, None)
            except Exception:
                pass
            await asyncio.sleep(0.5)
    try:
        if hasattr(call, 'leave_call'):
            await call.leave_call(cid)
            return True
    except Exception:
        pass
    return False


async def progress_updater():
    while True:
        try:
            await asyncio.sleep(10)
            for cid, st in list(states.items()):
                if st.current and not st.paused and st.player_msg_id:
                    await edit_player(cid, st)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.debug("progress: %s", e)


# ============ HANDLERS ============
@bot.on_message(filters.all, group=999)
async def h_poll_msg(client, message: Message):
    try:
        chat = message.chat
        cs = f"{chat.type.name}:{chat.id}" if chat else "?"
        us = _safe_name(message)
        text = (message.text or message.caption or "")[:50]
        print(f"[POLL] {cs} | {us} | {text!r}", flush=True)
    except Exception:
        pass


@bot.on_callback_query(group=999)
async def h_poll_cb(client, cq: CallbackQuery):
    try:
        print(f"[POLL CB] {cq.data!r}", flush=True)
    except Exception:
        pass


@bot.on_message(filters.command("start"))
async def h_start(client, message: Message):
    uid = _safe_uid(message)
    print(f"[CMD] /start from {uid}", flush=True)
    me = await client.get_me()

    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "🎵 <b>BCF MUSIC BOT</b>\n\n"
            "Start voice chat, then:\n<code>/play song name</code>\n\n"
            "Use /help for commands."
        )
        return

    text = (
        "╭───────────────────────────╮\n"
        "   🎵 <b>BCF MUSIC BOT</b> 🎵\n"
        "╰───────────────────────────╯\n\n"
        "🎧 <b>Stream high-quality music</b>\n"
        "in group voice chats!\n\n"
        "━━━━ <b>Features</b> ━━━━\n"
        "▶️ Multi-source (Saavn, Audius, Radio, YT, SC)\n"
        "🎼 Crystal clear 320kbps audio\n"
        "📊 Live progress bar\n"
        "📃 Queue, Loop, Shuffle\n"
        "🔊 Volume 0-100\n"
        "⚡ Auto-fallback (PulseStream → yt-dlp)\n\n"
        "━━━━ <b>Setup</b> ━━━━\n"
        "1️⃣ Add to group\n"
        "2️⃣ Make bot <b>admin</b>\n"
        "3️⃣ Start voice chat\n"
        "4️⃣ <code>/play song name</code>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add to Group",
                              url=f"https://t.me/{me.username}?startgroup=true")],
        [InlineKeyboardButton("📖 Help", callback_data="help"),
         InlineKeyboardButton("🔧 Setup", callback_data="setup")],
    ])
    await message.reply(text, reply_markup=kb)


@bot.on_message(filters.command("help"))
async def h_help(client, message: Message):
    await message.reply(
        "🎵 <b>BCF BOT Commands</b>\n\n"
        "/play &lt;song&gt; /pause /resume /stop /end\n"
        "/replay /volume &lt;0-100&gt; /mute /unmute\n"
        "/queue /clear /skip /shuffle /loop\n"
        "/nowplaying /ping /stats /settings\n"
        "/auth /unauth /setplaymode"
    )


@bot.on_message(filters.command(["play", "vplay", "p"]))
async def h_play(client, message: Message):
    uid = _safe_uid(message)
    print(f"[CMD] /play from {uid}", flush=True)

    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("❌ Groups only.")

    if len(message.command) < 2:
        return await message.reply("Usage: <code>/play song name</code>")

    q = " ".join(message.command[1:]).strip()
    cid = message.chat.id

    if not await can_control(cid, uid):
        return await message.reply("🔒 Admin only. /setplaymode all.")

    st = st_for(cid)
    if len(st.queue) >= 30:
        return await message.reply("📃 Queue full (30).")

    status = await message.reply(f"🔎 Searching: <b>{q}</b>…")

    result = await resolve_audio(q)
    if not result:
        return await status.edit_text(
            "❌ Could not resolve.\n\n"
            "<b>Try:</b>\n• Different song name\n• YouTube URL\n• Direct mp3 URL"
        )

    title, stream_url, duration = result
    vid = _extract_video_id(q) or ""
    track = Track(
        title=title, stream_url=stream_url, duration=duration,
        requester=uid, video_id=vid,
    )

    if st.current is None:
        await status.delete()
        if await stream_track(cid, track):
            m = await bot.send_message(cid, np_text(st), reply_markup=player_kb(st))
            st.player_msg_id = m.id
    else:
        st.queue.append(track)
        await status.edit_text(
            f"➕ Queued #{len(st.queue)}: <b>{title}</b>\n⏱ {fmt_dur(duration)}"
        )
        await edit_player(cid, st)


@bot.on_message(filters.command("pause"))
async def h_pause(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    if not st.current or st.paused:
        return await message.reply("Nothing to pause.")
    try:
        await call.pause(cid)
        st.paused = True
        await edit_player(cid, st)
        await message.reply("⏸ Paused.")
    except Exception as e:
        await message.reply(f"❌ {e}")


@bot.on_message(filters.command("resume"))
async def h_resume(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    if not st.current or not st.paused:
        return await message.reply("Nothing to resume.")
    try:
        await call.resume(cid)
        st.paused = False
        await edit_player(cid, st)
        await message.reply("▶️ Resumed.")
    except Exception as e:
        await message.reply(f"❌ {e}")


@bot.on_message(filters.command("skip"))
async def h_skip(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    if not st.current:
        return await message.reply("Nothing playing.")
    st.current = None
    await play_next(cid)
    await message.reply("⏭ Skipped.")


@bot.on_message(filters.command("stop"))
async def h_stop(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    st.queue.clear()
    st.current = None
    st.paused = False
    st.started_at = 0
    ok = await force_leave(cid)
    await message.reply("⏹ Stopped & left voice chat." if ok else "⏹ Stopped.")
    await edit_player(cid, st)


@bot.on_message(filters.command("end"))
async def h_end(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    st.queue.clear()
    st.current = None
    st.started_at = 0
    ok = await force_leave(cid)
    await message.reply("👋 Left voice chat." if ok else "👋 Force leave attempted.")


@bot.on_message(filters.command("replay"))
async def h_replay(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    if not st.current:
        return await message.reply("Nothing to replay.")
    await stream_track(cid, st.current)
    await message.reply("🔄 Replaying.")


@bot.on_message(filters.command("volume"))
async def h_volume(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    if len(message.command) < 2:
        return await message.reply("Usage: <code>/volume 50</code> (0-100)")
    try:
        v = int(message.command[1])
        if not 0 <= v <= 100:
            raise ValueError
    except ValueError:
        return await message.reply("Volume must be 0-100.")
    st = st_for(cid)
    try:
        await call.change_volume_call(cid, v)
        st.volume = v
        set_group(cid, volume=v)
        await message.reply(f"🔊 Volume: <b>{v}%</b>")
        await edit_player(cid, st)
    except Exception as e:
        await message.reply(f"❌ {e}")


@bot.on_message(filters.command("mute"))
async def h_mute(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    try:
        await call.mute(cid)
        st.muted = True
        set_group(cid, muted=1)
        await message.reply("🔇 Muted.")
        await edit_player(cid, st)
    except Exception as e:
        await message.reply(f"❌ {e}")


@bot.on_message(filters.command("unmute"))
async def h_unmute(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    try:
        await call.unmute(cid)
        st.muted = False
        set_group(cid, muted=0)
        await message.reply("🔈 Unmuted.")
        await edit_player(cid, st)
    except Exception as e:
        await message.reply(f"❌ {e}")


@bot.on_message(filters.command("queue"))
async def h_queue(client, message: Message):
    st = st_for(message.chat.id)
    if not st.current and not st.queue:
        return await message.reply("📃 Queue is empty.")
    lines = ["📃 <b>Queue</b>\n"]
    if st.current:
        lines.append(f"▶️ <b>Now:</b> {st.current.title}")
    for i, t in enumerate(st.queue, 1):
        lines.append(f"{i}. {t.title}")
    await message.reply("\n".join(lines))


@bot.on_message(filters.command("clear"))
async def h_clear(client, message: Message):
    if not await can_control(message.chat.id, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st_for(message.chat.id).queue.clear()
    await message.reply("🧹 Queue cleared.")


@bot.on_message(filters.command("shuffle"))
async def h_shuffle(client, message: Message):
    import random
    if not await can_control(message.chat.id, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    random.shuffle(st_for(message.chat.id).queue)
    await message.reply("🔀 Shuffled.")


@bot.on_message(filters.command("loop"))
async def h_loop(client, message: Message):
    cid = message.chat.id
    if not await can_control(cid, _safe_uid(message)):
        return await message.reply("🔒 Admin only.")
    st = st_for(cid)
    arg = (message.command[1] if len(message.command) > 1 else "").lower()
    if arg in ("off", "0"):
        st.loop_mode = "off"
    elif arg in ("one", "1"):
        st.loop_mode = "one"
    elif arg in ("all", ""):
        st.loop_mode = "all" if st.loop_mode != "all" else "off"
    else:
        return await message.reply("Usage: <code>/loop off|one|all</code>")
    set_group(cid, loop_mode=st.loop_mode)
    await message.reply(f"🔁 Loop: <b>{st.loop_mode}</b>")
    await edit_player(cid, st)


@bot.on_message(filters.command(["nowplaying", "np"]))
async def h_np(client, message: Message):
    st = st_for(message.chat.id)
    await message.reply(np_text(st), reply_markup=player_kb(st))


@bot.on_message(filters.command("ping"))
async def h_ping(client, message: Message):
    t = time.time()
    m = await message.reply("🏓")
    await m.edit_text(f"🏓 Pong! <code>{(time.time() - t) * 1000:.0f} ms</code>")


@bot.on_message(filters.command("stats"))
async def h_stats(client, message: Message):
    await message.reply(
        f"📊 <b>Stats</b>\nActive: <code>{len(states)}</code>\n"
        f"Played: <code>{get_stat('tracks_played')}</code>\n"
        f"Uptime: <code>{fmt_dur(int(time.time() - START_TS))}</code>"
    )


@bot.on_message(filters.command("settings"))
async def h_settings(client, message: Message):
    row = get_group(message.chat.id)
    await message.reply(
        f"⚙️ <b>Settings</b>\nMode: <code>{row['play_mode']}</code>\n"
        f"Volume: <code>{row['volume']}</code>\n"
        f"Loop: <code>{row['loop_mode']}</code>\n"
        f"Muted: <code>{bool(row['muted'])}</code>"
    )


@bot.on_message(filters.command("auth"))
async def h_auth(client, message: Message):
    cid = message.chat.id
    if not await is_admin(cid, _safe_uid(message)):
        return await message.reply("🔒 Admins only.")
    if len(message.command) < 2:
        return await message.reply("Usage: <code>/auth USER_ID</code>")
    try:
        uid = int(message.command[1])
    except ValueError:
        return await message.reply("Invalid user ID.")
    auth_add(cid, uid)
    await message.reply(f"✅ Authorized <code>{uid}</code>.")


@bot.on_message(filters.command("unauth"))
async def h_unauth(client, message: Message):
    cid = message.chat.id
    if not await is_admin(cid, _safe_uid(message)):
        return await message.reply("🔒 Admins only.")
    if len(message.command) < 2:
        return await message.reply("Usage: <code>/unauth USER_ID</code>")
    try:
        uid = int(message.command[1])
    except ValueError:
        return await message.reply("Invalid user ID.")
    auth_remove(cid, uid)
    await message.reply(f"✅ Removed <code>{uid}</code>.")


@bot.on_message(filters.command("setplaymode"))
async def h_setplaymode(client, message: Message):
    cid = message.chat.id
    if not await is_admin(cid, _safe_uid(message)):
        return await message.reply("🔒 Admins only.")
    if len(message.command) < 2 or message.command[1].lower() not in ("admin", "all"):
        return await message.reply("Usage: <code>/setplaymode admin|all</code>")
    set_group(cid, play_mode=message.command[1].lower())
    await message.reply(f"✅ Mode: <b>{message.command[1].lower()}</b>")


@bot.on_message(filters.command("admin"))
async def h_admin(client, message: Message):
    if _safe_uid(message) != OWNER_ID:
        return await message.reply("🔒 Owner only.")
    import pytgcalls
    pulse_status = "✅ available" if pulse_available else "❌ disabled"
    await message.reply(
        f"🛡 <b>Admin</b>\n\n"
        f"Active: <code>{len(states)}</code>\n"
        f"Played: <code>{get_stat('tracks_played')}</code>\n"
        f"Uptime: <code>{fmt_dur(int(time.time() - START_TS))}</code>\n"
        f"py-tgcalls: <code>{pytgcalls.__version__}</code>\n"
        f"PulseStream: {pulse_status}"
    )


@bot.on_message(filters.command(["apitest", "test"]))
async def h_apitest(client, message: Message):
    if _safe_uid(message) != OWNER_ID:
        return await message.reply("🔒 Owner only.")
    m = await message.reply("🔎 Testing all sources…")

    lines = ["🔎 <b>Source Test</b>\n"]

    # PulseStream
    ph = await _pulse_health()
    if ph and ph.get("status") == "ok":
        srcs = ", ".join(ph.get("sources") or [])
        lines.append(f"✅ PulseStream: healthy ({srcs})")
    else:
        lines.append(f"❌ PulseStream: failed")

    # yt-dlp
    try:
        import yt_dlp
        lines.append(f"✅ yt-dlp: v{yt_dlp.version.__version__}")
    except Exception:
        lines.append(f"❌ yt-dlp: missing")

    # Live resolve test
    result = await resolve_audio("Kesariya")
    if result:
        lines.append(f"\n✅ Resolve: {result[0][:50]}")
    else:
        lines.append(f"\n❌ Resolve: failed")

    await m.edit_text("\n".join(lines))


@bot.on_message(filters.command(["trending", "trend"]))
async def h_trending(client, message: Message):
    """Fetch trending songs from PulseStream (bengali by default)."""
    lang = "bengali"
    if len(message.command) > 1:
        lang = message.command[1].lower()
    valid = {"english", "hindi", "bengali", "bangla", "punjabi", "lofi", "phonk"}
    if lang not in valid:
        return await message.reply(
            "Usage: <code>/trending english|hindi|bengali|punjabi|lofi|phonk</code>"
        )

    m = await message.reply(f"🔎 Fetching {lang} trending…")
    try:
        params = {"lang": lang, "limit": 10, "api_key": PULSE_API_KEY}
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(
                f"{PULSE_API_BASE}/api/v1/trending",
                params=params, headers=_pulse_headers(),
            ) as r:
                if r.status != 200:
                    return await m.edit_text(f"❌ Trending failed (HTTP {r.status})")
                data = await r.json()
                tracks = data.get("tracks") or data.get("results") or []
                if not tracks:
                    return await m.edit_text("📭 No trending tracks found.")
                lines = [f"🔥 <b>Trending — {lang.title()}</b>\n"]
                for i, t in enumerate(tracks[:10], 1):
                    title = (t.get("title") or "?")[:50]
                    artist = (t.get("artist") or t.get("singers") or "")[:30]
                    dur = int(t.get("duration") or 0)
                    if dur > 100000:
                        dur //= 1000
                    lines.append(f"{i}. <b>{title}</b> — {artist} <code>{fmt_dur(dur)}</code>")
                lines.append("\n💡 <code>/play song name</code> to play")
                await m.edit_text("\n".join(lines))
    except Exception as e:
        await m.edit_text(f"❌ {type(e).__name__}: {str(e)[:120]}")


@bot.on_message(filters.command(["lyrics", "lyric"]))
async def h_lyrics(client, message: Message):
    """Fetch lyrics from PulseStream for a song query."""
    if len(message.command) < 2:
        return await message.reply("Usage: <code>/lyrics song name</code>")
    q = " ".join(message.command[1:]).strip()
    m = await message.reply(f"🔎 Finding lyrics for <b>{q}</b>…")

    # Search to get a saavn track ID
    tracks = await _pulse_search(q, limit=1)
    if not tracks:
        return await m.edit_text("❌ Song not found.")

    tid = tracks[0].get("id") or ""
    if not tid.startswith("saavn:"):
        # Try to find saavn result
        tracks = await _pulse_search(q, limit=5)
        saavn_tid = next(
            (t.get("id") for t in tracks if (t.get("id") or "").startswith("saavn:")),
            None,
        )
        if not saavn_tid:
            return await m.edit_text("❌ No saavn source for this track.")
        tid = saavn_tid

    # Fetch lyrics
    try:
        params = {"api_key": PULSE_API_KEY}
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(
                f"{PULSE_API_BASE}/api/v1/lyrics/{tid}",
                params=params, headers=_pulse_headers(),
            ) as r:
                if r.status == 404:
                    return await m.edit_text("📭 Lyrics not available for this song.")
                if r.status != 200:
                    return await m.edit_text(f"❌ Lyrics failed (HTTP {r.status})")
                data = await r.json()
                lyrics = (
                    data.get("lyrics")
                    or data.get("plain_lyrics")
                    or data.get("text")
                    or ""
                )
                if not lyrics:
                    return await m.edit_text("📭 Lyrics not available.")
                title = tracks[0].get("title", q)
                # Telegram message limit
                header = f"🎤 <b>{title}</b>\n\n"
                if len(header) + len(lyrics) > 4000:
                    lyrics = lyrics[:3900 - len(header)] + "…"
                await m.edit_text(header + lyrics)
    except Exception as e:
        await m.edit_text(f"❌ {type(e).__name__}: {str(e)[:120]}")


@bot.on_message(filters.command(["radio"]))
async def h_radio(client, message: Message):
    """Search and play live internet radio from PulseStream."""
    q = "lofi"
    if len(message.command) > 1:
        q = " ".join(message.command[1:]).strip()

    cid = message.chat.id
    uid = _safe_uid(message)
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("❌ Groups only.")
    if not await can_control(cid, uid):
        return await message.reply("🔒 Admin only.")

    m = await message.reply(f"🔎 Searching radio: <b>{q}</b>…")
    try:
        params = {"q": q, "limit": 5, "api_key": PULSE_API_KEY}
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(
                f"{PULSE_API_BASE}/api/v1/radio",
                params=params, headers=_pulse_headers(),
            ) as r:
                if r.status != 200:
                    return await m.edit_text(f"❌ Radio search failed (HTTP {r.status})")
                data = await r.json()
                stations = data.get("stations") or data.get("tracks") or data.get("results") or []
                if not stations:
                    return await m.edit_text("📭 No radio stations found.")
                top = stations[0]
                title = top.get("title") or top.get("name") or q
                stream_url = top.get("stream_url") or top.get("url")
                if not stream_url:
                    return await m.edit_text("❌ Radio stream URL missing.")
                if "api_key=" not in stream_url:
                    stream_url += ("&" if "?" in stream_url else "?") + f"api_key={PULSE_API_KEY}"

                st = st_for(cid)
                track = Track(
                    title=f"📻 {title}", stream_url=stream_url,
                    duration=0, requester=uid, video_id="",
                )
                if st.current is None:
                    await m.delete()
                    if await stream_track(cid, track):
                        pm = await bot.send_message(cid, np_text(st), reply_markup=player_kb(st))
                        st.player_msg_id = pm.id
                else:
                    st.queue.append(track)
                    await m.edit_text(f"➕ Queued radio: <b>{title}</b>")
                    await edit_player(cid, st)
    except Exception as e:
        await m.edit_text(f"❌ {type(e).__name__}: {str(e)[:120]}")


@bot.on_message(filters.new_chat_members)
async def h_new_members(client, message: Message):
    try:
        me = await client.get_me()
        if me.id in [m.id for m in message.new_chat_members]:
            await message.reply(
                "🎵 <b>BCF MUSIC BOT joined!</b>\n\n"
                "1. Make me <b>admin</b>\n2. Start voice chat\n3. <code>/play song name</code>"
            )
    except Exception:
        pass


# ============ CALLBACKS ============
@bot.on_callback_query(filters.regex(r"^setup$"))
async def cb_setup(client, cq: CallbackQuery):
    await cq.answer()
    await cq.message.reply(
        "🔧 <b>Setup</b>\n\n"
        "1. Bot admin (Invite + Voice Chats)\n"
        "2. Start voice chat\n3. <code>/play song name</code>"
    )


@bot.on_callback_query(filters.regex(r"^help$"))
async def cb_help(client, cq: CallbackQuery):
    await cq.answer()
    await cq.message.reply(
        "🎵 /play /pause /resume /stop /queue /mute /unmute /volume /nowplaying"
    )


@bot.on_callback_query(filters.regex(
    r"^(pause|resume|skip|stop|loop|shuffle|queue|mute|unmute|replay|help)$"))
async def h_cb_player(client, cq: CallbackQuery):
    cid = cq.message.chat.id
    uid = cq.from_user.id if cq.from_user else 0

    if not await can_control(cid, uid):
        return await cq.answer("🔒 Admin only.", show_alert=True)

    st = st_for(cid)
    d = cq.data

    try:
        if d == "pause":
            if not st.current:
                return await cq.answer("Nothing playing.", show_alert=True)
            if st.paused:
                return await cq.answer("Already paused.", show_alert=True)
            await call.pause(cid)
            st.paused = True
            await edit_player(cid, st)
            return await cq.answer("⏸ Paused")

        elif d == "resume":
            if not st.current:
                return await cq.answer("Nothing playing.", show_alert=True)
            if not st.paused:
                return await cq.answer("Already playing.", show_alert=True)
            await call.resume(cid)
            st.paused = False
            await edit_player(cid, st)
            return await cq.answer("▶️ Resumed")

        elif d == "skip":
            if not st.current:
                return await cq.answer("Nothing playing.", show_alert=True)
            st.current = None
            st.paused = False
            await play_next(cid)
            return await cq.answer("⏭ Skipped")

        elif d == "stop":
            st.queue.clear()
            st.current = None
            st.paused = False
            st.started_at = 0
            ok = await force_leave(cid)
            await edit_player(cid, st)
            return await cq.answer("⏹ Stopped" if ok else "⏹ Stopped")

        elif d == "loop":
            st.loop_mode = {"off": "all", "all": "one", "one": "off"}[st.loop_mode]
            set_group(cid, loop_mode=st.loop_mode)
            await edit_player(cid, st)
            return await cq.answer(f"🔁 Loop: {st.loop_mode}")

        elif d == "shuffle":
            import random
            random.shuffle(st.queue)
            return await cq.answer(f"🔀 Shuffled ({len(st.queue)})")

        elif d == "mute":
            await call.mute(cid)
            st.muted = True
            set_group(cid, muted=1)
            await edit_player(cid, st)
            return await cq.answer("🔇 Muted")

        elif d == "unmute":
            await call.unmute(cid)
            st.muted = False
            set_group(cid, muted=0)
            await edit_player(cid, st)
            return await cq.answer("🔈 Unmuted")

        elif d == "replay":
            if not st.current:
                return await cq.answer("Nothing to replay.", show_alert=True)
            await stream_track(cid, st.current)
            return await cq.answer("🔄 Replaying")

        elif d == "queue":
            if not st.current and not st.queue:
                return await cq.answer("Queue empty.", show_alert=True)
            txt = "\n".join(f"{i}. {t.title}" for i, t in enumerate(st.queue, 1)) or "Empty"
            return await cq.answer(txt[:190], show_alert=True)

        elif d == "help":
            return await cq.answer("Use /help", show_alert=True)

    except Exception as e:
        log.exception("cb %s: %s", d, e)
        await cq.answer(f"Error: {type(e).__name__}", show_alert=True)


# ============ MAIN ============
async def main():
    global call, pulse_available

    print("\n" + "=" * 60, flush=True)
    print("  BCF MUSIC BOT — v34 (PulseStream + yt-dlp)", flush=True)
    print("=" * 60, flush=True)

    print(f"🌐 PulseStream: {PULSE_API_BASE}", flush=True)
    print(f"🔑 Pulse Key  : {PULSE_API_KEY[:25]}…", flush=True)

    # PulseStream health check
    ph = await _pulse_health()
    if ph and ph.get("status") == "ok":
        print(f"✅ PulseStream: OK — sources={ph.get('sources')}", flush=True)
    else:
        print("⚠️ PulseStream: health check failed — using yt-dlp fallback", flush=True)

    # PulseStream live test
    test_res = await _pulse_search("Kesariya", limit=1)
    if test_res:
        print(f"✅ PulseStream: search OK ({test_res[0].get('title', '?')[:40]})", flush=True)
    else:
        print("⚠️ PulseStream: search returned 0 results", flush=True)

    # yt-dlp
    try:
        import yt_dlp
        print(f"✅ yt-dlp: v{yt_dlp.version.__version__}", flush=True)
    except Exception:
        print("❌ yt-dlp: not installed", flush=True)

    await bot.start()
    me = await bot.get_me()
    log.info("Bot: @%s (id=%s)", me.username, me.id)

    total = sum(len(g) for g in bot.dispatcher.groups.values())
    print(f"\n📦 Total handlers: {total}", flush=True)

    await assistant.start()
    ame = await assistant.get_me()
    log.info("Assistant: @%s", ame.username)

    log.info("Creating PyTgCalls…")
    call = PyTgCalls(assistant)
    await call.start()
    log.info("PyTgCalls ready")

    progress_task = asyncio.create_task(progress_updater())

    print("\n" + "=" * 60, flush=True)
    print("🎉 BOT LIVE", flush=True)
    print("=" * 60 + "\n", flush=True)

    try:
        await idle()
    finally:
        progress_task.cancel()
        with suppress(Exception):
            await assistant.stop()
        with suppress(Exception):
            await bot.stop()


def run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n⏹ Interrupted.", flush=True)
    except Exception:
        log.exception("Fatal")
        sys.exit(1)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    run()