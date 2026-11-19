"""Football Live Manager - Discord bot + realtime API for the Android overlay.

Runs the original /live on|off bot AND a FastAPI WebSocket server in one process.
"""
import asyncio
import os
import secrets
from collections import deque
from datetime import timezone

import discord
import uvicorn
from discord import app_commands
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

GUILD_ID = 1572722827142893599

STREAM_CHANNEL_ID = 1572722829911130135
CHAT_CHANNEL_ID = 1572725601092247552

TOKEN = os.getenv("DISCORD_TOKEN")
CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")  # secret used ONLY by the Android app
PORT = int(os.getenv("PORT", "8000"))
HISTORY_LIMIT = 50

intents = discord.Intents.default()
intents.message_content = True  # needed to read chat text (enable in Developer Portal)

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# --------------------------------------------------------------------------
# Realtime hub (state + connected Android clients)
# --------------------------------------------------------------------------
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.messages: deque = deque(maxlen=HISTORY_LIMIT)
        self.live = False

    async def broadcast(self, payload: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await asyncio.wait_for(ws.send_json(payload), timeout=5)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = Hub()


def member_count() -> int:
    """Non-bot members currently connected to the stream voice channel."""
    guild = client.get_guild(GUILD_ID)
    voice = guild.get_channel(STREAM_CHANNEL_ID) if guild else None
    if voice is None:
        return 0
    return sum(1 for m in voice.members if not m.bot)


def snapshot() -> dict:
    return {
        "type": "state",
        "live": hub.live,
        "members": member_count(),
        "messages": list(hub.messages),
    }


def serialize(m: discord.Message) -> dict:
    content = m.clean_content or ""
    if m.attachments:
        files = " ".join(f"[file: {a.filename}]" for a in m.attachments)
        content = f"{content}\n{files}".strip()
    return {
        "id": str(m.id),
        "author": m.author.display_name,
        "avatar": m.author.display_avatar.with_size(64).with_format("png").url,
        "content": content[:1500],
        "timestamp": m.created_at.astimezone(timezone.utc).isoformat(),
        "ts": int(m.created_at.timestamp() * 1000),
    }


async def refresh_state():
    """Rebuild live flag + recent chat from Discord, then push to all apps."""
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        return
    voice = guild.get_channel(STREAM_CHANNEL_ID)
    chat = guild.get_channel(CHAT_CHANNEL_ID)
    if voice is not None:
        hub.live = voice.overwrites_for(guild.default_role).view_channel is True
    hub.messages.clear()
    if hub.live and chat is not None:
        try:
            recent = [m async for m in chat.history(limit=HISTORY_LIMIT)]
            for m in reversed(recent):
                hub.messages.append(serialize(m))
        except Exception as error:  # missing Read Message History, etc.
            print("Could not load chat history:", repr(error))
    await hub.broadcast(snapshot())


# --------------------------------------------------------------------------
# Original bot logic (unchanged behaviour) + realtime notifications
# --------------------------------------------------------------------------
async def set_live_status(guild, live):
    everyone = guild.default_role
    voice = guild.get_channel(STREAM_CHANNEL_ID)

    if voice is None:
        raise ValueError("LIVE STREAM channel not found")

    voice_permissions = voice.overwrites_for(everyone)
    voice_permissions.view_channel = live
    voice_permissions.connect = live
    voice_permissions.speak = False
    voice_permissions.stream = live

    await voice.set_permissions(everyone, overwrite=voice_permissions)

    chat = guild.get_channel(CHAT_CHANNEL_ID)

    if chat is None:
        raise ValueError("LIVE CHAT channel not found")

    if not live:
        deleted_count = 0
        async for message in chat.history(limit=None):
            await message.delete()
            deleted_count += 1
        print(f"Deleted {deleted_count} messages from LIVE CHAT")

    chat_permissions = chat.overwrites_for(everyone)
    chat_permissions.view_channel = live
    chat_permissions.send_messages = live

    await chat.set_permissions(everyone, overwrite=chat_permissions)

    # --- realtime sync for the Android overlay ---
    hub.live = live
    if not live:
        hub.messages.clear()
        await hub.broadcast({"type": "clear"})
    await hub.broadcast({"type": "status", "live": live})
    await hub.broadcast({"type": "members", "count": member_count()})


@tree.command(name="live", description="Open or close the live channels")
@app_commands.describe(status="Choose whether to start or stop the stream")
@app_commands.choices(status=[
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off")
])
async def live(interaction: discord.Interaction, status: app_commands.Choice[str]):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "You need Administrator permission to control the live stream.",
            ephemeral=True
        )
        return

    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "This command is not configured for this server.",
            ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        is_live = status.value == "on"
        await set_live_status(interaction.guild, is_live)

        message = (
            "🔴 Live channels are now OPEN!"
            if is_live
            else "⚫ Live channels are now HIDDEN! All chat messages deleted."
        )

        await interaction.followup.send(message, ephemeral=True)

    except Exception as error:
        print("Error:", repr(error))
        await interaction.followup.send(
            "Something went wrong. Check the bot permissions and channel IDs.",
            ephemeral=True
        )


_synced = False


@client.event
async def on_ready():
    global _synced
    print(f"Logged in as {client.user}")
    if not _synced:  # on_ready can fire again after a reconnect
        guild = discord.Object(id=GUILD_ID)
        tree.copy_global_to(guild=guild)
        await tree.sync(guild=guild)
        _synced = True
        print("Slash commands synchronized!")
    await refresh_state()


@client.event
async def on_message(message: discord.Message):
    if message.channel.id != CHAT_CHANNEL_ID or message.author == client.user:
        return
    data = serialize(message)
    hub.messages.append(data)
    await hub.broadcast({"type": "message", **data})


@client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    if payload.channel_id != CHAT_CHANNEL_ID:
        return
    mid = str(payload.message_id)
    hub.messages = deque((m for m in hub.messages if m["id"] != mid), maxlen=HISTORY_LIMIT)
    await hub.broadcast({"type": "delete", "id": mid})


@client.event
async def on_voice_state_update(member, before, after):
    ids = {c.id for c in (before.channel, after.channel) if c is not None}
    if STREAM_CHANNEL_ID in ids and before.channel != after.channel:
        await hub.broadcast({"type": "members", "count": member_count()})


# --------------------------------------------------------------------------
# FastAPI app
# --------------------------------------------------------------------------
app = FastAPI(title="Football Live Overlay API", docs_url=None, redoc_url=None)


@app.get("/")
@app.get("/health")
async def health():
    return {"ok": True, "bot_ready": client.is_ready(), "clients": len(hub.clients)}


def _authorized(ws: WebSocket) -> bool:
    header = ws.headers.get("authorization", "")
    supplied = header[7:].strip() if header.lower().startswith("bearer ") else ""
    if not CLIENT_TOKEN or not supplied:
        return False
    return secrets.compare_digest(supplied.encode(), CLIENT_TOKEN.encode())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if not _authorized(ws):
        await ws.close(code=1008)  # rejected before accept -> HTTP 403
        return
    await ws.accept()
    hub.clients.add(ws)
    try:
        await ws.send_json(snapshot())
        while True:
            text = await ws.receive_text()
            if text == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception as error:
        print("WebSocket error:", repr(error))
    finally:
        hub.clients.discard(ws)


async def main():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN environment variable is missing")
    if not CLIENT_TOKEN or len(CLIENT_TOKEN) < 16:
        raise RuntimeError("CLIENT_TOKEN environment variable is missing or too short (use 32+ random chars)")
    discord.utils.setup_logging()
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info"))
    async with client:
        await asyncio.gather(client.start(TOKEN), server.serve())


if __name__ == "__main__":
    asyncio.run(main())
