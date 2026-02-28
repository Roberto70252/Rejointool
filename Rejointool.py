import sys
import subprocess
import os
import time
import ctypes
import threading
import json
import re
import datetime
import atexit
import random
import winreg
import webbrowser
import shutil
import platform
import socket
import hashlib
import uuid
import base64
import queue
import traceback

if sys.platform == 'win32':
    import msvcrt
    os.system('chcp 65001 > nul')
    if not ctypes.windll.shell32.IsUserAnAdmin():
        ctypes.windll.shell32.ShellExecuteW(None, 'runas', sys.executable, ' '.join(sys.argv), None, 1)
        sys.exit(0)

os.system('')
os.environ['PATH'] = os.path.dirname(sys.executable) + os.pathsep + os.environ['PATH']
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ══════════════════════════════════════════════════════════════
#  TOOL IDENTITY
# ══════════════════════════════════════════════════════════════
TOOL_NAME          = 'Pulse Rejoin Tool'
TOOL_VERSION       = '5.0.0'
TOOL_DISCORD       = 'https://discord.gg/gJvFrDGKt2'
TOOL_DISCORD_SHORT = 'discord.gg/gJvFrDGKt2'
TOOL_CREDIT        = 'Pulse'
LOGO_FILE          = 'pulse_logo.png'
BUILD_DATE         = '2025'

REQUIRED_LIBRARIES = {
    'rich':              'rich',
    'psutil':            'psutil',
    'requests':          'requests',
    'win32gui':          'pywin32',
    'selenium':          'selenium',
    'webdriver_manager': 'webdriver-manager',
    'mss':               'mss',
    'cv2':               'opencv-python',
    'numpy':             'numpy',
    'keyboard':          'keyboard',
    'cryptography':      'cryptography',
    'rich_pixels':       'rich-pixels',
    'websocket':         'websocket-client',
}

CHROME_PROFILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chrome_data')

# ══════════════════════════════════════════════════════════════
#  EMBED COLOURS (decimal)
# ══════════════════════════════════════════════════════════════
EMBED_COLORS = {
    'success':  0x00FF96,
    'error':    0xFF3333,
    'warning':  0xFFAA00,
    'info':     0x00BFFF,
    'crash':    0xFF0055,
    'restart':  0x9B59B6,
    'hop':      0x1ABC9C,
    'start':    0x2ECC71,
    'stop':     0xE74C3C,
    'debug':    0x95A5A6,
    'account':  0x3498DB,
    'profile':  0xF39C12,
    'settings': 0x7F8C8D,
    'tool':     0x2C3E50,
    'cmd':      0x5865F2,   # discord blurple — for command responses
    'server':   0x57F287,   # green — server info
}

# ══════════════════════════════════════════════════════════════
#  DISCORD WEBHOOK LOGGER  (user-controlled only — no hardcoded URL)
# ══════════════════════════════════════════════════════════════
class DiscordWebhookLogger:
    """
    Thread-safe, rate-limited Discord webhook logger.
    All sends are queued and processed by a background thread.
    The webhook URL is always provided by the user — never hardcoded.
    Includes Discord command listener (.rejoin, .join, .getid).
    """

    def __init__(self, webhook_url: str = '', enabled: bool = True,
                 command_channel_id: str = '', bot_token: str = ''):
        self.webhook_url        = webhook_url or ''
        self.enabled            = enabled
        self.command_channel_id = command_channel_id
        self.bot_token          = bot_token
        self._queue             = queue.Queue(maxsize=500)
        self._last_send         = 0.0
        self._min_gap           = 1.2
        self._session_id        = uuid.uuid4().hex[:8].upper()
        self._hostname          = platform.node()[:24]
        self._worker            = threading.Thread(target=self._flush_loop, daemon=True, name='WebhookWorker')
        self._worker.start()
        # Command listener state
        self._cmd_callbacks     = {}   # name → callable
        self._cmd_listener      = None
        self._cmd_active        = False
        self._last_msg_id       = None

    # ── Register command callbacks ──────────────────────────

    def register_command(self, name: str, callback):
        """Register a callback for a dot-command (e.g. 'rejoin')."""
        self._cmd_callbacks[name.lower()] = callback

    def start_command_listener(self):
        """Start the always-on bot listener. Requires only bot_token + command_channel_id."""
        if not self.bot_token or not self.command_channel_id:
            return False
        if self._cmd_active and self._cmd_listener and self._cmd_listener.is_alive():
            return True  # already running
        self._cmd_active = True
        self._cmd_listener = threading.Thread(
            target=self._poll_commands, daemon=True, name='CmdListener'
        )
        self._cmd_listener.start()
        return True

    def stop_command_listener(self):
        self._cmd_active = False

    def _poll_commands(self):
        """
        Always-on bot loop using Discord Gateway (WebSocket) for instant response.
        Falls back to REST polling if websocket fails.
        Runs forever as long as bot_token and command_channel_id are set.
        """
        import requests as _req
        import json as _json

        # Try WebSocket gateway first
        try:
            import websocket as _ws
            self._run_gateway(_ws, _req)
        except ImportError:
            pass
        except Exception:
            pass

        # Fallback: REST polling at 1.5s intervals
        headers = {'Authorization': f'Bot {self.bot_token}',
                   'Content-Type': 'application/json'}
        url = f'https://discord.com/api/v10/channels/{self.command_channel_id}/messages?limit=10'
        while self._cmd_active:
            try:
                r = _req.get(url, headers=headers, timeout=8)
                if r.status_code == 200:
                    for msg in reversed(r.json()):
                        mid = msg.get('id')
                        if self._last_msg_id and int(mid) <= int(self._last_msg_id):
                            continue
                        self._last_msg_id = mid
                        content = msg.get('content', '').strip()
                        author  = msg.get('author', {})
                        if author.get('bot'):
                            continue
                        if content.startswith('.'):
                            self._handle_command(content, author, msg)
                elif r.status_code == 401:
                    log_event('Bot token invalid — command listener stopped.', 'ERROR', 'bot')
                    self._cmd_active = False
                    return
            except Exception:
                pass
            time.sleep(1.5)

    def _run_gateway(self, _ws, _req):
        """Discord Gateway WebSocket for real-time message events."""
        import json as _json
        import threading as _threading

        gw = _req.get('https://discord.com/api/v10/gateway', timeout=5).json().get('url', 'wss://gateway.discord.gg')
        gw += '/?v=10&encoding=json'
        _heartbeat_interval = [41250]
        _hb_thread          = [None]
        _seq                = [None]

        def send_heartbeat(ws):
            while self._cmd_active:
                time.sleep(_heartbeat_interval[0] / 1000)
                try:
                    ws.send(_json.dumps({'op': 1, 'd': _seq[0]}))
                except:
                    break

        def on_message(ws, raw):
            try:
                data = _json.loads(raw)
                op   = data.get('op')
                if data.get('s'):
                    _seq[0] = data['s']

                if op == 10:  # Hello
                    _heartbeat_interval[0] = data['d']['heartbeat_interval']
                    _hb_thread[0] = _threading.Thread(target=send_heartbeat, args=(ws,), daemon=True)
                    _hb_thread[0].start()
                    # Identify
                    ws.send(_json.dumps({
                        'op': 2,
                        'd': {
                            'token':      self.bot_token,
                            'intents':    512,  # GUILD_MESSAGES
                            'properties': {'os': 'windows', 'browser': 'pulse', 'device': 'pulse'},
                        }
                    }))

                elif op == 0:  # Dispatch
                    t = data.get('t')
                    d = data.get('d', {})
                    if t == 'MESSAGE_CREATE':
                        chan = d.get('channel_id', '')
                        if chan != str(self.command_channel_id):
                            return
                        author  = d.get('author', {})
                        if author.get('bot'):
                            return
                        content = d.get('content', '').strip()
                        mid     = d.get('id')
                        if mid:
                            self._last_msg_id = mid
                        if content.startswith('.'):
                            _threading.Thread(
                                target=self._handle_command,
                                args=(content, author, d),
                                daemon=True
                            ).start()

                elif op == 7:  # Reconnect
                    ws.close()
                elif op == 9:  # Invalid session
                    time.sleep(5)
                    ws.close()

            except Exception:
                pass

        def on_error(ws, err):
            pass

        def on_close(ws, *a):
            pass

        while self._cmd_active:
            try:
                sock = _ws.WebSocketApp(
                    gw,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                sock.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                pass
            if self._cmd_active:
                time.sleep(5)  # reconnect delay

    def _handle_command(self, content: str, author: dict, msg: dict):
        """Parse and dispatch a dot-command."""
        parts   = content[1:].split()
        if not parts:
            return
        cmd  = parts[0].lower()
        args = parts[1:]
        username = author.get('username', 'Unknown')

        if cmd in self._cmd_callbacks:
            try:
                result = self._cmd_callbacks[cmd](args, username)
                self._send_command_response(cmd, result, username)
            except Exception as e:
                self._send_command_response(cmd, f'Error: {e}', username)
        else:
            self._send_command_response(cmd,
                f'Unknown command. Available: {", ".join("."+c for c in self._cmd_callbacks)}',
                username)

    def _send_command_response(self, cmd: str, result: str, invoker: str):
        """Send a command result back to the webhook channel."""
        self._enqueue(self._build_embed(
            title       = f'⌨️  Command: .{cmd}',
            description = str(result),
            color       = EMBED_COLORS['cmd'],
            fields      = [
                {'name': '👤 Invoked by', 'value': invoker, 'inline': True},
                {'name': '🕐 Time',       'value': datetime.datetime.now().strftime('%H:%M:%S'), 'inline': True},
            ],
            event_type  = 'COMMAND',
        ))

    # ── Session events ──────────────────────────────────────

    def tool_started(self):
        self._enqueue(self._build_embed(
            title       = '🚀  Tool Started',
            description = f'**{TOOL_NAME}** has been launched.',
            color       = EMBED_COLORS['tool'],
            fields      = [
                {'name': '🔖 Version',  'value': TOOL_VERSION,                                        'inline': True},
                {'name': '🖥️ Machine',  'value': f'`{self._hostname}`',                               'inline': True},
                {'name': '🔑 Session',  'value': self._session_id,                                    'inline': True},
                {'name': '🕐 Time',     'value': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'inline': True},
                {'name': '👤 OS User',  'value': os.getenv('USERNAME', '?'),                          'inline': True},
                {'name': '💻 OS',       'value': f'{platform.system()} {platform.release()}',         'inline': True},
            ],
            event_type  = 'TOOL_START',
        ))

    def tool_closed(self):
        self._enqueue(self._build_embed(
            title       = '🔒  Tool Closed',
            description = f'**{TOOL_NAME}** was closed.',
            color       = EMBED_COLORS['tool'],
            fields      = [
                {'name': '🔑 Session', 'value': self._session_id, 'inline': True},
                {'name': '🕐 Time',    'value': datetime.datetime.now().strftime('%H:%M:%S'), 'inline': True},
            ],
            event_type  = 'TOOL_CLOSE',
        ))

    def session_start(self, instances: int, game_info: str = ''):
        self._enqueue(self._build_embed(
            title       = '🟢  Monitoring Session Started',
            description = f'Now monitoring **{instances}** instance(s).',
            color       = EMBED_COLORS['start'],
            fields      = [
                {'name': '🎮 Game(s)',   'value': game_info or '—',      'inline': True},
                {'name': '📦 Instances', 'value': str(instances),        'inline': True},
                {'name': '🖥️ Machine',   'value': f'`{self._hostname}`', 'inline': True},
            ],
            event_type  = 'SESSION_START',
        ))

    def session_stop(self, uptime_secs: float, crashes: int, restarts: int):
        self._enqueue(self._build_embed(
            title       = '🔴  Monitoring Session Ended',
            description = 'The monitoring session has been stopped.',
            color       = EMBED_COLORS['stop'],
            fields      = [
                {'name': '⏱️ Uptime',   'value': self._fmt_time(uptime_secs), 'inline': True},
                {'name': '💥 Crashes',  'value': str(crashes),               'inline': True},
                {'name': '🔄 Restarts', 'value': str(restarts),              'inline': True},
            ],
            event_type  = 'SESSION_STOP',
        ))

    # ── Server / game info events ────────────────────────────

    def server_info(self, account: str, place_id, server_id: str, user_id,
                    players: int, max_players: int, game_name: str = ''):
        """Rich embed with full server + user info."""
        self._enqueue(self._build_embed(
            title       = '🌐  Server Info',
            description = f'Live server details for **{account}**.',
            color       = EMBED_COLORS['server'],
            fields      = [
                {'name': '👤 Account',    'value': account,                                                      'inline': True},
                {'name': '🆔 User ID',    'value': str(user_id or '—'),                                         'inline': True},
                {'name': '🎮 Game',       'value': game_name or str(place_id),                                  'inline': True},
                {'name': '🗺️ Place ID',   'value': str(place_id),                                               'inline': True},
                {'name': '🖧 Server ID',  'value': f'`{server_id}`' if server_id else '—',                      'inline': False},
                {'name': '👥 Players',    'value': f'{players}/{max_players}',                                   'inline': True},
                {'name': '🔗 Profile',    'value': f'https://www.roblox.com/users/{user_id}/profile' if user_id else '—', 'inline': False},
                {'name': '🎯 Game Link',  'value': f'https://www.roblox.com/games/{place_id}' if place_id else '—',       'inline': False},
            ],
            event_type  = 'SERVER_INFO',
        ))

    def log_message(self, level: str, instance_name: str, message: str):
        """Forward a log line to webhook."""
        color_map = {
            'SUCCESS': EMBED_COLORS['success'],
            'ERROR':   EMBED_COLORS['error'],
            'WARN':    EMBED_COLORS['warning'],
            'INFO':    EMBED_COLORS['info'],
            'DEBUG':   EMBED_COLORS['debug'],
        }
        self._enqueue(self._build_embed(
            title       = f'📋  Log — {level}',
            description = message,
            color       = color_map.get(level, EMBED_COLORS['info']),
            fields      = [
                {'name': '📛 Instance', 'value': instance_name, 'inline': True},
                {'name': '🕐 Time',     'value': datetime.datetime.now().strftime('%H:%M:%S'), 'inline': True},
            ],
            event_type  = f'LOG_{level}',
        ))

    # ── Account events ──────────────────────────────────────

    def account_added(self, username: str, user_id, total_accounts: int):
        self._enqueue(self._build_embed(
            title       = '👤  Account Added',
            description = f'Roblox account **{username}** added to vault.',
            color       = EMBED_COLORS['account'],
            fields      = [
                {'name': '👤 Username',       'value': username,                                                        'inline': True},
                {'name': '🆔 User ID',        'value': str(user_id),                                                   'inline': True},
                {'name': '🔢 Total Accounts', 'value': str(total_accounts),                                            'inline': True},
                {'name': '🔗 Profile',        'value': f'https://www.roblox.com/users/{user_id}/profile',              'inline': False},
            ],
            event_type  = 'ACCOUNT_ADDED',
        ))

    def account_removed(self, username: str, total_accounts: int):
        self._enqueue(self._build_embed(
            title       = '🗑️  Account Removed',
            description = f'**{username}** removed from vault.',
            color       = EMBED_COLORS['warning'],
            fields      = [
                {'name': '👤 Username',       'value': username,           'inline': True},
                {'name': '🔢 Total Accounts', 'value': str(total_accounts),'inline': True},
            ],
            event_type  = 'ACCOUNT_REMOVED',
        ))

    def account_validated(self, username: str, user_id, valid: bool):
        self._enqueue(self._build_embed(
            title       = f'{"✅" if valid else "❌"}  Account Validated',
            description = f'**{username}** — cookie is **{"valid" if valid else "INVALID / EXPIRED"}**.',
            color       = EMBED_COLORS['success'] if valid else EMBED_COLORS['error'],
            fields      = [
                {'name': '👤 Username', 'value': username,                            'inline': True},
                {'name': '🆔 User ID',  'value': str(user_id or '?'),                'inline': True},
                {'name': '🔑 Status',   'value': 'Valid ✓' if valid else 'Expired ✗','inline': True},
            ],
            event_type  = 'ACCOUNT_VALIDATED',
        ))

    def cookie_added_batch(self, added: list, failed: int):
        names_str = '\n'.join(f'• **{u}** (ID: {i})' for u, i in added) or '—'
        self._enqueue(self._build_embed(
            title       = '🍪  Cookies Pasted',
            description = f'Batch add: **{len(added)}** succeeded, **{failed}** failed.',
            color       = EMBED_COLORS['account'],
            fields      = [
                {'name': '✅ Added',   'value': names_str[:1000], 'inline': False},
                {'name': '❌ Failed', 'value': str(failed),       'inline': True},
                {'name': '📦 Total',  'value': str(len(added)),   'inline': True},
            ],
            event_type  = 'COOKIE_BATCH',
        ))

    # ── Profile events ──────────────────────────────────────

    def profile_created(self, name: str, mode: str, place_id, accounts: list):
        self._enqueue(self._build_embed(
            title       = '📋  Profile Created',
            description = f'New profile **{name}** created.',
            color       = EMBED_COLORS['profile'],
            fields      = [
                {'name': '📛 Name',    'value': name,                      'inline': True},
                {'name': '🖥️ Mode',    'value': mode.capitalize(),          'inline': True},
                {'name': '🎮 PlaceID', 'value': str(place_id),              'inline': True},
                {'name': '👥 Accounts','value': ', '.join(accounts) or '—', 'inline': False},
            ],
            event_type  = 'PROFILE_CREATED',
        ))

    def profile_launched(self, name: str, mode: str, instance_count: int, place_id=None):
        self._enqueue(self._build_embed(
            title       = '▶️  Profile Launched',
            description = f'Profile **{name}** launched.',
            color       = EMBED_COLORS['start'],
            fields      = [
                {'name': '📛 Profile',  'value': name,                'inline': True},
                {'name': '🖥️ Mode',     'value': mode.capitalize(),   'inline': True},
                {'name': '📦 Instances','value': str(instance_count), 'inline': True},
                {'name': '🎮 Place ID', 'value': str(place_id or '—'),'inline': True},
            ],
            event_type  = 'PROFILE_LAUNCHED',
        ))

    def profile_deleted(self, name: str):
        self._enqueue(self._build_embed(
            title       = '🗑️  Profile Deleted',
            description = f'Profile **{name}** deleted.',
            color       = EMBED_COLORS['warning'],
            fields      = [{'name': '📛 Profile', 'value': name, 'inline': True}],
            event_type  = 'PROFILE_DELETED',
        ))

    # ── Monitoring events ────────────────────────────────────

    def crash_detected(self, instance_name: str, crash_count: int, game: str = ''):
        self._enqueue(self._build_embed(
            title       = '💥  Crash Detected',
            description = f'**{instance_name}** crashed — relaunching.',
            color       = EMBED_COLORS['crash'],
            fields      = [
                {'name': '📛 Instance',      'value': instance_name,    'inline': True},
                {'name': '🎮 Game',          'value': game or '—',      'inline': True},
                {'name': '🔢 Total Crashes', 'value': str(crash_count), 'inline': True},
            ],
            event_type  = 'CRASH',
            urgent      = True,
        ))

    def relaunch_success(self, instance_name: str, attempt: int):
        self._enqueue(self._build_embed(
            title       = '✅  Relaunch Successful',
            description = f'**{instance_name}** is back online.',
            color       = EMBED_COLORS['success'],
            fields      = [
                {'name': '📛 Instance', 'value': instance_name, 'inline': True},
                {'name': '🔁 Attempt',  'value': str(attempt),  'inline': True},
            ],
            event_type  = 'RELAUNCH',
        ))

    def scheduled_restart(self, instance_name: str, restart_count: int):
        self._enqueue(self._build_embed(
            title       = '🔄  Scheduled Restart',
            description = f'**{instance_name}** restarting on schedule.',
            color       = EMBED_COLORS['restart'],
            fields      = [
                {'name': '📛 Instance',  'value': instance_name,      'inline': True},
                {'name': '🔢 Restart #', 'value': str(restart_count), 'inline': True},
            ],
            event_type  = 'SCHEDULED_RESTART',
        ))

    def freeze_detected(self, instance_name: str, similarity: float):
        self._enqueue(self._build_embed(
            title       = '🧊  Freeze Detected',
            description = f'**{instance_name}** frozen — relaunching.',
            color       = EMBED_COLORS['warning'],
            fields      = [
                {'name': '📛 Instance',   'value': instance_name,       'inline': True},
                {'name': '📊 Similarity', 'value': f'{similarity:.1%}', 'inline': True},
            ],
            event_type  = 'FREEZE',
        ))

    def server_hop(self, account: str, hop_number: int, players: int, max_players: int,
                   server_id: str = ''):
        self._enqueue(self._build_embed(
            title       = '🔀  Server Hop',
            description = f'**{account}** hopped to a new server.',
            color       = EMBED_COLORS['hop'],
            fields      = [
                {'name': '👤 Account',   'value': account,                    'inline': True},
                {'name': '🔢 Hop #',     'value': str(hop_number),            'inline': True},
                {'name': '👥 Players',   'value': f'{players}/{max_players}', 'inline': True},
                {'name': '🖧 Server ID', 'value': f'`{server_id}`' if server_id else '—', 'inline': False},
            ],
            event_type  = 'SERVER_HOP',
        ))

    def crash_limit_reached(self, instance_name: str, limit: int):
        self._enqueue(self._build_embed(
            title       = '🛑  Crash Limit Reached',
            description = f'**{instance_name}** hit crash limit — stopped.',
            color       = EMBED_COLORS['error'],
            fields      = [
                {'name': '📛 Instance', 'value': instance_name, 'inline': True},
                {'name': '🔢 Limit',    'value': str(limit),    'inline': True},
            ],
            event_type  = 'CRASH_LIMIT',
            urgent      = True,
        ))

    def settings_changed(self, setting_name: str, old_val, new_val):
        self._enqueue(self._build_embed(
            title       = '⚙️  Setting Changed',
            description = 'A tool setting was modified.',
            color       = EMBED_COLORS['settings'],
            fields      = [
                {'name': '🔧 Setting',   'value': setting_name, 'inline': True},
                {'name': '📤 Old Value', 'value': str(old_val), 'inline': True},
                {'name': '📥 New Value', 'value': str(new_val), 'inline': True},
            ],
            event_type  = 'SETTINGS_CHANGED',
        ))

    def roblox_killed(self, count: int):
        self._enqueue(self._build_embed(
            title       = '🧹  Roblox Processes Killed',
            description = f'**{count}** Roblox instance(s) force-killed.',
            color       = EMBED_COLORS['warning'],
            fields      = [{'name': '🔢 Killed', 'value': str(count), 'inline': True}],
            event_type  = 'ROBLOX_KILLED',
        ))

    def generic(self, title: str, description: str, level: str = 'info', fields: list = None):
        self._enqueue(self._build_embed(
            title       = title,
            description = description,
            color       = EMBED_COLORS.get(level, EMBED_COLORS['info']),
            fields      = fields or [],
            event_type  = level.upper(),
        ))

    def test_connection(self) -> tuple:
        try:
            embed = self._build_embed(
                title       = '🔔  Webhook Test',
                description = f'{TOOL_NAME} webhook is connected and working!',
                color       = EMBED_COLORS['success'],
                fields      = [
                    {'name': '🖥️ Machine', 'value': f'`{self._hostname}`', 'inline': True},
                    {'name': '🔖 Version', 'value': TOOL_VERSION,          'inline': True},
                    {'name': '🔑 Session', 'value': self._session_id,      'inline': True},
                    {'name': '📋 Commands Available',
                     'value': '`.rejoin` `.join <serverID/placeID>` `.getid`',
                     'inline': False},
                ],
                event_type  = 'TEST',
            )
            self._send_now(embed)
            return True, 'Webhook test sent!'
        except Exception as e:
            return False, f'Webhook error: {e}'

    # ── Internal helpers ────────────────────────────────────

    def _build_embed(self, title, description, color, fields, event_type, urgent=False):
        now   = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        embed = {
            'title':       title,
            'description': description,
            'color':       color,
            'fields':      fields,
            'footer': {
                'text': f'{TOOL_NAME} v{TOOL_VERSION}  •  Session {self._session_id}  •  {now}'
            },
            'timestamp': datetime.datetime.utcnow().isoformat(),
        }
        payload = {
            'username':   TOOL_NAME,
            'avatar_url': 'https://cdn.discordapp.com/embed/avatars/0.png',
            'embeds':     [embed],
        }
        if urgent:
            payload['content'] = '||@here||'
        return payload

    def _enqueue(self, payload):
        if not self.enabled or not self.webhook_url:
            return
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            pass

    def _send_now(self, payload):
        import requests as _req
        _req.post(self.webhook_url, json=payload, timeout=8)

    def _flush_loop(self):
        import requests as _req
        while True:
            try:
                payload = self._queue.get(timeout=5)
            except queue.Empty:
                continue
            if not self.enabled or not self.webhook_url:
                continue
            gap = time.time() - self._last_send
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            try:
                resp = _req.post(self.webhook_url, json=payload, timeout=8)
                if resp.status_code == 429:
                    retry = resp.json().get('retry_after', 2)
                    time.sleep(float(retry) + 0.5)
                    _req.post(self.webhook_url, json=payload, timeout=8)
            except Exception:
                pass
            self._last_send = time.time()

    @staticmethod
    def _fmt_time(secs: float) -> str:
        h, r = divmod(int(secs), 3600); m, s = divmod(r, 60)
        if h: return f'{h}h {m}m {s}s'
        if m: return f'{m}m {s}s'
        return f'{s}s'


# Global webhook instance
webhook: DiscordWebhookLogger = None  # type: ignore

def init_webhook(cfg: dict):
    global webhook
    ws      = cfg.get('settings', {}).get('webhook', {})
    url     = ws.get('url', '')
    enabled = ws.get('enabled', False)
    bot_tok = ws.get('bot_token', '')
    chan_id  = ws.get('command_channel_id', '')

    # Stop existing bot before recreating
    if webhook is not None:
        try: webhook.stop_command_listener()
        except: pass

    webhook = DiscordWebhookLogger(
        webhook_url        = url,
        enabled            = enabled,
        command_channel_id = chan_id,
        bot_token          = bot_tok,
    )

    # Auto-start bot immediately if token + channel set — no session needed
    if bot_tok and chan_id:
        webhook.start_command_listener()


# ══════════════════════════════════════════════════════════════
#  TITAN SPOOFER INTEGRATION
# ══════════════════════════════════════════════════════════════
if getattr(sys, 'frozen', False):
    _TITAN_BASE = os.path.dirname(sys.executable)
else:
    _TITAN_BASE = os.path.dirname(os.path.abspath(__file__))

TITAN_EXE  = os.path.join(_TITAN_BASE, 'Titan.exe')
TITAN_DLL  = os.path.join(_TITAN_BASE, 'TITAN.dll')
TITAN_PROC = None

def titan_is_available():  return os.path.exists(TITAN_EXE)
def titan_is_running():
    global TITAN_PROC
    if TITAN_PROC is None: return False
    if TITAN_PROC.poll() is not None: TITAN_PROC = None; return False
    return True

def titan_launch():
    global TITAN_PROC
    if titan_is_running():       return True,  'Titan already running.'
    if not titan_is_available(): return False, f'Titan.exe not found: {TITAN_EXE}'
    try:
        TITAN_PROC = subprocess.Popen([TITAN_EXE], cwd=_TITAN_BASE,
                                      creationflags=subprocess.CREATE_NO_WINDOW)
        time.sleep(1.5)
        if titan_is_running(): return True, f'Titan launched (PID {TITAN_PROC.pid})'
        return False, 'Titan exited immediately.'
    except Exception as e:
        return False, f'Failed: {e}'

def titan_stop():
    global TITAN_PROC
    if not titan_is_running(): return False, 'Titan is not running.'
    try:
        TITAN_PROC.terminate(); TITAN_PROC.wait(timeout=5); TITAN_PROC = None
        return True, 'Titan stopped.'
    except Exception as e:
        try: TITAN_PROC.kill(); TITAN_PROC = None
        except: pass
        return True, f'Titan force-killed. ({e})'


# ══════════════════════════════════════════════════════════════
#  BOOTSTRAP / LIBRARY CHECK
# ══════════════════════════════════════════════════════════════
def clear_screen():
    subprocess.run('cls' if sys.platform == 'win32' else 'clear', shell=True)

def print_header(title):
    print('═' * 64)
    print(f'║  {title:^58}  ║')
    print('═' * 64)
    print()

def check_and_install_libraries(libs):
    print_header(f'Dependency Check  —  {TOOL_NAME} v{TOOL_VERSION}')
    missing = []
    for imp, pkg in libs.items():
        try:   __import__(imp); print(f'  ✓  {pkg}')
        except ImportError:    print(f'  ✗  {pkg}  ← MISSING'); missing.append(pkg) if pkg not in missing else None
    print('─' * 64)
    if not missing: return True
    print_header('Installing Missing Libraries')
    for pkg in missing:
        print(f'  → Installing {pkg} ...')
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', pkg],
                           check=True, capture_output=True, text=True)
            print(f'  ✓  Installed {pkg}')
        except subprocess.CalledProcessError as e:
            print(f'  ✗  Failed: {e.stderr[:200]}'); return False
    print('\n  All libraries installed. Please restart the tool.')
    time.sleep(8); sys.exit(0)


# ══════════════════════════════════════════════════════════════
#  HEAVY IMPORTS
# ══════════════════════════════════════════════════════════════
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from webdriver_manager.chrome import ChromeDriverManager
    import requests
    import win32event, win32api, win32gui, win32con, win32process
    import win32com.client
    from win32com.shell import shell, shellcon
    import psutil
    import cv2
    import numpy as np
    from mss import mss
    import keyboard
    from cryptography.fernet import Fernet
    from rich_pixels import Pixels
except ImportError as e:
    print(f'Critical import error: {e}')
    input('Press Enter to exit.')
    sys.exit(1)

def _chk(mod, name):
    try:    __import__(mod); return True
    except: return False

DESKTOP_MODE_ENABLED = _chk('psutil',   'psutil')
SHARE_LINK_ENABLED   = _chk('requests', 'requests')
WINDOWS_INTEGRATION  = _chk('win32gui', 'win32gui')

try:
    from rich.console import Console
    from rich.table   import Table
    from rich.panel   import Panel
    from rich.align   import Align
    from rich.prompt  import Prompt
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.text    import Text
    from rich.rule    import Rule
    from rich.live    import Live
    from rich.layout  import Layout
    from rich.columns import Columns
    from rich         import box
    rich_console = Console()
    RICH_ENABLED = True
except ImportError:
    rich_console = None
    RICH_ENABLED = False


# ══════════════════════════════════════════════════════════════
#  CONSTANTS & STATE
# ══════════════════════════════════════════════════════════════
ROBLOX_PROCESS_NAME  = 'RobloxPlayerBeta.exe'
ROBLOX_PACKAGE_NAME  = 'com.roblox.client'
ROBLOX_ACTIVITY_NAME = 'com.roblox.client.ActivityProtocolLaunch'
SINGLETON_MUTEX_NAME = 'PULSE_REJOINTOOL_V5_MUTEX'
UI_WIDTH             = 104

SESSION_STATUS         = {}
# Stores current server IDs per session key → {'server_id': ..., 'place_id': ..., ...}
SESSION_SERVER_INFO    = {}
STATUS_LOCK            = threading.RLock()
MONITORING_ACTIVE      = True
TOGGLE_REQUESTED       = False
EXIT_TO_MENU_REQUESTED = False
DEBUG_MODE_ACTIVE      = False
DEBUG_LOG              = []
mutex_handle           = None


# ══════════════════════════════════════════════════════════════
#  GAMES DATABASE
# ══════════════════════════════════════════════════════════════
GAMES_DB = {
    'Jailbreak':                     606849621,
    'Da Hood':                       2788229376,
    'Blade Ball':                    13772394625,
    'Arsenal':                       286090429,
    'Murder Mystery 2':              142823291,
    'Criminality':                   3696840781,
    'Bad Business':                  3272915504,
    'Phantom Forces':                292439477,
    'Counter Blox':                  301549746,
    'BedWars':                       6872265039,
    'Frontlines':                    6872265040,
    'Entry Point':                   1064459085,
    'Blox Fruits':                   2753915549,
    'Pet Simulator 99':              8737899170,
    'Pet Simulator X':               6284583030,
    'Anime Fighting Sim':            706722714,
    'Anime Dimensions':              6837256862,
    'Slayers Unleashed':             5217916141,
    'AUT (A Universal Time)':        7286536498,
    'King Legacy':                   5961350021,
    'Fruit Battlegrounds':           11252756347,
    'Grand Piece Online':            2753915550,
    'Brookhaven':                    4924922222,
    'Adopt Me!':                     920587237,
    'Welcome to Bloxburg':           185655149,
    'Royale High':                   735030788,
    'Livetopia':                     4872321990,
    'Tower of Hell':                 1962086868,
    'Obby Creator':                  1359583088,
    'Mega Easy Obby':                1537690962,
    'Speed Run 4':                   116690330,
    'DIG':                           126244816328678,
    'Mining Simulator 2':            8131807688,
    'Anime Catch!':                  6490671380,
    'Clicker Simulator':             4483666998,
    'Islands':                       4872321991,
    'Lumberjack Simulator':          4872321992,
    'The Strongest Battlegrounds':   17017769292,
    'Fisch':                         16732694052,
    'Grow a Garden':                 126244816000000,
    'Type Soul':                     14900000000,
    'Untitled Boxing Game':          14579547839,
    'Combat Warriors':               6471489973,
    'Rivals':                        17625359962,
    'Shindo Life':                   1944198494,
}

GAME_CATEGORIES = {
    'Action / PvP':      ['Jailbreak','Da Hood','Blade Ball','Arsenal','Murder Mystery 2',
                           'Criminality','Bad Business','Phantom Forces','Counter Blox',
                           'BedWars','Frontlines','Entry Point','The Strongest Battlegrounds',
                           'Combat Warriors','Rivals','Untitled Boxing Game'],
    'Farming / Grind':   ['Blox Fruits','Pet Simulator 99','Pet Simulator X','Anime Fighting Sim',
                           'Anime Dimensions','Slayers Unleashed','AUT (A Universal Time)',
                           'King Legacy','Fruit Battlegrounds','Grand Piece Online','Type Soul',
                           'Shindo Life','Fisch','Grow a Garden'],
    'Roleplay / Social': ['Brookhaven','Adopt Me!','Welcome to Bloxburg','Royale High','Livetopia'],
    'Casual / Obby':     ['Tower of Hell','Obby Creator','Mega Easy Obby','Speed Run 4'],
    'AFK / Farm':        ['DIG','Mining Simulator 2','Anime Catch!','Clicker Simulator',
                           'Islands','Lumberjack Simulator'],
}


# ══════════════════════════════════════════════════════════════
#  COLOURS / THEMES
# ══════════════════════════════════════════════════════════════
class C:
    RESET      = '\x1b[0m'
    BOLD       = '\x1b[1m'
    DIM        = '\x1b[2m'
    BLUE       = '\x1b[38;5;111m'
    CYAN       = '\x1b[38;5;51m'
    MAGENTA    = '\x1b[38;5;165m'
    ORANGE     = '\x1b[38;5;208m'
    GREEN      = '\x1b[38;5;82m'
    RED        = '\x1b[38;5;196m'
    BRIGHT_RED = '\x1b[38;5;196m'
    GREY       = '\x1b[38;5;244m'
    WHITE      = '\x1b[38;5;252m'
    YELLOW     = '\x1b[38;5;226m'
    PULSE      = '\x1b[38;5;39m'
    TEAL       = '\x1b[38;5;6m'
    PINK       = '\x1b[38;5;213m'

COLOR_MAP = {
    'Pulse Blue': '\x1b[38;5;39m',
    'Cyan':       '\x1b[38;5;51m',
    'Magenta':    '\x1b[38;5;165m',
    'Orange':     '\x1b[38;5;208m',
    'Green':      '\x1b[38;5;82m',
    'White':      '\x1b[38;5;252m',
    'Red':        '\x1b[38;5;196m',
}

RICH_THEMES = {
    'Pulse':     {'primary':'bright_blue',   'secondary':'bright_cyan',  'success':'bright_green','error':'red',        'warning':'yellow','info':'bright_blue','dim':'dim white',   'box_style':box.ROUNDED},
    'Cyberpunk': {'primary':'bright_cyan',   'secondary':'magenta',      'success':'bright_green','error':'bright_red', 'warning':'yellow','info':'blue',       'dim':'dim cyan',    'box_style':box.HEAVY},
    'Matrix':    {'primary':'bright_green',  'secondary':'green',        'success':'bright_green','error':'red',        'warning':'yellow','info':'cyan',       'dim':'dim green',   'box_style':box.DOUBLE},
    'Neon':      {'primary':'bright_magenta','secondary':'bright_cyan',  'success':'bright_green','error':'bright_red', 'warning':'yellow','info':'blue',       'dim':'magenta',     'box_style':box.DOUBLE_EDGE},
    'Dark':      {'primary':'white',         'secondary':'bright_white', 'success':'green',       'error':'red',        'warning':'yellow','info':'blue',       'dim':'dim white',   'box_style':box.SIMPLE},
    'Blood':     {'primary':'bright_red',    'secondary':'red',          'success':'bright_green','error':'bright_red', 'warning':'orange1','info':'red',       'dim':'dim red',     'box_style':box.HEAVY},
    'Ocean':     {'primary':'cyan',          'secondary':'blue',         'success':'bright_green','error':'red',        'warning':'yellow','info':'bright_cyan','dim':'dim cyan',    'box_style':box.ROUNDED},
    'Sunset':    {'primary':'orange1',       'secondary':'yellow',       'success':'bright_green','error':'red',        'warning':'orange1','info':'yellow',    'dim':'dim yellow',  'box_style':box.ROUNDED},
    'Stealth':   {'primary':'grey70',        'secondary':'grey82',       'success':'green',       'error':'red',        'warning':'yellow','info':'grey74',     'dim':'dim grey50',  'box_style':box.MINIMAL},
    'Aurora':    {'primary':'bright_green',  'secondary':'bright_cyan',  'success':'bright_green','error':'red',        'warning':'yellow','info':'cyan',       'dim':'dim green',   'box_style':box.ROUNDED},
    'Crimson':   {'primary':'red',           'secondary':'bright_red',   'success':'bright_green','error':'bright_red', 'warning':'yellow','info':'red',        'dim':'dim red',     'box_style':box.HEAVY_EDGE},
}

current_theme = RICH_THEMES['Pulse']
(H_TL, H_T, H_TR, H_S, H_BL, H_B, H_BR) = ('╭','─','╮','│','╰','─','╯')


# ══════════════════════════════════════════════════════════════
#  FILE PATHS
# ══════════════════════════════════════════════════════════════
if getattr(sys, 'frozen', False):
    base_path = os.path.dirname(sys.executable)
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

CONFIG_FILE       = os.path.join(base_path, 'pulse_config.json')
CRITICAL_LOG_FILE = os.path.join(base_path, 'pulse_critical.log')
SESSION_LOG_FILE  = os.path.join(base_path, 'pulse_sessions.log')
NOTES_FILE        = os.path.join(base_path, 'pulse_notes.txt')
BACKUP_DIR        = os.path.join(base_path, 'backups')
STATS_FILE        = os.path.join(base_path, 'pulse_stats.json')


# ══════════════════════════════════════════════════════════════
#  MUTEX
# ══════════════════════════════════════════════════════════════
def acquire_mutex():
    global mutex_handle
    try:
        mutex_handle = win32event.CreateMutex(None, True, SINGLETON_MUTEX_NAME)
        if win32api.GetLastError() == 183:
            print(f'[ERROR] {TOOL_NAME} is already running.')
            win32api.CloseHandle(mutex_handle); mutex_handle = None
            return False
        return True
    except Exception as e:
        print(f'Mutex error: {e}'); return False

def release_mutex():
    if mutex_handle:
        try: win32event.ReleaseMutex(mutex_handle); win32api.CloseHandle(mutex_handle)
        except: pass

atexit.register(release_mutex)


# ══════════════════════════════════════════════════════════════
#  STATS TRACKING
# ══════════════════════════════════════════════════════════════
def load_stats():
    if not os.path.exists(STATS_FILE):
        return {'total_sessions':0,'total_crashes':0,'total_restarts':0,
                'total_hops':0,'uptime_seconds':0,'sessions_by_game':{},
                'first_use':datetime.datetime.now().isoformat(),'last_use':''}
    try:
        with open(STATS_FILE,'r') as f: return json.load(f)
    except: return {}

def save_stats(s):
    try:
        with open(STATS_FILE,'w') as f: json.dump(s, f, indent=2)
    except: pass

def increment_stat(key, amount=1):
    s = load_stats()
    s[key] = s.get(key, 0) + amount
    s['last_use'] = datetime.datetime.now().isoformat()
    save_stats(s)


# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════
def write_log(message, level='INFO'):
    try:
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(SESSION_LOG_FILE,'a',encoding='utf-8') as f:
            f.write(f'[{ts}] [{level:7}] {message}\n')
    except: pass

def log_event(message, level='INFO', session_key='system'):
    global DEBUG_LOG
    ts    = datetime.datetime.now().strftime('%H:%M:%S')
    entry = f'[{ts}] [{level:7}] [{session_key}] {message}'
    DEBUG_LOG.append(entry)
    if len(DEBUG_LOG) > 600: DEBUG_LOG = DEBUG_LOG[-500:]
    write_log(f'[{session_key}] {message}', level)
    # Forward important events to webhook
    if webhook and webhook.enabled and webhook.webhook_url:
        cfg = load_config()
        if _webhook_event_enabled(cfg, 'log_forward') and level in ('ERROR','WARN','SUCCESS'):
            inst = SESSION_STATUS.get(session_key, {}).get('instance_name', session_key)
            webhook.log_message(level, inst, message)
    colors = {'INFO':C.CYAN,'SUCCESS':C.GREEN,'ERROR':C.RED,'WARN':C.ORANGE,'DEBUG':C.MAGENTA}
    with STATUS_LOCK:
        if session_key in SESSION_STATUS:
            disp = colorize(message, colors.get(level, C.GREY))
            if level == 'SUCCESS':
                disp = colorize('● Running', C.GREEN)
            elif level == 'ERROR':
                increment_stat('total_crashes')
            SESSION_STATUS[session_key]['status'] = disp


# ══════════════════════════════════════════════════════════════
#  UI HELPERS
# ══════════════════════════════════════════════════════════════
def get_visible_len(s):
    return len(re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]','',s))

def colorize(text, color): return f'{color}{text}{C.RESET}'

def print_centered(line=''):
    has_ansi = bool(re.search(r'\x1B\[[0-?]*[ -/]*[@-~]', line))
    if RICH_ENABLED and not has_ansi:
        rich_console.print(Align.center(line)); return
    try:    w = os.get_terminal_size().columns
    except: w = UI_WIDTH
    pad = ' ' * max(0, (w - get_visible_len(line)) // 2)
    print(pad + line)

def show_ok(msg):
    if RICH_ENABLED: rich_console.print(f"  [{current_theme['success']}]✓  {msg}[/{current_theme['success']}]")
    else: print_centered(colorize(f'[+] {msg}', C.GREEN))

def show_err(msg):
    if RICH_ENABLED: rich_console.print(f"  [{current_theme['error']}]✗  {msg}[/{current_theme['error']}]")
    else: print_centered(colorize(f'[X] {msg}', C.RED))

def show_warn(msg):
    if RICH_ENABLED: rich_console.print(f"  [{current_theme['warning']}]⚠  {msg}[/{current_theme['warning']}]")
    else: print_centered(colorize(f'[!] {msg}', C.ORANGE))

def show_info(msg):
    if RICH_ENABLED: rich_console.print(f"  [{current_theme['info']}]ℹ  {msg}[/{current_theme['info']}]")
    else: print_centered(colorize(f'[i] {msg}', C.BLUE))

def show_cmd(msg):
    if RICH_ENABLED: rich_console.print(f"  [bold bright_magenta]⌨  {msg}[/bold bright_magenta]")
    else: print_centered(colorize(f'[CMD] {msg}', C.MAGENTA))

def pause(msg='Press Enter to continue ...'):
    if RICH_ENABLED:
        rich_console.print(f"\n  [dim]{msg}[/dim]")
        try: input()
        except: pass
    else:
        input(colorize(f'\n  {msg}', C.DIM))

def draw_header(title, subtitle=None):
    if not RICH_ENABLED:
        clear_screen()
        print_centered(colorize(f'  {TOOL_NAME} v{TOOL_VERSION}  •  {TOOL_CREDIT}  •  {TOOL_DISCORD_SHORT}  ', C.DIM+C.PULSE))
        print_centered(f'{C.GREY}{H_TL}{H_T*(UI_WIDTH-2)}{H_TR}{C.RESET}')
        print_centered(f'{C.GREY}{H_S}{C.RESET}{colorize(title.center(UI_WIDTH-2), C.BOLD+C.CYAN)}{C.GREY}{H_S}{C.RESET}')
        print_centered(f'{C.GREY}{H_BL}{H_B*(UI_WIDTH-2)}{H_TR}{C.RESET}')
        print(); return
    rich_console.clear()
    sub  = f"[dim]v{TOOL_VERSION}  •  Credits: {TOOL_CREDIT}  •  [link={TOOL_DISCORD}]{TOOL_DISCORD_SHORT}[/link][/dim]"
    body = f'[bold]{title}[/bold]'
    if subtitle: body += f'\n[dim]{subtitle}[/dim]'
    rich_console.print(Panel(Align.center(body), subtitle=sub,
                             style=current_theme['primary'],
                             box=current_theme['box_style'], padding=(0, 3)))
    rich_console.print()

def get_input(prompt, color=None):
    if not RICH_ENABLED:
        try:    w = os.get_terminal_size().columns
        except: w = UI_WIDTH
        fp = colorize(f'  ❯ {prompt}: ', color or C.CYAN)
        return input(' ' * max(0,(w-get_visible_len(fp))//2) + fp)
    try: return Prompt.ask(f"  [{current_theme['info']}]{prompt}[/{current_theme['info']}]")
    except (EOFError, KeyboardInterrupt): return ''

def get_choice(prompt, options):
    if not RICH_ENABLED:
        for i, opt in enumerate(options,1):
            clean = re.sub(r'\[/?[^\]]+\]','',opt)
            print_centered(f"  {colorize(f'[{i}]',C.GREY)}  {C.WHITE}{clean}{C.RESET}")
        print()
        try:
            v = get_input(prompt); return int(v) if v.isdigit() else None
        except (ValueError, EOFError): return None
    tbl = Table(show_header=False, box=box.SIMPLE, padding=(0,2))
    tbl.add_column('N', style=f"bold {current_theme['primary']}", width=6)
    tbl.add_column('Option', style='white')
    for i, opt in enumerate(options,1): tbl.add_row(f'[{i}]', opt)
    rich_console.print(tbl); rich_console.print()
    try:
        v = Prompt.ask(f"  [{current_theme['info']}]{prompt}[/{current_theme['info']}]", default='1')
        return int(v) if v.isdigit() else None
    except (ValueError, EOFError, KeyboardInterrupt): return None

def is_admin():
    try: return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except: return False

def format_uptime(seconds):
    h, r = divmod(int(seconds), 3600); m, s = divmod(r, 60)
    return f'{h:02d}:{m:02d}:{s:02d}'


# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════
_DEFAULT_CONFIG = {
    'profiles': {},
    'roblox_accounts': {},
    'last_used_profile': None,
    'notes': '',
    'settings': {
        'theme': 'Pulse',
        'color_override': None,
        'freeze_detection':        {'enabled': False, 'interval': 30},
        'scheduled_restart':       {'enabled': False, 'interval': 60},
        'session_duration':        0,
        'auto_close_on_ban':       False,
        'notify_on_crash':         True,
        'launch_delay':            5,
        'minimize_on_start':       False,
        'auto_update_check':       True,
        'crash_sound':             False,
        'max_crashes_before_stop': 0,
        'webhook': {
            'enabled':            False,
            'url':                '',        # always user-provided
            'setup_done':         False,
            'bot_token':          '',        # optional — for command listener
            'command_channel_id': '',        # optional — channel to poll for commands
            'commands_enabled':   False,
            'events': {
                'tool_start':         True,
                'tool_close':         True,
                'session_start':      True,
                'session_stop':       True,
                'crash':              True,
                'relaunch':           True,
                'scheduled_restart':  True,
                'freeze':             True,
                'server_hop':         True,
                'crash_limit':        True,
                'account_added':      True,
                'account_removed':    True,
                'account_validated':  False,
                'cookie_batch':       True,
                'profile_created':    True,
                'profile_launched':   True,
                'profile_deleted':    True,
                'settings_changed':   False,
                'roblox_killed':      True,
                'server_info':        True,
                'log_forward':        False,
            },
        },
    },
}

def save_config(data):
    try:
        with open(CONFIG_FILE,'w',encoding='utf-8') as f: json.dump(data, f, indent=4)
    except IOError: show_err('Could not save config!')

def load_config():
    if not os.path.exists(CONFIG_FILE): save_config(_DEFAULT_CONFIG); return _DEFAULT_CONFIG.copy()
    try:
        with open(CONFIG_FILE,'r',encoding='utf-8') as f:
            cfg = json.loads(f.read() or '{}')
        for k, v in _DEFAULT_CONFIG.items():
            if k not in cfg: cfg[k] = v
        for k, v in _DEFAULT_CONFIG['settings'].items():
            cfg.setdefault('settings',{})[k] = cfg['settings'].get(k, v)
        wh_defaults = _DEFAULT_CONFIG['settings']['webhook']
        cfg['settings'].setdefault('webhook', {})
        for k, v in wh_defaults.items():
            if k not in cfg['settings']['webhook']:
                cfg['settings']['webhook'][k] = v
        for ek, ev in wh_defaults['events'].items():
            cfg['settings']['webhook'].setdefault('events', {})[ek] = \
                cfg['settings']['webhook']['events'].get(ek, ev)
        return cfg
    except (json.JSONDecodeError, FileNotFoundError):
        save_config(_DEFAULT_CONFIG); return _DEFAULT_CONFIG.copy()

def apply_theme(cfg=None):
    global current_theme
    if cfg is None: cfg = load_config()
    t = cfg.get('settings',{}).get('theme','Pulse')
    current_theme = RICH_THEMES.get(t, RICH_THEMES['Pulse'])

def _webhook_event_enabled(cfg, event_name):
    ws = cfg.get('settings', {}).get('webhook', {})
    return ws.get('enabled', False) and ws.get('events', {}).get(event_name, True)

def _wh(event_name: str):
    if not webhook or not webhook.enabled or not webhook.webhook_url:
        return False
    cfg = load_config()
    return _webhook_event_enabled(cfg, event_name)


# ══════════════════════════════════════════════════════════════
#  DISCORD BOT COMMAND HANDLERS  (26 commands)
# ══════════════════════════════════════════════════════════════
# Each handler: (args: list[str], invoker: str) -> str

def _cmd_help(args, invoker):
    lines = [
        f"**📋 {TOOL_NAME} v{TOOL_VERSION} — Commands**\n",
        "**── Session Control ──**",
        "`.rejoin [name]`        — Force rejoin all or named instance",
        "`.stop <name>`          — Stop monitoring an instance",
        "`.stopall`              — Stop all sessions",
        "`.pause [name]`         — Pause auto-restart",
        "`.resume [name]`        — Resume auto-restart",
        "`.restart [name]`       — Trigger restart now",
        "`.setrestart <m> [name]`— Change restart interval",
        "",
        "**── Navigation ──**",
        "`.join <placeID|srvID> [acc]` — Join a game/server",
        "`.hop [name]`           — Hop to new server now",
        "`.stophop`              — Stop server hopper",
        "",
        "**── Info ──**",
        "`.status`               — Full session status",
        "`.getid [name]`         — Server / place / user IDs",
        "`.uptime`               — Session uptimes",
        "`.stats`                — Lifetime statistics",
        "`.sysinfo`              — Machine CPU/RAM/disk",
        "`.accounts`             — List saved accounts",
        "`.validate [acc]`       — Validate cookie(s)",
        "",
        "**── Roblox API ──**",
        "`.servers <placeID>`    — List live servers",
        "`.players <placeID>`    — Player count",
        "`.gameinfo <placeID>`   — Game details",
        "",
        "**── Tool Control ──**",
        "`.titan start|stop|status` — Titan spoofer",
        "`.killroblox`           — Kill all Roblox processes",
        "`.ping`                 — Bot latency check",
        "`.log [n]`              — Last n log entries",
        "`.crash <name>`         — Simulate crash (test)",
        "`.alert <msg>`          — Send a custom alert",
        "",
        f"*Prefix: `.`  •  {TOOL_DISCORD_SHORT}*",
    ]
    return "\n".join(lines)

# ── Session control ──────────────────────────────────────────

def _cmd_rejoin(args, invoker):
    if not SESSION_STATUS:
        return "⚠ No active monitoring sessions."
    target  = " ".join(args).strip().lower() if args else None
    results = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if target and target not in name.lower():
                continue
            sd["force_rejoin"] = True
            results.append(f"✅ Rejoin queued → **{name}**")
    return "\n".join(results) if results else f"⚠ No instance matching `{target}`."

def _cmd_stop(args, invoker):
    if not SESSION_STATUS:
        return "⚠ No active sessions."
    target = " ".join(args).strip().lower() if args else None
    if not target:
        return "⚠ Usage: `.stop <instance_name>`"
    stopped = []
    with STATUS_LOCK:
        for sk, sd in list(SESSION_STATUS.items()):
            name = sd.get("instance_name", sk)
            if target in name.lower():
                sd["force_stop"] = True
                stopped.append(name)
    return f"🛑 Stopped: {chr(44).join(stopped)}" if stopped else f"⚠ No instance matching `{target}`."

def _cmd_stopall(args, invoker):
    global MONITORING_ACTIVE
    count = len(SESSION_STATUS)
    if count == 0:
        return "⚠ No active sessions."
    with STATUS_LOCK:
        for sd in SESSION_STATUS.values():
            sd["force_stop"] = True
    MONITORING_ACTIVE = False
    return f"🛑 Stopping all **{count}** session(s)."

def _cmd_pause(args, invoker):
    target = " ".join(args).strip().lower() if args else None
    paused = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if not target or target in name.lower():
                sd["restarter_enabled"] = False
                paused.append(name)
    return f"⏸ Paused: {chr(44).join(paused)}" if paused else "⚠ No matching instances."

def _cmd_resume(args, invoker):
    target = " ".join(args).strip().lower() if args else None
    resumed = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if not target or target in name.lower():
                sd["restarter_enabled"] = True
                sd["next_restart_time"] = 0
                resumed.append(name)
    return f"▶️ Resumed: {chr(44).join(resumed)}" if resumed else "⚠ No matching instances."

def _cmd_restart(args, invoker):
    target = " ".join(args).strip().lower() if args else None
    triggered = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if not target or target in name.lower():
                sd["next_restart_time"] = time.time() - 1
                triggered.append(name)
    return f"🔄 Restart triggered: {chr(44).join(triggered)}" if triggered else "⚠ No matching instances."

def _cmd_setrestart(args, invoker):
    if not args:
        return "⚠ Usage: `.setrestart <minutes> [instance_name]`"
    try:
        mins = float(args[0])
        if mins < 1:
            return "⚠ Minimum 1 minute."
    except ValueError:
        return "⚠ Invalid minutes value."
    target  = " ".join(args[1:]).strip().lower() if len(args) > 1 else None
    changed = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if not target or target in name.lower():
                sd["restarter_enabled"] = True
                sd["next_restart_time"] = time.time() + mins * 60
                changed.append(name)
    cfg = load_config()
    cfg["settings"]["scheduled_restart"]["interval"] = mins
    save_config(cfg)
    suffix = f": {chr(44).join(changed)}" if changed else " (no active sessions matched)"
    return f"⏱ Restart interval → **{mins}m**{suffix}"

# ── Status / Info ────────────────────────────────────────────

def _cmd_status(args, invoker):
    with STATUS_LOCK:
        if not SESSION_STATUS:
            return "⚠ No active monitoring sessions."
        lines = [f"**📊 Status — {len(SESSION_STATUS)} instance(s)**\n"]
        for sk, sd in SESSION_STATUS.items():
            name      = sd.get("instance_name", sk)
            crashes   = sd.get("crashes", 0)
            restarts  = sd.get("restarts", 0)
            auto_r    = "✅ ON" if sd.get("restarter_enabled") else "❌ OFF"
            place_id  = sd.get("place_id", "—")
            uid       = sd.get("user_id", "—")
            uptime    = int(time.time() - sd.get("start_time", time.time()))
            h, r      = divmod(uptime, 3600); m, s = divmod(r, 60)
            uptime_s  = f"{h}h {m}m {s}s"
            game_name = next((k for k, v in GAMES_DB.items() if v == place_id), str(place_id))
            lines.append(
                f"**{name}**\n"
                f"  🎮 {game_name}  ⏱ {uptime_s}\n"
                f"  💥 Crashes: {crashes}  🔄 Restarts: {restarts}  🔁 Auto-R: {auto_r}\n"
                f"  🆔 UID: `{uid}`  🗺 Place: `{place_id}`\n"
            )
        return "\n".join(lines)

def _cmd_getid(args, invoker):
    target = " ".join(args).strip().lower() if args else None
    with STATUS_LOCK:
        if not SESSION_STATUS:
            return "⚠ No active monitoring sessions."
        lines = ["**🆔 Session IDs**\n"]
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if target and target not in name.lower():
                continue
            place_id  = sd.get("place_id", "—")
            uid       = sd.get("user_id", "—")
            srv       = SESSION_SERVER_INFO.get(sk, {})
            server_id = srv.get("server_id", "Unknown")
            lines.append(
                f"**{name}**\n"
                f"  🆔 User ID: `{uid}`\n"
                f"  🗺️ Place ID: `{place_id}`\n"
                f"  🖧 Server ID: `{server_id}`\n"
                f"  🔗 https://www.roblox.com/users/{uid}/profile\n"
            )
        return "\n".join(lines) if len(lines) > 1 else f"⚠ No instance matching `{target}`."

def _cmd_uptime(args, invoker):
    with STATUS_LOCK:
        if not SESSION_STATUS:
            return "⚠ No active sessions."
        lines = ["**⏱ Uptimes**\n"]
        for sk, sd in SESSION_STATUS.items():
            name  = sd.get("instance_name", sk)
            secs  = int(time.time() - sd.get("start_time", time.time()))
            h, r  = divmod(secs, 3600); m, s = divmod(r, 60)
            lines.append(f"**{name}**: {h}h {m}m {s}s")
        return "\n".join(lines)

def _cmd_stats(args, invoker):
    s = load_stats()
    return (
        f"**📈 Lifetime Stats**\n\n"
        f"🎮 Sessions: **{s.get('total_sessions',0)}**\n"
        f"💥 Crashes caught: **{s.get('total_crashes',0)}**\n"
        f"🔄 Restarts: **{s.get('total_restarts',0)}**\n"
        f"🔀 Server hops: **{s.get('total_hops',0)}**\n"
        f"📅 First use: {s.get('first_use','?')[:19]}\n"
        f"🕐 Last use: {s.get('last_use','?')[:19]}"
    )

def _cmd_sysinfo(args, invoker):
    try:
        cpu  = psutil.cpu_percent(interval=1)
        mem  = psutil.virtual_memory()
        disk = psutil.disk_usage("C:\\" if sys.platform == "win32" else "/")
        rblx = [p for p in psutil.process_iter(["name","memory_info"])
                if p.info["name"] == ROBLOX_PROCESS_NAME]
        rmb  = sum(p.info["memory_info"].rss for p in rblx) // 1024 // 1024
        return (
            f"**🖥 System Info**\n\n"
            f"💻 OS: {platform.system()} {platform.release()}\n"
            f"⚙️ CPU: **{cpu}%**\n"
            f"🧠 RAM: **{mem.used//1024//1024:,}MB** / {mem.total//1024//1024:,}MB ({mem.percent}%)\n"
            f"💾 Disk free: **{disk.free//1024//1024//1024}GB** / {disk.total//1024//1024//1024}GB\n"
            f"🎮 Roblox RAM: **{rmb}MB** ({len(rblx)} instance(s))\n"
            f"🤖 Titan: {'Running ✅' if titan_is_running() else 'Stopped ❌'}"
        )
    except Exception as e:
        return f"❌ Error: {e}"

def _cmd_accounts(args, invoker):
    accounts = load_accounts()
    if not accounts:
        return "⚠ No accounts saved."
    lines = [f"**👤 Saved Accounts ({len(accounts)})**\n"]
    for name in accounts:
        uid = _uid_cache.get(name, "?")
        lines.append(f"• **{name}** — ID: `{uid}`")
    return "\n".join(lines)

def _cmd_validate(args, invoker):
    accounts = load_accounts()
    if not accounts:
        return "⚠ No accounts saved."
    target = " ".join(args).strip().lower() if args else None
    lines  = ["**✅ Cookie Validation**\n"]
    for name, cookie in accounts.items():
        if target and target not in name.lower():
            continue
        uname, uid = get_user_info(clean_cookie(cookie))
        if uname:
            _uid_cache[name] = uid
            lines.append(f"✅ **{name}** — valid (ID: `{uid}`)")
        else:
            lines.append(f"❌ **{name}** — INVALID / EXPIRED")
    return "\n".join(lines) if len(lines) > 1 else "⚠ No matching accounts."

# ── Navigation ───────────────────────────────────────────────

def _cmd_join(args, invoker):
    if not args:
        return "⚠ Usage: `.join <placeID or serverID> [accountName]`"
    target_id  = args[0]
    acc_filter = " ".join(args[1:]).strip().lower() if len(args) > 1 else None
    accounts   = load_accounts()
    if not accounts:
        return "⚠ No accounts in vault."
    is_server = "-" in target_id
    results   = []
    for acc_name, cookie in accounts.items():
        if acc_filter and acc_filter not in acc_name.lower():
            continue
        try:
            if is_server:
                place_id = None
                with STATUS_LOCK:
                    for sd in SESSION_STATUS.values():
                        if sd.get("place_id"):
                            place_id = sd["place_id"]; break
                if not place_id:
                    return "⚠ No active session for place_id. Use `.join <placeID>` instead."
                ok = join_server_by_id(place_id, target_id, cookie)
                results.append(f'{"✅" if ok else "❌"} **{acc_name}** → server `{target_id[:16]}...`')
            else:
                pid     = int(target_id)
                servers = get_server_list(pid, cookie, max_servers=10)
                if not servers:
                    results.append(f"⚠ **{acc_name}** — no servers for `{pid}`")
                    continue
                servers.sort(key=lambda s: s["playing"])
                sv = servers[0]
                ok = join_server_by_id(pid, sv["id"], cookie)
                results.append(f'{"✅" if ok else "❌"} **{acc_name}** → place `{pid}` ({sv["playing"]}/{sv["max"]}p)')
        except Exception as e:
            results.append(f"❌ **{acc_name}**: {e}")
        time.sleep(0.5)
    return "\n".join(results) if results else "⚠ No matching accounts."

def _cmd_hop(args, invoker):
    acc_filter = " ".join(args).strip().lower() if args else None
    if not SESSION_STATUS:
        return "⚠ No active sessions."
    accounts = load_accounts()
    results  = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name     = sd.get("instance_name", sk)
            acc_name = sd.get("roblox_account_name", "")
            place_id = sd.get("place_id")
            if acc_filter and acc_filter not in name.lower() and acc_filter not in acc_name.lower():
                continue
            if not place_id or acc_name not in accounts:
                results.append(f"⚠ **{name}** — missing place_id or cookie")
                continue
            cookie  = clean_cookie(accounts[acc_name])
            servers = get_server_list(place_id, cookie, max_servers=20)
            if not servers:
                results.append(f"⚠ **{name}** — no servers found")
                continue
            random.shuffle(servers)
            sv = servers[0]
            ok = join_server_by_id(place_id, sv["id"], cookie)
            if ok:
                increment_stat("total_hops")
                SESSION_SERVER_INFO[sk] = {"server_id": sv["id"], "place_id": place_id}
                results.append(f"🔀 **{name}** hopped → {sv['playing']}/{sv['max']} players")
            else:
                results.append(f"❌ **{name}** hop failed")
    return "\n".join(results) if results else "⚠ No matching instances."

def _cmd_stophop(args, invoker):
    global SERVER_HOP_ACTIVE
    if not SERVER_HOP_ACTIVE:
        return "⚠ Server hopper is not running."
    SERVER_HOP_ACTIVE = False
    with STATUS_LOCK:
        SESSION_STATUS.pop("serverhopper", None)
    return "⏹ Server hopper stopped."

# ── Roblox API ───────────────────────────────────────────────

def _cmd_servers(args, invoker):
    if not args:
        return "⚠ Usage: `.servers <placeID>`"
    try:
        pid = int(args[0])
    except ValueError:
        return "⚠ Place ID must be a number."
    accounts = load_accounts()
    if not accounts:
        return "⚠ No accounts — need a cookie to query Roblox."
    cookie  = clean_cookie(list(accounts.values())[0])
    servers = get_server_list(pid, cookie, max_servers=15)
    if not servers:
        return f"⚠ No servers found for place `{pid}`."
    servers.sort(key=lambda s: s["playing"], reverse=True)
    game_name = next((k for k, v in GAMES_DB.items() if v == pid), f"Place {pid}")
    lines = [f"**🖧 Servers — {game_name}** ({len(servers)} found)\n"]
    for i, sv in enumerate(servers[:10], 1):
        fill = int(sv["playing"] / max(sv["max"], 1) * 10)
        bar  = "█" * fill + "░" * (10 - fill)
        ping = sv.get("ping", 0)
        lines.append(f"`{i:02d}` [{bar}] **{sv['playing']}/{sv['max']}** ping:{ping}ms `{sv['id'][:20]}`")
    return "\n".join(lines)

def _cmd_players(args, invoker):
    if not args:
        return "⚠ Usage: `.players <placeID>`"
    try:
        pid = int(args[0])
    except ValueError:
        return "⚠ Invalid place ID."
    try:
        r   = requests.get(f"https://apis.roblox.com/universes/v1/places/{pid}/universe",
                           headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        uid = r.json().get("universeId") if r.status_code == 200 else None
        if uid:
            r2   = requests.get(f"https://games.roblox.com/v1/games?universeIds={uid}",
                                headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
            data = r2.json().get("data", [{}])[0]
            name    = data.get("name", f"Place {pid}")
            playing = data.get("playing", "?")
            visits  = data.get("visits", "?")
            return (
                f"**👥 {name}**\n"
                f"  🟢 Playing now: **{playing:,}**\n"
                f"  📊 Total visits: **{visits:,}**\n"
                f"  🔗 https://www.roblox.com/games/{pid}"
            )
        accounts = load_accounts()
        cookie   = clean_cookie(list(accounts.values())[0]) if accounts else ""
        servers  = get_server_list(pid, cookie, max_servers=50)
        total    = sum(s["playing"] for s in servers)
        return f"👥 ~**{total}** players across {len(servers)} servers (place `{pid}`)"
    except Exception as e:
        return f"❌ Error: {e}"

def _cmd_gameinfo(args, invoker):
    if not args:
        return "⚠ Usage: `.gameinfo <placeID>`"
    try:
        pid = int(args[0])
    except ValueError:
        return "⚠ Invalid place ID."
    try:
        r    = requests.get(f"https://apis.roblox.com/universes/v1/places/{pid}/universe",
                            headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        uid  = r.json().get("universeId") if r.status_code == 200 else None
        if not uid:
            return f"⚠ Could not find universe for place `{pid}`."
        r2   = requests.get(f"https://games.roblox.com/v1/games?universeIds={uid}",
                            headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        d    = r2.json().get("data", [{}])[0]
        desc = (d.get("description") or "")[:200].replace("\n"," ")
        db   = next((k for k, v in GAMES_DB.items() if v == pid), None)
        return (
            f"**🎮 {d.get('name','?')}**{'  _('+db+')_' if db else ''}\n\n"
            f"👤 Creator: **{d.get('creator',{}).get('name','?')}**\n"
            f"🟢 Playing: **{d.get('playing',0):,}**  👥 Max/server: **{d.get('maxPlayers',0)}**\n"
            f"📊 Visits: **{d.get('visits',0):,}**  ⭐ Favorites: **{d.get('favoritedCount',0):,}**\n"
            f"📝 {desc}\n"
            f"🔗 https://www.roblox.com/games/{pid}"
        )
    except Exception as e:
        return f"❌ Error: {e}"

# ── Tool control ─────────────────────────────────────────────

def _cmd_titan(args, invoker):
    sub = args[0].lower() if args else "status"
    if sub == "start":
        if not titan_is_available():
            return "❌ Titan.exe not found."
        ok, msg = titan_launch()
        return f'{"✅" if ok else "❌"} {msg}'
    elif sub == "stop":
        ok, msg = titan_stop()
        return f'{"✅" if ok else "⚠"} {msg}'
    else:
        avail   = titan_is_available()
        running = titan_is_running()
        return (
            f"**⚙ Titan Status**\n"
            f"  Found: {'✅' if avail else '❌'}\n"
            f"  Running: {'✅ Yes' if running else '❌ No'}"
        )

def _cmd_killroblox(args, invoker):
    killed = 0
    try:
        for p in psutil.process_iter(["name"]):
            if p.info["name"] == ROBLOX_PROCESS_NAME:
                try: p.kill(); killed += 1
                except: pass
    except: pass
    if killed and _wh("roblox_killed"):
        webhook.roblox_killed(killed)
    return f"🧹 Killed **{killed}** Roblox process(es)."

def _cmd_ping(args, invoker):
    t  = time.time()
    try:
        r  = requests.get("https://www.roblox.com", timeout=5)
        ms = int((time.time() - t) * 1000)
        ok = r.status_code < 400
    except:
        ms = -1; ok = False
    bot_ms = int((time.time() - t) * 1000)
    return (
        f"**🏓 Pong!**\n"
        f"  🤖 Bot: **{bot_ms}ms**\n"
        f"  🌐 Roblox: {'✅' if ok else '❌'} {ms}ms\n"
        f"  🖥 `{platform.node()[:24]}`"
    )

def _cmd_log(args, invoker):
    try:
        n = int(args[0]) if args else 15
        n = min(max(n, 1), 40)
    except ValueError:
        n = 15
    entries = DEBUG_LOG[-n:] if DEBUG_LOG else []
    if not entries:
        return "📋 No log entries yet."
    clean = [re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]', '', e) for e in entries]
    return f"**📋 Last {len(clean)} log entries**\n```\n" + "\n".join(clean) + "\n```"

def _cmd_crash(args, invoker):
    target = " ".join(args).strip().lower() if args else None
    if not target:
        return "⚠ Usage: `.crash <instance_name>`"
    triggered = []
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            name = sd.get("instance_name", sk)
            if target in name.lower():
                pid = sd.get("pid")
                if pid:
                    try: psutil.Process(pid).kill(); triggered.append(name)
                    except: triggered.append(f"{name} (kill failed)")
                else:
                    sd["force_rejoin"] = True; triggered.append(f"{name} (rejoin signal)")
    return f"💥 Crash simulated: {chr(44).join(triggered)}" if triggered else f"⚠ No instance matching `{target}`."

def _cmd_alert(args, invoker):
    if not args:
        return "⚠ Usage: `.alert <message>`"
    msg = " ".join(args)
    webhook.generic(
        title       = f"🔔  Alert from {invoker}",
        description = msg,
        level       = "warning",
        fields      = [{"name": "🕐 Time", "value": datetime.datetime.now().strftime("%H:%M:%S"), "inline": True}],
    )
    return f"🔔 Alert sent: *{msg[:80]}*"

def register_discord_commands():
    """Register all commands and auto-start bot if token+channel are configured."""
    if not webhook:
        return
    cmds = {
        "help":        _cmd_help,
        "status":      _cmd_status,
        "rejoin":      _cmd_rejoin,
        "stop":        _cmd_stop,
        "stopall":     _cmd_stopall,
        "pause":       _cmd_pause,
        "resume":      _cmd_resume,
        "restart":     _cmd_restart,
        "setrestart":  _cmd_setrestart,
        "getid":       _cmd_getid,
        "uptime":      _cmd_uptime,
        "stats":       _cmd_stats,
        "sysinfo":     _cmd_sysinfo,
        "accounts":    _cmd_accounts,
        "validate":    _cmd_validate,
        "join":        _cmd_join,
        "hop":         _cmd_hop,
        "stophop":     _cmd_stophop,
        "servers":     _cmd_servers,
        "players":     _cmd_players,
        "gameinfo":    _cmd_gameinfo,
        "titan":       _cmd_titan,
        "killroblox":  _cmd_killroblox,
        "ping":        _cmd_ping,
        "log":         _cmd_log,
        "crash":       _cmd_crash,
        "alert":       _cmd_alert,
    }
    for name, fn in cmds.items():
        webhook.register_command(name, fn)

    # Auto-start: bot runs whenever token + channel are set, no extra toggle needed
    cfg = load_config()
    ws  = cfg.get("settings", {}).get("webhook", {})
    if ws.get("bot_token") and ws.get("command_channel_id"):
        started = webhook.start_command_listener()
        if started:
            log_event(f"Discord bot started — channel {ws['command_channel_id']}", "INFO", "bot")
        else:
            log_event("Discord bot already running.", "INFO", "bot")
    else:
        log_event("Discord bot skipped — no token/channel set.", "WARN", "bot")


# ══════════════════════════════════════════════════════════════
#  ENCRYPTION / ACCOUNTS
# ══════════════════════════════════════════════════════════════
def _enc_key():
    try:   return base64.urlsafe_b64encode(hashlib.sha256(str(uuid.getnode()).encode()).digest())
    except: return base64.urlsafe_b64encode(hashlib.sha256(b'PulseRejoinToolV5').digest())

def encrypt_cookie(s):
    try:
        if not s: return ''
        return Fernet(_enc_key()).encrypt(s.encode()).decode()
    except: return s

def decrypt_cookie(s):
    try:
        if not s or s.startswith('_|WARNING:'): return s
        return Fernet(_enc_key()).decrypt(s.encode()).decode()
    except: return s

def clean_cookie(raw):
    if not isinstance(raw, str): return ''
    if '|_' in raw:
        try: return raw.split('|_')[-1].split(';')[0]
        except: pass
    if '.ROBLOSECURITY=' in raw:
        try: return raw.split('.ROBLOSECURITY=')[1].split(';')[0]
        except: pass
    return raw.strip().split(';')[0]

def load_accounts():
    cfg = load_config()
    return {u: decrypt_cookie(c) for u, c in cfg.get('roblox_accounts',{}).items()}

def save_accounts(accounts):
    cfg = load_config()
    cfg['roblox_accounts'] = {u: encrypt_cookie(c) for u, c in accounts.items()}
    save_config(cfg)

def get_user_info(cookie):
    try:
        r = requests.get('https://users.roblox.com/v1/users/authenticated',
                         headers={'Cookie':f'.ROBLOSECURITY={cookie}','User-Agent':'Mozilla/5.0'},
                         timeout=10)
        if r.status_code == 200:
            d = r.json(); return d.get('name'), d.get('id')
    except: pass
    return None, None

def get_auth_ticket(cookie):
    url     = 'https://auth.roblox.com/v1/authentication-ticket'
    headers = {'Cookie':f'.ROBLOSECURITY={cookie}','Referer':'https://www.roblox.com/games',
               'Content-Type':'application/json'}
    s = requests.Session()
    try:
        r    = s.post(url, headers=headers)
        csrf = r.headers.get('x-csrf-token')
        if not csrf: return None
        headers['X-CSRF-TOKEN'] = csrf
        r = s.post(url, headers=headers)
        if r.status_code == 200 and 'rbx-authentication-ticket' in r.headers:
            return r.headers['rbx-authentication-ticket']
    except: pass
    return None

def get_current_server_id(user_id: int, place_id: int, cookie: str) -> str:
    """Try to fetch the server ID the user is currently in via Roblox API."""
    try:
        r = requests.get(
            f'https://presence.roblox.com/v1/presence/users',
            json={'userIds': [user_id]},
            headers={'Cookie': f'.ROBLOSECURITY={cookie}', 'User-Agent': 'Mozilla/5.0'},
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            for p in data.get('userPresences', []):
                if p.get('gameId'):
                    return str(p['gameId'])
    except: pass
    return ''


# ══════════════════════════════════════════════════════════════
#  ACCOUNT MANAGER  (improved UI)
# ══════════════════════════════════════════════════════════════
def manage_accounts():
    while True:
        draw_header('ACCOUNT MANAGER','Manage your Roblox accounts')
        accounts = load_accounts(); names = list(accounts.keys())

        if not names:
            show_warn('No accounts saved.')
        else:
            if RICH_ENABLED:
                tbl = Table(box=box.ROUNDED, show_header=True,
                            header_style=f"bold {current_theme['primary']}", expand=False)
                tbl.add_column('#',             width=5, justify='center')
                tbl.add_column('Username',      style='bold white', min_width=22)
                tbl.add_column('User ID',       width=14, justify='center')
                tbl.add_column('Cookie Status', width=24)
                tbl.add_column('Profile Link',  width=44)
                for i, n in enumerate(names):
                    cv  = accounts[n]
                    st  = f"[green]● Set ({len(cv)} chars)[/green]" if cv else "[red]✗ Missing[/red]"
                    uid = _cached_uid(n)
                    uid_str  = str(uid) if uid else '[dim]—[/dim]'
                    prof_url = f'[link=https://www.roblox.com/users/{uid}/profile][dim]roblox.com/users/{uid}[/dim][/link]' if uid else '[dim]—[/dim]'
                    tbl.add_row(str(i+1), n, uid_str, st, prof_url)
                rich_console.print(tbl)
        print()
        opts = [
            '➕  Add accounts (paste cookie)',
            '✅  Validate all accounts',
            '🗑   Delete an account',
            '🔍  Extract cookie',
            'ℹ   View account info',
            '🔄  Re-validate single',
            '📡  Send server info to webhook',
            '⬅   Back',
        ]
        choice = get_choice('Select option', opts)
        if   choice == 1: _add_cookies()
        elif choice == 2: _validate_all(accounts)
        elif choice == 3: _delete_account(accounts, names)
        elif choice == 4: _extract_cookie(accounts, names)
        elif choice == 5: _view_account_info(accounts, names)
        elif choice == 6: _refresh_single(accounts, names)
        elif choice == 7: _push_server_info_to_webhook(accounts, names)
        elif choice == 8: return

# Simple UID cache (in-memory per session)
_uid_cache = {}
def _cached_uid(username):
    return _uid_cache.get(username)
def _store_uid(username, uid):
    _uid_cache[username] = uid

def _add_cookies():
    draw_header('ADD ACCOUNTS','Paste one cookie per line  —  blank line to finish')
    lines = []
    while True:
        try:
            ln = input(f'{C.DIM}  paste> {C.RESET}')
            if not ln.strip(): break
            lines.append(ln)
        except EOFError: break
    if not lines: show_warn('Nothing entered.'); time.sleep(2); return
    saved = load_accounts(); ok = fail = 0; added_accounts = []
    for i, raw in enumerate(lines):
        cv = clean_cookie(raw)
        print_centered(f'  Validating {i+1}/{len(lines)} ...')
        name, uid = get_user_info(cv)
        if name:
            saved[name] = raw.strip(); _store_uid(name, uid)
            show_ok(f"'{name}' (ID: {uid}) added."); ok += 1
            added_accounts.append((name, uid))
        else:
            show_err(f'Cookie #{i+1}: Invalid or expired.'); fail += 1
        time.sleep(0.4)
    save_accounts(saved)
    total_now = len(saved)
    if webhook and webhook.enabled and webhook.webhook_url:
        for name, uid in added_accounts:
            if _wh('account_added'): webhook.account_added(name, uid, total_now)
        if len(added_accounts) > 1 and _wh('cookie_batch'):
            webhook.cookie_added_batch(added_accounts, fail)
    show_ok(f'Done — {ok} added, {fail} failed.'); pause()

def _validate_all(accounts):
    draw_header('VALIDATE ALL ACCOUNTS')
    if not accounts: show_warn('No accounts.'); pause(); return
    valid = invalid = 0
    for name, cookie in accounts.items():
        print_centered(f'  Checking {name} ...')
        cv = clean_cookie(cookie); n, uid = get_user_info(cv)
        if n:
            _store_uid(name, uid)
            show_ok(f'{name} → valid (ID: {uid})'); valid += 1
            if _wh('account_validated'): webhook.account_validated(name, uid, True)
        else:
            show_err(f'{name} → INVALID or EXPIRED'); invalid += 1
            if _wh('account_validated'): webhook.account_validated(name, None, False)
        time.sleep(0.3)
    show_info(f'Results: {valid} valid, {invalid} invalid.'); pause()

def _delete_account(accounts, names):
    if not names: show_warn('No accounts.'); time.sleep(2); return
    ch = get_choice('Delete which account', names+['Cancel'])
    if ch and ch <= len(names):
        n = names[ch-1]
        if get_input(f"Delete '{n}'? (y/n)").lower() == 'y':
            del accounts[n]; save_accounts(accounts)
            _uid_cache.pop(n, None)
            if _wh('account_removed'): webhook.account_removed(n, len(accounts))
            show_ok(f"'{n}' deleted.")
    time.sleep(1.5)

def _extract_cookie(accounts, names):
    if not names: show_warn('No accounts.'); time.sleep(2); return
    ch = get_choice('Extract cookie for', names+['Cancel'])
    if ch and ch <= len(names):
        n = names[ch-1]; draw_header(f'COOKIE — {n}'); print(); print(accounts[n]); print(); pause()

def _view_account_info(accounts, names):
    if not names: show_warn('No accounts.'); time.sleep(2); return
    ch = get_choice('Select account', names+['Cancel'])
    if ch and ch <= len(names):
        n = names[ch-1]; draw_header(f'ACCOUNT INFO — {n}')
        cv = clean_cookie(accounts[n]); show_info('Fetching ...')
        uname, uid = get_user_info(cv)
        if uname:
            _store_uid(n, uid)
            if RICH_ENABLED:
                tbl = Table(box=box.ROUNDED, show_header=False)
                tbl.add_column('Key',   style='dim',   width=22)
                tbl.add_column('Value', style='white')
                tbl.add_row('Username',      uname)
                tbl.add_row('User ID',       str(uid))
                tbl.add_row('Profile URL',   f'https://www.roblox.com/users/{uid}/profile')
                tbl.add_row('Cookie Length', str(len(accounts[n]))+' chars')
                tbl.add_row('Cookie Status','[green]Valid ✓[/green]')
                rich_console.print(tbl)
            else:
                print_centered(f'Username: {uname}'); print_centered(f'User ID: {uid}')
        else:
            show_err('Could not fetch info (cookie may be expired).')
        pause()

def _refresh_single(accounts, names):
    if not names: show_warn('No accounts.'); time.sleep(2); return
    ch = get_choice('Re-validate which account', names+['Cancel'])
    if ch and ch <= len(names):
        n = names[ch-1]; cv = clean_cookie(accounts[n]); show_info(f'Validating {n} ...')
        uname, uid = get_user_info(cv)
        if uname: _store_uid(n, uid); show_ok(f'{n} → valid (ID: {uid})')
        else:     show_err(f'{n} → INVALID or EXPIRED')
        pause()

def _push_server_info_to_webhook(accounts, names):
    """Fetch and push current server info for all active sessions to webhook."""
    draw_header('PUSH SERVER INFO','Sends live server + user info to your webhook')
    if not webhook or not webhook.enabled or not webhook.webhook_url:
        show_err('Webhook not configured. Set it up in Settings → Webhook.')
        pause(); return
    if not SESSION_STATUS:
        show_warn('No active monitoring sessions. Launch a session first.')
        pause(); return

    pushed = 0
    with STATUS_LOCK:
        for sk, sd in SESSION_STATUS.items():
            acc_name = sd.get('roblox_account_name') or sd.get('instance_name', sk)
            place_id = sd.get('place_id')
            uid      = sd.get('user_id') or _uid_cache.get(acc_name)
            cookie   = accounts.get(acc_name, '')
            if not cookie: continue
            server_id = ''
            if uid and place_id:
                server_id = get_current_server_id(uid, place_id, clean_cookie(cookie))
            # Update local cache
            SESSION_SERVER_INFO[sk] = {'server_id': server_id, 'place_id': place_id}
            # Find game name
            game_name = next((k for k,v in GAMES_DB.items() if v == place_id), '')
            # Get player counts from server list if we have server_id
            players = max_players = 0
            webhook.server_info(
                account    = acc_name,
                place_id   = place_id,
                server_id  = server_id,
                user_id    = uid,
                players    = players,
                max_players= max_players,
                game_name  = game_name,
            )
            show_ok(f'{acc_name} → pushed (server: {server_id[:16] if server_id else "unknown"})')
            pushed += 1
            time.sleep(0.5)

    show_info(f'Pushed info for {pushed} session(s) to webhook.'); pause()


# ══════════════════════════════════════════════════════════════
#  EMULATOR DETECTION
# ══════════════════════════════════════════════════════════════
MUMU_PROC     = ['MuMuVMM Headless Frontend.exe','MuMuPlayer.exe','NemuHeadless.exe']
MUMU_DIRS     = ['mumu','nemu','mumuplayer']
LDPLAYER_PROC = ['dnplayer.exe','ldplayer.exe','Ld9BoxHeadless.exe']
LDPLAYER_DIRS = ['ldplayer','dnplayer','xuanzhi']
BS_PROC       = ['HD-Player.exe','BlueStacks.exe','BlueStacksX.exe']

def _try_mumu(base):
    if not base or not os.path.exists(base): return None
    for rel in ['nx_main/adb.exe','vmonitor/bin/adb.exe','shell/adb.exe']:
        adb = os.path.join(base, rel); vms = os.path.join(base, 'vms')
        if os.path.exists(adb) and os.path.exists(vms): return (base, adb, vms)
    return None

def get_mumu_paths():
    if sys.platform != 'win32': return (None,None,None)
    if DESKTOP_MODE_ENABLED:
        for p in psutil.process_iter(['name','exe']):
            if p.info['name'] in MUMU_PROC:
                for b in [os.path.dirname(p.info['exe']), os.path.dirname(os.path.dirname(p.info['exe']))]:
                    r = _try_mumu(b)
                    if r: return r
    for d in [os.getenv('ProgramFiles'), os.getenv('ProgramFiles(x86)'), os.getenv('ProgramData')]:
        if d and os.path.exists(d):
            try:
                for dn in os.listdir(d):
                    if any(n in dn.lower() for n in MUMU_DIRS):
                        r = _try_mumu(os.path.join(d,dn))
                        if r: return r
            except: pass
    return (None,None,None)

def find_mumu():
    _, _, vms = get_mumu_paths()
    if not vms: return []
    active = []
    try:
        for folder in os.listdir(vms):
            try:
                lp = os.path.join(vms,folder,'logs','VBox.log')
                if not os.path.exists(lp): continue
                with open(lp,'r',errors='ignore') as f:
                    if 'stopped' in f.read(): continue
                port = name = None
                cfg  = os.path.join(vms,folder,'configs','vm_config.json')
                if os.path.exists(cfg):
                    with open(cfg) as f:
                        d = json.load(f)
                        port = d.get('vm',{}).get('nat',{}).get('port_forward',{}).get('adb',{}).get('host_port')
                ex = os.path.join(vms,folder,'configs','extra_config.json')
                if os.path.exists(ex):
                    with open(ex) as f: name = json.load(f).get('playerName',f'MuMu-{folder}')
                if port: active.append({'port':str(port),'name':name or f'MuMu-{folder}','type':'mumu'})
            except: continue
    except: pass
    return active

def _try_ld(path):
    if not path or not os.path.exists(path): return None
    if os.path.exists(os.path.join(path,'vms')) and os.path.exists(os.path.join(path,'dnconsole.exe')):
        return (path, os.path.join(path,'adb.exe'), os.path.join(path,'vms'))
    return None

def get_ld_paths():
    if DESKTOP_MODE_ENABLED:
        for p in psutil.process_iter(['name','exe']):
            try:
                if p.info['name'] in LDPLAYER_PROC:
                    r = _try_ld(os.path.dirname(p.info['exe']))
                    if r: return r
            except: continue
    for d in [os.getenv('ProgramFiles'), os.getenv('ProgramFiles(x86)'), 'C:\\']:
        if d and os.path.exists(d):
            try:
                for dn in os.listdir(d):
                    if any(n in dn.lower() for n in LDPLAYER_DIRS):
                        r = _try_ld(os.path.join(d,dn))
                        if r: return r
            except: pass
    return (None,None,None)

def find_ldplayer():
    ld_dir,_,_ = get_ld_paths()
    if not ld_dir: return []
    console = os.path.join(ld_dir,'dnconsole.exe')
    if not os.path.exists(console): return []
    active = []
    try:
        res = subprocess.run([console,'list2'],capture_output=True,text=True,check=True,
                             creationflags=subprocess.CREATE_NO_WINDOW,cwd=ld_dir)
        running = []
        for line in res.stdout.strip().splitlines():
            parts = line.split(',')
            if len(parts) >= 5 and parts[4] == '1':
                try: running.append({'index':int(parts[0]),'name':parts[1]})
                except: continue
        for inst in running:
            ar = subprocess.run([console,'adb','--index',str(inst['index']),'--command','get-serialno'],
                                capture_output=True,text=True,creationflags=subprocess.CREATE_NO_WINDOW,
                                cwd=ld_dir,timeout=10)
            m = re.search(r'emulator-(\d+)', ar.stdout+ar.stderr)
            if m: active.append({'port':str(int(m.group(1))+1),'name':inst['name'],'type':'ldplayer'})
    except: pass
    return active

def get_bs_paths():
    if not DESKTOP_MODE_ENABLED: return (None,None)
    for p in psutil.process_iter(['name','exe']):
        try:
            if p.info['name'] in BS_PROC:
                adb = os.path.join(os.path.dirname(p.info['exe']),'HD-Adb.exe')
                if os.path.exists(adb): return (os.path.dirname(p.info['exe']), adb)
        except: continue
    return (None,None)

def find_bluestacks():
    _, adb = get_bs_paths()
    if not adb: return []
    conf = os.path.join(os.getenv('ProgramData',''),'BlueStacks_nxt','bluestacks.conf')
    if not os.path.exists(conf): return []
    ports = {}
    with open(conf,'r',encoding='utf-8') as f:
        for line in f:
            m = re.search(r'bst\.instance\.([^. ]+)\.adb_port="(\d+)"', line)
            if m: ports[m.group(1)] = m.group(2)
    active = []
    try:
        res = subprocess.run([adb,'devices'],capture_output=True,text=True,timeout=10,
                             creationflags=subprocess.CREATE_NO_WINDOW)
        for name, port in ports.items():
            if f'127.0.0.1:{port}' in res.stdout:
                active.append({'port':port,'name':f'BlueStacks-{name}','type':'bluestacks'})
    except: pass
    return active

def find_emulators(mode):
    if mode == 'mumu':       return find_mumu()
    if mode == 'ldplayer':   return find_ldplayer()
    if mode == 'bluestacks': return find_bluestacks()
    return []

def get_adb(mode):
    if mode == 'mumu':       _, a, _ = get_mumu_paths();  return a
    if mode == 'ldplayer':   _, a, _ = get_ld_paths();    return a
    if mode == 'bluestacks': _, a    = get_bs_paths();    return a
    return None

def run_adb(adb, port, cmd, timeout=10):
    full = [adb,'-s',f'127.0.0.1:{port}','shell']+cmd
    try:
        return subprocess.run(full,capture_output=True,text=True,timeout=timeout,check=True,
                              encoding='utf-8',errors='ignore',
                              creationflags=subprocess.CREATE_NO_WINDOW)
    except Exception as e:
        log_event(f'ADB error: {e}','ERROR',str(port)); return None


# ══════════════════════════════════════════════════════════════
#  VISION / CV2
# ══════════════════════════════════════════════════════════════
def find_roblox_hwnd(pid):
    def cb(h, hwnds):
        if win32gui.IsWindowVisible(h) and win32gui.IsWindowEnabled(h):
            _, fp = win32process.GetWindowThreadProcessId(h)
            if fp == pid: hwnds.append(h)
        return True
    hwnds = []; win32gui.EnumWindows(cb, hwnds)
    return hwnds[0] if hwnds else None

def find_visual_error(hwnd, tmpl_dir, thresh=0.9):
    if not hwnd or not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd): return False
    try:
        l,t,r,b = win32gui.GetWindowRect(hwnd); w,h = r-l, b-t
        if w<=0 or h<=0: return False
        with mss() as sct: img = np.array(sct.grab({'top':t,'left':l,'width':w,'height':h}))
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        for root,_,files in os.walk(tmpl_dir):
            for fn in files:
                if fn.lower().endswith('.png'):
                    tmpl = cv2.imread(os.path.join(root,fn), cv2.IMREAD_GRAYSCALE)
                    if tmpl is None: continue
                    res  = cv2.matchTemplate(gray, tmpl, cv2.TM_CCOEFF_NORMED)
                    if cv2.minMaxLoc(res)[1] >= thresh: return True
    except: pass
    return False

def detect_freeze(hwnd, thresh=0.98, interval=10, checks=3, emulator=False):
    if not hwnd or not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd): return False
    try:
        l,t,r,b = win32gui.GetWindowRect(hwnd); w,h = r-l, b-t
        if w<=0 or h<=0: return False
        mon = {'top':t,'left':l,'width':w,'height':h}
        if emulator: thresh=min(thresh,0.95); checks=max(checks,3); interval=max(interval,8)
        with mss() as sct: prev = np.array(sct.grab(mon))
        if emulator:
            nw,nh = int(w*0.5), int(h*0.5); prev = cv2.resize(prev,(nw,nh))
        prev_g = cv2.cvtColor(prev, cv2.COLOR_BGR2GRAY); hits = 0
        for _ in range(checks*2):
            time.sleep(interval)
            if not win32gui.IsWindow(hwnd): return False
            with mss() as sct: cur = np.array(sct.grab(mon))
            if emulator: cur = cv2.resize(cur,(nw,nh))
            cur_g = cv2.cvtColor(cur, cv2.COLOR_BGR2GRAY)
            mse   = np.mean((prev_g.astype(float)-cur_g.astype(float))**2)
            sim   = 1.0 - mse/255.0**2
            if sim >= thresh:
                hits += 1
                if hits >= checks:
                    log_event(f'Freeze detected (sim={sim:.3f})','WARN','freeze')
                    if webhook: webhook.freeze_detected('Unknown', sim)
                    return True
            else: hits=0; prev_g=cur_g
    except: pass
    return False


# ══════════════════════════════════════════════════════════════
#  LAUNCH HELPERS
# ══════════════════════════════════════════════════════════════
def launch_private_server_browser(place_id, link_code, cookie, sk):
    driver = None
    try:
        log_event('Browser launch (private server)...','INFO',sk)
        opts = webdriver.ChromeOptions()
        opts.add_argument(f'user-data-dir={CHROME_PROFILE_PATH}')
        for a in ['--no-sandbox','--disable-dev-shm-usage','--disable-logging',
                  '--log-level=3','--window-size=1000,700','--disable-features=ExternalProtocolPrompt']:
            opts.add_argument(a)
        opts.add_experimental_option('excludeSwitches',['enable-logging'])
        opts.add_experimental_option('useAutomationExtension',False)
        opts.add_experimental_option('prefs',{'protocol_handler.excluded_schemes':{'roblox-player':False,'roblox':False}})
        svc = ChromeService(ChromeDriverManager().install()); svc.creation_flags=subprocess.CREATE_NO_WINDOW
        driver = webdriver.Chrome(service=svc, options=opts); driver.set_page_load_timeout(20)
        driver.get('https://www.roblox.com'); time.sleep(1)
        driver.delete_all_cookies()
        driver.add_cookie({'name':'.ROBLOSECURITY','value':clean_cookie(cookie),
                           'domain':'.roblox.com','path':'/','secure':True,'httpOnly':True})
        driver.get(f'https://www.roblox.com/games/{place_id}/?privateServerLinkCode={link_code}')
        time.sleep(5)
        for _ in range(15):
            rws = []
            def _f(h,w):
                if win32gui.IsWindowVisible(h):
                    t2 = win32gui.GetWindowText(h)
                    if 'Roblox' in t2 and t2 != 'Roblox': w.append(h)
                return True
            win32gui.EnumWindows(_f, rws)
            if rws: log_event('Roblox launched!','SUCCESS',sk); return True
            time.sleep(1)
        return True
    except Exception as e:
        log_event(f'Browser launch failed: {e}','ERROR',sk); return False
    finally:
        if driver:
            try: driver.quit()
            except: pass

def launch_roblox(profile, target, adb_path=None):
    sk        = str(target); place_id = profile.get('place_id')
    link_code = profile.get('private_server_link_code'); cookie = profile.get('roblox_cookie')
    if not place_id: log_event('Missing Place ID!','ERROR',sk); return
    if profile.get('mode') == 'desktop':
        if not cookie: log_event('Missing cookie!','ERROR',sk); return
        if link_code:  launch_private_server_browser(place_id,link_code,cookie,sk); return
        ticket = get_auth_ticket(clean_cookie(cookie))
        if not ticket: log_event('Auth ticket failed.','ERROR',sk); return
        btid = random.randint(100000000000,999999999999)
        pl   = (f'https%3A%2F%2Fassetgame.roblox.com%2Fgame%2FPlaceLauncher.ashx%3Frequest%3D'
                f'RequestGame%26browserTrackerId%3D{btid}%26placeId%3D{place_id}%26isPlayTogetherGame%3Dfalse')
        url  = (f'roblox-player:1+launchmode:play+gameinfo:{ticket}'
                f'+launchtime:{int(time.time()*1000)}+placelauncherurl:{pl}'
                f'+browsertrackerid:{btid}+robloxLocale:en_us+gameLocale:en_us+channel:+LaunchExp:InApp')
        try:  os.startfile(url); log_event('Launch command sent!','INFO',sk)
        except Exception as e: log_event(f'Launch error: {e}','ERROR',sk)
    elif profile.get('mode') in ['mumu','ldplayer','bluestacks']:
        if not adb_path: log_event('No ADB!','ERROR',sk); return
        run_adb(adb_path, sk, ['am','start','-n',
                                f'{ROBLOX_PACKAGE_NAME}/{ROBLOX_ACTIVITY_NAME}',
                                '-a','android.intent.action.VIEW',
                                '-d',f'roblox://placeId={place_id}'])

def find_new_pid(existing):
    for _ in range(20):
        try:
            cur = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
            new = cur - existing
            if new: return new.pop()
        except: pass
        time.sleep(1)
    return None


# ══════════════════════════════════════════════════════════════
#  MONITORING THREADS
# ══════════════════════════════════════════════════════════════
def _check_crash_limit(sk):
    cfg   = load_config(); limit = cfg.get('settings',{}).get('max_crashes_before_stop',0)
    if limit <= 0: return False
    crashes = SESSION_STATUS.get(sk,{}).get('crashes',0)
    if crashes >= limit:
        log_event(f'Crash limit ({limit}) reached. Stopping.','ERROR',sk)
        if webhook: webhook.crash_limit_reached(SESSION_STATUS.get(sk,{}).get('instance_name',sk), limit)
        with STATUS_LOCK:
            if sk in SESSION_STATUS:
                SESSION_STATUS[sk]['status'] = colorize(f'Stopped (crash limit)', C.RED)
        return True
    return False

def _handle_force_rejoin(sk, profile, mode, adb=None):
    """Check for force_rejoin flag and process if set."""
    with STATUS_LOCK:
        sd = SESSION_STATUS.get(sk, {})
        if not sd.get('force_rejoin'):
            return False
        sd['force_rejoin'] = False
    log_event('Force rejoin requested via Discord command.','INFO',sk)
    if mode == 'desktop':
        pid = SESSION_STATUS.get(sk, {}).get('pid')
        if pid:
            try: psutil.Process(pid).kill()
            except: pass
    elif adb:
        run_adb(adb, sk, ['am','force-stop',ROBLOX_PACKAGE_NAME])
        time.sleep(2)
        launch_roblox(profile, sk, adb_path=adb)
    return True

def monitor_emulator(port, adb, profile):
    subprocess.run([adb,'connect',f'127.0.0.1:{port}'],
                   stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                   creationflags=subprocess.CREATE_NO_WINDOW)
    time.sleep(2)
    launch_roblox(profile, port, adb_path=adb)
    with STATUS_LOCK:
        if str(port) in SESSION_STATUS: SESSION_STATUS[str(port)]['crashes'] = 0
    time.sleep(10); _last_freeze = time.time()
    while MONITORING_ACTIVE:
        sk = str(port)
        # Force rejoin check
        if _handle_force_rejoin(sk, profile, profile.get('mode','mumu'), adb):
            time.sleep(10); continue
        name = SESSION_STATUS.get(sk,{}).get('instance_name','')
        hwnd = win32gui.FindWindow(None,name) if name else None
        if find_visual_error(hwnd, base_path):
            log_event('Visual error. Relaunching...','WARN',sk)
            run_adb(adb, port, ['am','force-stop',ROBLOX_PACKAGE_NAME]); time.sleep(2)
            launch_roblox(profile, port, adb_path=adb); time.sleep(10); continue
        now = time.time(); fci = profile.get('freeze_check_interval_main',120)
        if profile.get('freeze_detection_enabled',False) and now-_last_freeze >= fci:
            if detect_freeze(hwnd, emulator=True):
                log_event('Freeze detected. Relaunching...','WARN',sk)
                run_adb(adb, port, ['am','force-stop',ROBLOX_PACKAGE_NAME]); time.sleep(2)
                launch_roblox(profile, port, adb_path=adb); time.sleep(10); continue
            _last_freeze = now
        try:
            ps = run_adb(adb, port, ['ps'])
            if ps and ROBLOX_PACKAGE_NAME in ps.stdout:
                log_event('Running','SUCCESS',sk); time.sleep(5)
            else:
                if _check_crash_limit(sk): break
                inst_name = SESSION_STATUS.get(sk,{}).get('instance_name',sk)
                crash_n   = SESSION_STATUS.get(sk,{}).get('crashes',0) + 1
                log_event('Roblox gone. Relaunching...','WARN',sk)
                with STATUS_LOCK:
                    sd = SESSION_STATUS.get(sk)
                    if sd: sd['crashes'] += 1
                if webhook: webhook.crash_detected(inst_name, crash_n)
                run_adb(adb, port, ['am','force-stop',ROBLOX_PACKAGE_NAME]); time.sleep(2)
                launch_roblox(profile, port, adb_path=adb); time.sleep(10)
                if webhook: webhook.relaunch_success(inst_name, crash_n)
        except Exception as e:
            log_event(f'Error: {e}','ERROR',sk)
            subprocess.run([adb,'connect',f'127.0.0.1:{port}'],
                           stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                           creationflags=subprocess.CREATE_NO_WINDOW)
            time.sleep(15)

def monitor_desktop(profile, sk):
    pid = profile.get('pid')
    if not pid: return
    with STATUS_LOCK:
        if sk in SESSION_STATUS and SESSION_STATUS[sk].get('crashes') == -1:
            SESSION_STATUS[sk]['crashes'] = 0
    _last_freeze = [time.time()]
    # Store place_id and user_id in session status for .getid
    with STATUS_LOCK:
        if sk in SESSION_STATUS:
            SESSION_STATUS[sk]['place_id'] = profile.get('place_id')
            acc_name = profile.get('roblox_account_name','')
            SESSION_STATUS[sk]['roblox_account_name'] = acc_name
            uid = _uid_cache.get(acc_name)
            if uid: SESSION_STATUS[sk]['user_id'] = uid

    while MONITORING_ACTIVE:
        # Force rejoin check
        if _handle_force_rejoin(sk, profile, 'desktop'):
            existing = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
            launch_roblox(profile, sk)
            new_pid = find_new_pid(existing)
            if new_pid:
                pid = new_pid; profile['pid'] = new_pid
                with STATUS_LOCK:
                    if sk in SESSION_STATUS: SESSION_STATUS[sk]['pid'] = new_pid
            time.sleep(10); continue

        hwnd = find_roblox_hwnd(pid); crashed = False
        if find_visual_error(hwnd, base_path):
            log_event('Visual error. Relaunching...','WARN',sk)
            try: psutil.Process(pid).kill()
            except: pass
            crashed = True
        now = time.time(); fci = profile.get('freeze_check_interval_main',120)
        if not crashed and profile.get('freeze_detection_enabled',False) and now-_last_freeze[0] >= fci:
            if detect_freeze(hwnd):
                inst_name = SESSION_STATUS.get(sk,{}).get('instance_name',sk)
                log_event('Freeze. Relaunching...','WARN',sk)
                if webhook: webhook.freeze_detected(inst_name, 0.99)
                try: psutil.Process(pid).kill()
                except: pass
                crashed = True
            _last_freeze[0] = now
        if not crashed:
            try:
                if psutil.pid_exists(pid):
                    proc = psutil.Process(pid)
                    if proc.name()==ROBLOX_PROCESS_NAME and proc.status() in ['running','sleeping','disk-sleep']:
                        log_event('Running','SUCCESS',sk); time.sleep(5); continue
                crashed = True
            except psutil.NoSuchProcess: crashed = True
            except Exception as e:
                log_event(f'Monitor error: {e}','ERROR',sk); time.sleep(5); continue
        if crashed:
            if _check_crash_limit(sk): break
            inst_name = SESSION_STATUS.get(sk,{}).get('instance_name',sk)
            crash_n   = SESSION_STATUS.get(sk,{}).get('crashes',0) + 1
            log_event('Crash detected. Relaunching...','ERROR',sk)
            if webhook: webhook.crash_detected(inst_name, crash_n)
            with STATUS_LOCK:
                sd = SESSION_STATUS.get(sk)
                if sd:
                    sd['crashes'] += 1
                    if sd.get('restarter_enabled') and profile.get('restarter_interval',0) > 0:
                        sd['next_restart_time'] = time.time() + profile['restarter_interval']*60
            if not profile.get('roblox_cookie'): break
            existing = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
            launch_roblox(profile, sk); new_pid = find_new_pid(existing)
            if new_pid:
                pid=new_pid; profile['pid']=new_pid
                with STATUS_LOCK:
                    if sk in SESSION_STATUS: SESSION_STATUS[sk]['pid']=new_pid
                time.sleep(5)
                if webhook: webhook.relaunch_success(inst_name, crash_n)
            else:
                log_event('Relaunch failed.','ERROR',sk); time.sleep(10)

def restarter_emulator(port, mins, profile, adb):
    while MONITORING_ACTIVE:
        time.sleep(1); restart=False
        with STATUS_LOCK:
            sd = SESSION_STATUS.get(str(port))
            if not sd or not sd.get('restarter_enabled'): continue
            if sd.get('next_restart_time',0)==0: sd['next_restart_time']=time.time()+mins*60
            if time.time() >= sd.get('next_restart_time',float('inf')):
                restart=True; sd['restarts']+=1; sd['next_restart_time']=time.time()+mins*60
        if restart:
            inst_name = SESSION_STATUS.get(str(port),{}).get('instance_name',str(port))
            log_event('Scheduled restart!','INFO',str(port))
            increment_stat('total_restarts')
            if webhook: webhook.scheduled_restart(inst_name, SESSION_STATUS.get(str(port),{}).get('restarts',1))
            run_adb(adb, str(port), ['am','force-stop',ROBLOX_PACKAGE_NAME]); time.sleep(5)
            launch_roblox(profile, str(port), adb_path=adb)

def restarter_desktop(profile, mins, sk):
    while MONITORING_ACTIVE:
        time.sleep(1); restart=False
        with STATUS_LOCK:
            sd = SESSION_STATUS.get(sk)
            if not sd or not sd.get('restarter_enabled'): continue
            if sd.get('next_restart_time',0)==0: sd['next_restart_time']=time.time()+mins*60
            if time.time() >= sd.get('next_restart_time',float('inf')):
                restart=True; sd['restarts']+=1; sd['next_restart_time']=time.time()+mins*60
        if restart:
            inst_name = SESSION_STATUS.get(sk,{}).get('instance_name',sk)
            log_event('Scheduled restart!','INFO',sk)
            increment_stat('total_restarts')
            if webhook: webhook.scheduled_restart(inst_name, SESSION_STATUS.get(sk,{}).get('restarts',1))
            try:
                pid = SESSION_STATUS.get(sk,{}).get('pid')
                if pid and psutil.pid_exists(pid): psutil.Process(pid).kill()
            except: pass


# ══════════════════════════════════════════════════════════════
#  HOTKEY / TOGGLE
# ══════════════════════════════════════════════════════════════
def req_toggle(): global TOGGLE_REQUESTED;       TOGGLE_REQUESTED=True
def req_menu():   global EXIT_TO_MENU_REQUESTED; EXIT_TO_MENU_REQUESTED=True
def req_debug():  global DEBUG_MODE_ACTIVE;      DEBUG_MODE_ACTIVE=not DEBUG_MODE_ACTIVE

def handle_toggle_menu():
    draw_header('TOGGLE SCHEDULED RESTART')
    with STATUS_LOCK: sessions = list(SESSION_STATUS.items())
    if not sessions: show_warn('No active sessions.'); time.sleep(2); return
    opts = []
    for k, d in sessions:
        st = colorize('ON',C.GREEN) if d.get('restarter_enabled') else colorize('OFF',C.RED)
        opts.append(f"{d['instance_name']}  [{st}]")
    opts.append('Cancel')
    ch = get_choice('Toggle restart for', opts)
    if ch and 1 <= ch <= len(sessions):
        k, d = sessions[ch-1]
        with STATUS_LOCK:
            cur = SESSION_STATUS[k].get('restarter_enabled',False)
            SESSION_STATUS[k]['restarter_enabled'] = not cur
            c = C.GREEN if not cur else C.RED
            print_centered(colorize(f'Restarts {"ENABLED" if not cur else "DISABLED"} for {d["instance_name"]}',c))
        time.sleep(1.5)

def end_after(hours):
    if hours > 0:
        time.sleep(hours*3600)
        global MONITORING_ACTIVE; MONITORING_ACTIVE=False; os._exit(0)

def warn_before_launch():
    draw_header('⚠  ACCOUNT SAFETY  ⚠')
    show_warn('Using executors or third-party tools risks bans.')
    show_warn('Use alternate accounts. Pulse is NOT responsible.')
    print()
    return get_input('Continue? (y/n)').lower() == 'y'


# ══════════════════════════════════════════════════════════════
#  LIVE DASHBOARD  (improved)
# ══════════════════════════════════════════════════════════════
def draw_dashboard(start_time, duration_hours):
    if not RICH_ENABLED:
        clear_screen()
        print_centered(colorize(f'  {TOOL_NAME} v{TOOL_VERSION}  LIVE DASHBOARD  ',C.BOLD+C.CYAN))
        with STATUS_LOCK:
            for k, d in sorted(SESSION_STATUS.items()):
                print_centered(f"  {d.get('instance_name',k)}  |  {d.get('status','')}  |  "
                               f"Crashes:{d.get('crashes',0)}  Restarts:{d.get('restarts',0)}")
        print_centered(colorize('  CTRL+T: Toggle  CTRL+X: Menu  CTRL+G: Debug  CTRL+C: Exit  ',C.DIM))
        return

    rich_console.clear()

    if duration_hours > 0:
        rem    = max(0, duration_hours*3600-(time.time()-start_time))
        ttext  = f'⏱  Remaining: {format_uptime(rem)}'
        tstyle = current_theme['warning']
    else:
        el     = time.time()-start_time
        ttext  = f'⏱  Uptime: {format_uptime(el)}'
        tstyle = current_theme['info']

    with STATUS_LOCK:
        total_crashes  = sum(d.get('crashes',0) for d in SESSION_STATUS.values() if d.get('crashes',0)>=0)
        total_restarts = sum(d.get('restarts',0) for d in SESSION_STATUS.values())
        active_count   = len(SESSION_STATUS)

    titan_badge = ''
    if titan_is_available():
        titan_badge = ('  [bright_green]⚙ Titan: ON[/bright_green]' if titan_is_running()
                       else '  [dim]⚙ Titan: OFF[/dim]')
    wh_badge = ''
    if webhook and webhook.enabled and webhook.webhook_url:
        cmd_badge = '  [bright_magenta]⌨ Cmds: ON[/bright_magenta]' if webhook._cmd_active else ''
        wh_badge  = f'  [bright_blue]📡 Webhook: ON[/bright_blue]{cmd_badge}'

    hdr = Panel(
        Align.center(
            f"[bold {current_theme['secondary']}]🎮  {TOOL_NAME.upper()}  v{TOOL_VERSION}  —  LIVE DASHBOARD[/bold {current_theme['secondary']}]\n"
            f"[{tstyle}]{ttext}[/{tstyle}]   "
            f"[dim]Sessions: {active_count}  •  Crashes: {total_crashes}  •  Restarts: {total_restarts}[/dim]"
            f"{titan_badge}{wh_badge}"
        ),
        box=current_theme['box_style'], style=current_theme['primary'], padding=(0,3),
    )

    tbl = Table(box=box.ROUNDED, show_header=True,
                header_style=f"bold {current_theme['primary']}", expand=True)
    tbl.add_column('Instance',     style=current_theme['info'], width=22, no_wrap=True)
    tbl.add_column('Status',       width=26)
    tbl.add_column('Place ID',     justify='center', width=16, style='dim')
    tbl.add_column('Crashes',      justify='center', width=8)
    tbl.add_column('Restarts',     justify='center', width=9)
    tbl.add_column('Next Restart', justify='center', width=13)
    tbl.add_column('Auto-R',       justify='center', width=7)
    tbl.add_column('Uptime',       justify='center', width=10)
    tbl.add_column('RAM',          justify='center', width=8)

    with STATUS_LOCK:
        if not SESSION_STATUS:
            tbl.add_row(f"[{current_theme['dim']}]No active sessions[/{current_theme['dim']}]",
                        '','','','','','','','')
        else:
            for k, d in sorted(SESSION_STATUS.items()):
                sc  = re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]','',str(d.get('status','')))
                sl  = sc.lower()
                if 'running' in sl or '●' in sl:
                    st = f"[{current_theme['success']}]{sc}[/{current_theme['success']}]"
                elif 'error' in sl or 'crash' in sl or 'stopped' in sl:
                    st = f"[{current_theme['error']}]{sc}[/{current_theme['error']}]"
                elif 'init' in sl or 'wait' in sl or 'launch' in sl:
                    st = f"[{current_theme['warning']}]{sc}[/{current_theme['warning']}]"
                else: st = sc

                nr = 'N/A'
                if d.get('restarter_enabled') and d.get('next_restart_time',0) > 0:
                    secs = d['next_restart_time']-time.time()
                    if secs > 0:
                        mm,ss = divmod(int(secs),60); nr = f'{mm:02d}:{ss:02d}'
                    else: nr = f"[{current_theme['warning']}]Now![/{current_theme['warning']}]"

                ar   = 'ON' if d.get('restarter_enabled') else 'OFF'
                ar_s = (f"[{current_theme['success']}]{ar}[/{current_theme['success']}]"
                        if d.get('restarter_enabled')
                        else f"[{current_theme['error']}]{ar}[/{current_theme['error']}]")

                el2        = time.time()-d.get('start_time',start_time)
                uptime_str = format_uptime(el2)
                ram_str    = '—'
                pid = d.get('pid')
                if pid:
                    try:
                        proc    = psutil.Process(pid)
                        mb      = proc.memory_info().rss//1024//1024
                        ram_str = f'{mb}MB'
                    except: pass

                place_id_str = str(d.get('place_id','—'))

                tbl.add_row(d.get('instance_name',k), st, place_id_str,
                            str(d.get('crashes',0)), str(d.get('restarts',0)),
                            nr, ar_s, uptime_str, ram_str)

    footer = Panel(
        Align.center(
            f"[{current_theme['dim']}]"
            f"CTRL+T: Toggle Restart  •  CTRL+X: Menu  •  CTRL+G: Debug  •  CTRL+C: Exit\n"
            f"[link={TOOL_DISCORD}]💬 {TOOL_DISCORD_SHORT}[/link]  •  v{TOOL_VERSION}  •  Credits: {TOOL_CREDIT}"
            f"[/{current_theme['dim']}]"
        ), box=box.SIMPLE, style=current_theme['dim'],
    )
    rich_console.print(hdr); rich_console.print(tbl); rich_console.print(footer)

def draw_debug():
    if not RICH_ENABLED:
        clear_screen()
        for e in DEBUG_LOG[-40:]: print(e); return
    rich_console.clear()
    log_text = '\n'.join(DEBUG_LOG[-60:]) if DEBUG_LOG else f"[{current_theme['dim']}]No logs yet[/{current_theme['dim']}]"
    rich_console.print(Panel(log_text, title='[bold magenta]🔍  Debug Log[/bold magenta]',
                             subtitle='[dim]Last 60 entries  •  CTRL+G to hide[/dim]',
                             box=box.ROUNDED, style='magenta', padding=(1,2)))
    rich_console.print(Align.center('[dim]CTRL+G: Toggle  •  CTRL+X: Menu  •  CTRL+C: Exit[/dim]'))


# ══════════════════════════════════════════════════════════════
#  GAME / PROFILE SELECTION
# ══════════════════════════════════════════════════════════════
def resolve_share_link(url):
    driver = None
    try:
        accounts = load_accounts()
        if not accounts: show_err('No accounts to resolve link.'); return (None,None)
        cookie = list(accounts.values())[0]
        opts   = webdriver.ChromeOptions()
        opts.add_argument(f'user-data-dir={CHROME_PROFILE_PATH}')
        opts.add_argument('--headless'); opts.add_argument('--no-sandbox')
        opts.add_argument('--log-level=3'); opts.add_argument('--disable-dev-shm-usage')
        opts.add_experimental_option('excludeSwitches',['enable-logging'])
        opts.add_experimental_option('prefs',{'protocol_handler.excluded_schemes':{'roblox-player':False,'roblox':False}})
        svc = ChromeService(ChromeDriverManager().install()); svc.creation_flags=subprocess.CREATE_NO_WINDOW
        driver = webdriver.Chrome(service=svc, options=opts); driver.set_page_load_timeout(20)
        driver.get('https://www.roblox.com'); time.sleep(1)
        driver.delete_all_cookies()
        driver.add_cookie({'name':'.ROBLOSECURITY','value':clean_cookie(cookie),
                           'domain':'.roblox.com','path':'/','secure':True,'httpOnly':True})
        driver.get(url); time.sleep(3); final=driver.current_url
        pm = re.search(r'/games/(\d+)/', final)
        lm = re.search(r'privateServerLinkCode=([\w\d\-=_]+)', final)
        if pm and lm: show_ok(f'Resolved! Place ID: {pm.group(1)}'); time.sleep(2); return (int(pm.group(1)),lm.group(1))
        show_err('Could not extract details from redirected URL.'); time.sleep(2); return (None,None)
    except Exception as e:
        show_err(f'Resolve error: {e}'); time.sleep(2); return (None,None)
    finally:
        if driver:
            try: driver.quit()
            except: pass

def select_game(ctx='SELECT GAME'):
    draw_header(ctx)
    top_opts  = list(GAME_CATEGORIES.keys()) + ['Custom Place ID','Private Server Link','Back']
    while True:
        print()
        cat_ch = get_choice('Category or option', top_opts)
        if not cat_ch: draw_header(ctx); continue
        if cat_ch <= len(GAME_CATEGORIES):
            cat_name  = list(GAME_CATEGORIES.keys())[cat_ch-1]
            game_list = GAME_CATEGORIES[cat_name]
            draw_header(ctx, cat_name)
            game_ch = get_choice('Select game', game_list+['Back'])
            if game_ch and game_ch <= len(game_list): return (GAMES_DB[game_list[game_ch-1]], None)
            draw_header(ctx); continue
        elif cat_ch == len(GAME_CATEGORIES)+1:
            draw_header(ctx)
            try: return (int(get_input('Enter Roblox Place ID')), None)
            except (ValueError, TypeError): show_err('Invalid ID.'); time.sleep(2); draw_header(ctx)
        elif cat_ch == len(GAME_CATEGORIES)+2:
            draw_header(ctx)
            url = get_input('Paste Private Server Link')
            m   = re.search(r'roblox\.com/games/(\d+)/.*?privateServerLinkCode=([\w\d\-=_]+)', url)
            if m: show_ok(f'Detected Place ID: {m.group(1)}'); time.sleep(1); return (int(m.group(1)),m.group(2))
            if re.search(r'roblox\.com/share\?code=([a-f0-9]+)&type=Server', url, re.IGNORECASE):
                p,l = resolve_share_link(url)
                if p: return (p,l)
            try:
                r  = requests.get(url,allow_redirects=True,timeout=10)
                pm = re.search(r'/games/(\d+)/',r.url)
                lm = re.search(r'privateServerLinkCode=([\w\d\-=_]+)',r.url)
                if pm and lm: return (int(pm.group(1)),lm.group(1))
            except: pass
            show_err('Could not resolve link.'); time.sleep(2); draw_header(ctx)
        else: return (None,None)

def configure_restart():
    cfg = load_config(); rd = cfg.get('settings',{}).get('scheduled_restart',{})
    draw_header('SCHEDULED RESTART')
    print_centered(f"Default: {'Enabled' if rd.get('enabled') else 'Disabled'} @ {rd.get('interval',60)}m")
    opts = ['Use Default','20 min','30 min','45 min','1 hour','2 hours','4 hours','6 hours',
            'Testing (30 sec)','Custom','Disable']
    ch = get_choice('Select interval', opts)
    if ch == 1:  return (rd.get('enabled',False), rd.get('interval',60))
    if ch == 11: return (False, rd.get('interval',60))
    if ch == 9:  return (True, 0.5)
    m_map = {2:20,3:30,4:45,5:60,6:120,7:240,8:360}
    if ch in m_map: return (True, m_map[ch])
    if ch == 10:
        while True:
            try:
                v = float(get_input('Minutes (min 20)'))
                if v >= 20: return (True, v)
                show_err('Minimum 20 min.')
            except ValueError: show_err('Invalid.')
    return (rd.get('enabled',False), rd.get('interval',60))


# ══════════════════════════════════════════════════════════════
#  PROFILE MANAGER
# ══════════════════════════════════════════════════════════════
def create_profile(cfg, name=None):
    is_edit  = name is not None
    settings = cfg['profiles'].get(name,{}) if is_edit else {}
    if not is_edit:
        mode_opts = ['MuMu Player','LDPlayer','BlueStacks',
                     'Normal Roblox (Desktop)' if DESKTOP_MODE_ENABLED else colorize('Desktop (psutil missing)',C.GREY),
                     'Cancel']
        draw_header('SELECT MODE')
        mc = get_choice('Mode', mode_opts); mode_map = {1:'mumu',2:'ldplayer',3:'bluestacks',4:'desktop'}
        if mc == 5 or mc not in mode_map: return
        if mc == 4 and not DESKTOP_MODE_ENABLED: show_err('psutil required.'); time.sleep(2); return
        settings['mode'] = mode_map[mc]

    draw_header(f"PROFILE: {name or 'New'}", settings.get('mode','').capitalize())
    if not is_edit or get_input('Change game? (y/n)').lower() == 'y':
        pid, lc = select_game(f"PROFILE: {name or 'New'}")
        if pid is not None: settings['place_id']=pid; settings['private_server_link_code']=lc
        elif not is_edit: return
    if not settings.get('place_id'): show_err('No game. Aborting.'); time.sleep(2); return

    if settings.get('mode') == 'desktop':
        accounts = load_accounts()
        if not accounts: show_err('No accounts! Add one first.'); time.sleep(3); return
        names = list(accounts.keys()); selected = settings.get('accounts',[]) if is_edit else []
        while True:
            draw_header('SELECT ACCOUNTS')
            for i, n in enumerate(names):
                mk = colorize(' ✓',C.GREEN) if n in selected else ''
                print_centered(f'  [{i+1}] {n}{mk}')
            print()
            s = get_input("Numbers to toggle (e.g. 1,3) or 'done'")
            if s.lower() == 'done':
                if not selected: show_err('Select at least one.'); time.sleep(2); continue
                break
            for idx in [int(x.strip()) for x in s.split(',') if x.strip().isdigit()]:
                if 1 <= idx <= len(names):
                    n = names[idx-1]; selected.remove(n) if n in selected else selected.append(n)
        settings['accounts'] = selected

    settings['restarter_on'], settings['restarter_interval'] = configure_restart()
    fd = cfg.get('settings',{}).get('freeze_detection',{})
    settings['freeze_detection_enabled']   = fd.get('enabled',False)
    settings['freeze_check_interval_main'] = fd.get('interval',30)

    if not is_edit:
        while True:
            n = get_input('Profile name')
            if n and n not in cfg['profiles']: name=n; break
            show_err('Empty or already exists.')

    cfg['profiles'][name] = settings; save_config(cfg)
    show_ok(f"Profile '{name}' saved!"); time.sleep(2)

def manage_profiles():
    while True:
        cfg=load_config(); apply_theme(cfg); draw_header('PROFILE MANAGER')
        profiles=cfg.get('profiles',{}); names=list(profiles.keys())
        if not names:
            show_warn('No profiles.')
            if get_choice('', ['Create Profile','Back']) == 1: create_profile(cfg)
            else: return
            continue
        display = []
        for n in names:
            mode  = profiles[n].get('mode','?').capitalize()
            accs  = len(profiles[n].get('accounts',[]))
            acc_s = f' • {accs} account(s)' if mode=='Desktop' else ''
            rest  = '🔄' if profiles[n].get('restarter_on') else ''
            display.append(f'{n}  [dim]({mode}{acc_s}) {rest}[/dim]' if RICH_ENABLED else f'{n} ({mode}{acc_s})')
        opts = ['▶  Launch','➕  Create New','✏   Edit','🗑   Delete','📋  Duplicate','🔧  Troubleshoot Connection','⬅   Back']
        print_centered(colorize(f'  {TOOL_DISCORD_SHORT}  •  Credits: {TOOL_CREDIT}  ',C.DIM+C.PULSE))
        ch = get_choice('Option', opts)
        if ch == 1:
            lch = get_choice('Launch which profile', display+['Cancel'])
            if lch and lch <= len(names):
                pd = profiles[names[lch-1]]
                if not warn_before_launch(): continue
                _profile_launch(pd, names[lch-1])
        elif ch == 2: create_profile(cfg)
        elif ch == 3:
            ech = get_choice('Edit which', display+['Cancel'])
            if ech and ech <= len(names): create_profile(cfg, names[ech-1])
        elif ch == 4:
            dch = get_choice('Delete which', display+['Cancel'])
            if dch and dch <= len(names):
                n = names[dch-1]
                if get_input(f"Delete '{n}'? (y/n)").lower() == 'y':
                    del cfg['profiles'][n]; save_config(cfg)
                    if _wh('profile_deleted'): webhook.profile_deleted(n)
                    show_ok(f"'{n}' deleted.")
        elif ch == 5:
            dup_ch = get_choice('Duplicate which', display+['Cancel'])
            if dup_ch and dup_ch <= len(names):
                src = names[dup_ch-1]; new = get_input(f"New name for copy of '{src}'")
                if new and new not in cfg['profiles']:
                    import copy; cfg['profiles'][new] = copy.deepcopy(cfg['profiles'][src])
                    save_config(cfg); show_ok(f"'{src}' duplicated as '{new}'.")
                else: show_err('Name empty or already exists.')
                time.sleep(1.5)
        elif ch == 6: troubleshoot()
        elif ch == 7: return

def _profile_launch(pd, name):
    cfg = load_config()
    if pd.get('mode') == 'desktop':
        assoc=pd.get('accounts',[]); saved=load_accounts(); runs=[]
        for acc in assoc:
            if acc not in saved: continue
            p=pd.copy(); p['profile_name']=f'{name} - {acc}'
            p['roblox_account_name']=acc; p['roblox_cookie']=saved[acc]; runs.append(p)
        if not runs: show_err('No accounts found.'); time.sleep(3); return
        cfg['last_used_profile']=name; save_config(cfg); launch_monitor_session(runs)
    else:
        active = find_emulators(pd['mode'])
        if not active: show_err(f'No active {pd["mode"].capitalize()} instances.'); time.sleep(3); return
        runs = []
        for emul in active:
            p=pd.copy(); p['profile_name']=name; p['instance_target']=emul; runs.append(p)
        cfg['last_used_profile']=name; save_config(cfg); launch_monitor_session(runs)

def troubleshoot():
    draw_header('CONNECTION TROUBLESHOOTER')
    ch = get_choice('Emulator type',['MuMu','LDPlayer','BlueStacks','Cancel'])
    if not ch or ch > 3: return
    modes={1:'mumu',2:'ldplayer',3:'bluestacks'}; adb=get_adb(modes[ch])
    if not adb: show_err('ADB not found.'); pause(); return
    show_info('Resetting ADB server...')
    try:
        subprocess.run([adb,'kill-server'],capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
        time.sleep(1)
        subprocess.run([adb,'start-server'],capture_output=True,text=True,timeout=5,
                       creationflags=subprocess.CREATE_NO_WINDOW)
        show_ok('ADB reset.')
    except Exception as e: show_err(f'{e}')
    show_info(f'Admin: {"Yes ✓" if is_admin() else "NO — run as administrator!"}')
    try:
        res = subprocess.run([adb,'devices'],capture_output=True,text=True,timeout=5,
                             creationflags=subprocess.CREATE_NO_WINDOW)
        for ln in res.stdout.strip().splitlines(): print_centered(colorize(f'  {ln}',C.WHITE))
    except Exception as e: show_err(f'{e}')
    pause()


# ══════════════════════════════════════════════════════════════
#  MONITOR ALL DASHBOARD
# ══════════════════════════════════════════════════════════════
def monitor_all_dashboard():
    draw_header('MONITOR ALL','Select platforms to include')
    ET = {'mumu','ldplayer','bluestacks'}
    plat_opts = {'mumu':'MuMu Player','ldplayer':'LDPlayer','bluestacks':'BlueStacks'}
    if DESKTOP_MODE_ENABLED:
        plat_opts['desktop']  = 'Normal Roblox (Desktop)'
        plat_opts['existing'] = 'Monitor Existing Instances'
    selected = []
    while True:
        draw_header('SELECT PLATFORMS')
        choices = []
        for k, n in plat_opts.items():
            tag = colorize(' ✓',C.GREEN) if k in selected else ''
            choices.append(f'{n}{tag}')
        choices += ['Continue →','Cancel']
        ch = get_choice('Toggle platforms', choices)
        if not ch: continue
        if 1 <= ch <= len(plat_opts):
            key      = list(plat_opts.keys())[ch-1]
            sel_emul = next((k for k in selected if k in ET), None)
            if key in ET and sel_emul and key != sel_emul:
                show_err('Only one emulator type per session.'); time.sleep(3); continue
            selected.remove(key) if key in selected else selected.append(key)
        elif ch == len(choices)-1:
            if not selected: show_err('Select at least one.'); time.sleep(2)
            else: break
        else: return

    profiles_to_run = []
    for plat in selected:
        if plat in ET:
            draw_header(f'SCAN: {plat_opts[plat].upper()}'); show_info('Scanning ...')
            active = find_emulators(plat)
            if not active: show_warn(f'No active {plat.capitalize()} instances.'); time.sleep(2); continue
            for emul in active: show_ok(f"Found: {emul['name']} (port {emul['port']})")
            time.sleep(1)
            for emul in active:
                pid, lc = select_game(f"CONFIGURE: {emul['name']}")
                if pid is None: continue
                ro, ri  = configure_restart()
                fd = load_config().get('settings',{}).get('freeze_detection',{})
                profiles_to_run.append({
                    'profile_name':f"MonAll-{emul['name']}",'mode':plat,'instance_target':emul,
                    'place_id':pid,'private_server_link_code':lc,'restarter_on':ro,'restarter_interval':ri,
                    'freeze_detection_enabled':fd.get('enabled',False),'freeze_check_interval_main':fd.get('interval',30),
                })
        elif plat == 'desktop':
            accounts = load_accounts()
            if not accounts: show_err('No accounts.'); time.sleep(3); continue
            try:
                n = int(get_input('How many desktop instances?'))
                if n <= 0: raise ValueError
            except: show_err('Invalid.'); time.sleep(2); continue
            for i in range(n):
                draw_header(f'DESKTOP INSTANCE {i+1}/{n}')
                anames = list(accounts.keys())
                ac = get_choice(f'Account for instance {i+1}', anames+['Cancel'])
                if not ac or ac > len(anames): continue
                aname = anames[ac-1]; pid, lc = select_game(f'CONFIGURE: {aname}')
                if pid is None: continue
                ro, ri = configure_restart()
                fd = load_config().get('settings',{}).get('freeze_detection',{})
                profiles_to_run.append({
                    'profile_name':f'MonAll-{aname}','mode':'desktop','place_id':pid,
                    'private_server_link_code':lc,'restarter_on':ro,'restarter_interval':ri,
                    'roblox_account_name':aname,'roblox_cookie':accounts[aname],
                    'freeze_detection_enabled':fd.get('enabled',False),'freeze_check_interval_main':fd.get('interval',30),
                })
        elif plat == 'existing':
            profiles_to_run.extend(scan_existing())

    if profiles_to_run:
        if not warn_before_launch(): return
        increment_stat('total_sessions'); launch_monitor_session(profiles_to_run)
    else:
        show_warn('No instances configured.'); time.sleep(3)

def scan_existing():
    draw_header('SCAN EXISTING INSTANCES'); profiles=[]
    active = find_mumu()+find_ldplayer()+find_bluestacks()
    pids   = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
    if not active and not pids: show_warn('No running instances.'); time.sleep(3); return []
    for emul in active:
        pid, lc = select_game(f"Game for {emul['name']}")
        if pid is None: continue
        ro, ri = configure_restart()
        profiles.append({'profile_name':f"Existing-{emul['name']}",'mode':emul['type'],
                         'instance_target':emul,'place_id':pid,'private_server_link_code':lc,
                         'restarter_on':ro,'restarter_interval':ri})
    if pids:
        accounts = load_accounts()
        if not accounts: show_err('No accounts to assign.'); time.sleep(4); return profiles
        for pid in pids:
            draw_header(f'DESKTOP PID {pid}')
            anames = list(accounts.keys())
            ac = get_choice('Assign account', anames+['Skip'])
            if not ac or ac > len(anames): continue
            aname = anames[ac-1]; gid, lc = select_game(f'Game for {aname}')
            if gid is None: continue
            ro, ri = configure_restart()
            profiles.append({'profile_name':f'Existing-{aname}','mode':'desktop','pid':pid,
                             'place_id':gid,'private_server_link_code':lc,'restarter_on':ro,
                             'restarter_interval':ri,'roblox_account_name':aname,'roblox_cookie':accounts[aname]})
    return profiles


# ══════════════════════════════════════════════════════════════
#  LAUNCH MONITOR SESSION
# ══════════════════════════════════════════════════════════════
def launch_monitor_session(profiles):
    global SESSION_STATUS, SESSION_SERVER_INFO, MONITORING_ACTIVE, TOGGLE_REQUESTED
    global EXIT_TO_MENU_REQUESTED, DEBUG_MODE_ACTIVE
    MONITORING_ACTIVE=True; SESSION_STATUS={}; SESSION_SERVER_INFO={}
    TOGGLE_REQUESTED=False; EXIT_TO_MENU_REQUESTED=False; DEBUG_MODE_ACTIVE=False

    if not isinstance(profiles, list): profiles=[profiles]
    ET = {'mumu','ldplayer','bluestacks'}
    ft = {p.get('mode') for p in profiles if p.get('mode') in ET}
    if len(ft) > 1:
        draw_header('LAUNCH ABORTED'); show_err(f'Cannot mix emulator types: {", ".join(ft)}'); pause(); return

    def kb_handler():
        global MONITORING_ACTIVE, TOGGLE_REQUESTED, EXIT_TO_MENU_REQUESTED, DEBUG_MODE_ACTIVE
        while MONITORING_ACTIVE:
            try:
                if msvcrt.kbhit():
                    k = msvcrt.getch()
                    if   k == b'\x14': req_toggle()
                    elif k == b'\x18': req_menu()
                    elif k == b'\x07': req_debug()
                    elif k == b'\x03': MONITORING_ACTIVE=False; break
            except: break
            time.sleep(0.5)

    threading.Thread(target=kb_handler, daemon=True).start()

    draw_header('SESSION DURATION')
    cfg   = load_config(); dur_h = cfg.get('settings',{}).get('session_duration',0)
    print_centered(colorize(f'Default: {"Unlimited" if dur_h==0 else str(dur_h)+"h"}', C.GREEN))
    if get_input('Override? (y/n)').lower() == 'y':
        while True:
            try:
                dur_h = float(get_input('Hours (0=unlimited)'))
                if dur_h >= 0: break
            except: show_err('Invalid.')

    start = time.time()
    if dur_h > 0: threading.Thread(target=end_after, args=(dur_h,), daemon=True).start()

    write_log(f'Session started with {len(profiles)} profile(s).','INFO')

    game_names = list({str(p.get('place_id','?')) for p in profiles})
    if _wh('session_start'): webhook.session_start(len(profiles), ', '.join(game_names[:3]))

    all_pids = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
    desk_count = 0
    _session_start = time.time()

    for profile in profiles:
        mode = profile['mode']
        if mode in ET:
            emul = profile.get('instance_target')
            if not emul: continue
            sk=str(emul['port']); inst=emul['name']
        elif mode == 'desktop':
            desk_count += 1
            inst = profile.get('roblox_account_name', f'Desktop {desk_count}')
            sk   = f'desk_{inst}_{profile.get("pid", desk_count)}'
        else: continue

        with STATUS_LOCK:
            SESSION_STATUS[sk] = {
                'profile_name':      profile.get('profile_name','?'),
                'instance_name':     inst,
                'status':            colorize('Initializing ...', C.YELLOW),
                'crashes':           -1,
                'restarts':          0,
                'restarter_enabled': profile.get('restarter_on', False),
                'next_restart_time': 0,
                'start_time':        time.time(),
                'pid':               profile.get('pid'),
                'place_id':          profile.get('place_id'),
                'roblox_account_name': profile.get('roblox_account_name',''),
                'user_id':           _uid_cache.get(profile.get('roblox_account_name',''),''),
                'force_rejoin':      False,
            }

        if mode in ET:
            adb = get_adb(mode)
            if not adb: log_event(f'No ADB for {mode}.','ERROR',sk); continue
            threading.Thread(target=monitor_emulator, args=(sk,adb,profile), daemon=True).start()
        elif mode == 'desktop' and DESKTOP_MODE_ENABLED:
            if 'pid' in profile and profile['pid'] is not None:
                threading.Thread(target=monitor_desktop, args=(profile,sk), daemon=True).start()
            else:
                launch_roblox(profile, sk); new_pid=find_new_pid(all_pids)
                if not new_pid: log_event(f'No process for {inst}.','ERROR',sk); continue
                all_pids.add(new_pid); profile['pid']=new_pid
                with STATUS_LOCK:
                    if sk in SESSION_STATUS: SESSION_STATUS[sk]['pid']=new_pid
                threading.Thread(target=monitor_desktop, args=(profile,sk), daemon=True).start()

        if profile.get('restarter_on') and profile.get('restarter_interval',0) > 0:
            ri = profile['restarter_interval']
            if mode == 'desktop':
                threading.Thread(target=restarter_desktop, args=(profile,ri,sk), daemon=True).start()
            else:
                adb = get_adb(mode)
                threading.Thread(target=restarter_emulator, args=(sk,ri,profile,adb), daemon=True).start()

        delay = load_config().get('settings',{}).get('launch_delay',5)
        print_centered(colorize(f'  Waiting {delay}s before next instance ...', C.DIM))
        time.sleep(delay)

    # Register and start Discord commands now that sessions are live
    register_discord_commands()

    try:
        while MONITORING_ACTIVE:
            if EXIT_TO_MENU_REQUESTED:   MONITORING_ACTIVE=False; continue
            if TOGGLE_REQUESTED:         handle_toggle_menu(); TOGGLE_REQUESTED=False
            if DEBUG_MODE_ACTIVE:        draw_debug()
            else:                        draw_dashboard(start, dur_h)
            time.sleep(1.5)
    except KeyboardInterrupt:
        MONITORING_ACTIVE=False; show_warn('\nStopped by user.'); time.sleep(2); sys.exit(0)
    except Exception as e:
        MONITORING_ACTIVE=False; show_err(f'Dashboard error: {e}'); pause()
    finally:
        if webhook: webhook.stop_command_listener()
        uptime = time.time()-_session_start
        with STATUS_LOCK:
            tot_c = sum(d.get('crashes',0) for d in SESSION_STATUS.values() if d.get('crashes',0)>=0)
            tot_r = sum(d.get('restarts',0) for d in SESSION_STATUS.values())
        if _wh('session_stop'): webhook.session_stop(uptime, tot_c, tot_r)
        EXIT_TO_MENU_REQUESTED=False; TOGGLE_REQUESTED=False
        write_log('Session ended.','INFO')


# ══════════════════════════════════════════════════════════════
#  SERVER HOPPER
# ══════════════════════════════════════════════════════════════
SERVER_HOP_ACTIVE = False
SERVER_HOP_THREAD = None

def get_server_list(place_id, cookie, max_servers=30):
    servers=[]; cursor=''
    headers={'Cookie':f'.ROBLOSECURITY={clean_cookie(cookie)}','User-Agent':'Mozilla/5.0'}
    try:
        for _ in range(3):
            url = (f'https://games.roblox.com/v1/games/{place_id}/servers/Public'
                   f'?sortOrder=Asc&limit=100{"&cursor="+cursor if cursor else ""}')
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200: break
            data = r.json()
            for sv in data.get('data',[]):
                if sv.get('playing',0) < sv.get('maxPlayers',99):
                    servers.append({'id':sv['id'],'playing':sv.get('playing',0),
                                    'max':sv.get('maxPlayers',99),'fps':sv.get('fps',0),'ping':sv.get('ping',0)})
            cursor = data.get('nextPageCursor') or ''
            if not cursor or len(servers) >= max_servers: break
            time.sleep(0.3)
    except Exception as e: log_event(f'Server list error: {e}','ERROR','serverhopper')
    return servers

def join_server_by_id(place_id, server_id, cookie):
    try:
        ticket = get_auth_ticket(clean_cookie(cookie))
        if not ticket: return False
        btid = random.randint(100000000000,999999999999)
        pl   = (f'https%3A%2F%2Fassetgame.roblox.com%2Fgame%2FPlaceLauncher.ashx%3Frequest%3D'
                f'RequestGameJob%26browserTrackerId%3D{btid}%26placeId%3D{place_id}'
                f'%26gameId%3D{server_id}%26isPlayTogetherGame%3Dfalse')
        url  = (f'roblox-player:1+launchmode:play+gameinfo:{ticket}'
                f'+launchtime:{int(time.time()*1000)}+placelauncherurl:{pl}'
                f'+browsertrackerid:{btid}+robloxLocale:en_us+gameLocale:en_us+channel:+LaunchExp:InApp')
        os.startfile(url); return True
    except Exception as e:
        log_event(f'Join server error: {e}','ERROR','serverhopper'); return False

def _kill_roblox_by_pid(pid):
    try:
        if pid and psutil.pid_exists(pid): psutil.Process(pid).kill(); time.sleep(2)
    except: pass

def server_hopper_session(place_id, cookie, interval_mins, filter_opt, account_name=''):
    global SERVER_HOP_ACTIVE
    log_event(f'Server hopper started (every {interval_mins}m)','INFO','serverhopper')
    hop_count=0; current_pid=None
    while SERVER_HOP_ACTIVE:
        wait_secs = int(interval_mins*60)
        for _ in range(wait_secs):
            if not SERVER_HOP_ACTIVE: return
            time.sleep(1)
        if not SERVER_HOP_ACTIVE: return
        log_event(f'Hop #{hop_count+1} — fetching servers...','INFO','serverhopper')
        servers = get_server_list(place_id, cookie)
        if not servers: log_event('No servers found.','WARN','serverhopper'); continue
        if filter_opt == 'least_players': servers.sort(key=lambda s: s['playing'])
        elif filter_opt == 'most_players': servers.sort(key=lambda s: s['playing'], reverse=True)
        elif filter_opt == 'best_fps':    servers.sort(key=lambda s: s.get('fps',0), reverse=True)
        elif filter_opt == 'lowest_ping': servers.sort(key=lambda s: s.get('ping',9999))
        else: random.shuffle(servers)
        target = servers[0]
        log_event(f"Hopping to {target['id'][:12]} ({target['playing']}/{target['max']} players)",'INFO','serverhopper')
        _kill_roblox_by_pid(current_pid); time.sleep(1)
        existing = {p.pid for p in psutil.process_iter(['name']) if p.info['name']==ROBLOX_PROCESS_NAME}
        ok = join_server_by_id(place_id, target['id'], cookie)
        if ok:
            hop_count += 1; increment_stat('total_hops')
            log_event(f'Hop #{hop_count} launched!','SUCCESS','serverhopper')
            if _wh('server_hop'):
                webhook.server_hop(account_name, hop_count, target['playing'], target['max'], target['id'])
            new_pid = find_new_pid(existing)
            if new_pid: current_pid=new_pid
        else:
            log_event('Hop failed.','ERROR','serverhopper')
    log_event('Server hopper stopped.','INFO','serverhopper')

def server_hopper_menu():
    global SERVER_HOP_ACTIVE, SERVER_HOP_THREAD
    while True:
        draw_header('SERVER HOPPER','Auto-hop to a new server on a timer')
        if SERVER_HOP_ACTIVE:
            if RICH_ENABLED: rich_console.print(Align.center('[bold bright_green]● SERVER HOPPER IS RUNNING[/bold bright_green]'))
            else: print_centered(colorize('● SERVER HOPPER IS RUNNING',C.GREEN+C.BOLD))
            print()
        opts = ['▶   Start Server Hopper','⏹   Stop Server Hopper','ℹ   What is Server Hopper?','⬅   Back']
        ch   = get_choice('Option', opts)
        if ch == 1:
            if SERVER_HOP_ACTIVE: show_warn('Already running.'); time.sleep(2); continue
            accounts = load_accounts()
            if not accounts: show_err('No accounts saved.'); time.sleep(3); continue
            names = list(accounts.keys()); ac = get_choice('Account to use', names+['Cancel'])
            if not ac or ac > len(names): continue
            acc_name=names[ac-1]; cookie=accounts[acc_name]
            pid, _ = select_game('SERVER HOPPER — SELECT GAME')
            if pid is None: continue
            draw_header('SERVER HOP INTERVAL')
            interval_opts = ['10 minutes','20 minutes','30 minutes','45 minutes',
                             '1 hour','2 hours','3 hours','Custom','Cancel']
            interval_map  = {1:10,2:20,3:30,4:45,5:60,6:120,7:180}
            ich = get_choice('Hop every ...', interval_opts)
            if not ich or ich == 9: continue
            if ich == 8:
                while True:
                    try:
                        mins=float(get_input('Custom interval in minutes (min 5)'))
                        if mins >= 5: interval=mins; break
                        show_err('Minimum 5 minutes.')
                    except ValueError: show_err('Invalid.')
            elif ich in interval_map: interval=interval_map[ich]
            else: continue
            draw_header('SERVER FILTER')
            filter_map = {1:'random',2:'least_players',3:'most_players',4:'best_fps',5:'lowest_ping'}
            fch = get_choice('Server preference',['Random server','Fewest players','Most players','Best FPS','Lowest ping'])
            filter_opt = filter_map.get(fch or 1, 'random')
            draw_header('CONFIRM')
            show_info(f'Account:  {acc_name}'); show_info(f'Place ID: {pid}')
            show_info(f'Interval: {interval} minutes'); show_info(f'Filter:   {filter_opt.replace("_"," ").title()}')
            print(); show_warn('Roblox will relaunch on each hop.'); print()
            if get_input('Start? (y/n)').lower() != 'y': continue
            show_info('Joining initial server ...')
            servers = get_server_list(pid, cookie)
            if servers:
                random.shuffle(servers); ok=join_server_by_id(pid,servers[0]['id'],cookie)
                if ok: show_ok('Launched! Hopper activates after first interval.')
                else:  show_warn('Initial join failed. Hopper will still run.')
            else:
                show_warn('No servers found. Hopper will try when timer hits.')
            SERVER_HOP_ACTIVE=True
            with STATUS_LOCK:
                SESSION_STATUS['serverhopper'] = {
                    'profile_name':'Server Hopper','instance_name':f'Hopper — {acc_name}',
                    'status':colorize('● Waiting for next hop',C.CYAN),'crashes':0,'restarts':0,
                    'restarter_enabled':False,'next_restart_time':time.time()+interval*60,'start_time':time.time(),
                    'place_id':pid,
                }
            SERVER_HOP_THREAD = threading.Thread(
                target=server_hopper_session,
                args=(pid, cookie, interval, filter_opt, acc_name), daemon=True,
            )
            SERVER_HOP_THREAD.start(); show_ok(f'Server Hopper started! Next hop in {interval} min.'); time.sleep(2)
        elif ch == 2:
            if not SERVER_HOP_ACTIVE: show_warn('Not running.')
            else:
                SERVER_HOP_ACTIVE=False
                with STATUS_LOCK: SESSION_STATUS.pop('serverhopper',None)
                show_ok('Server Hopper stopped.')
            time.sleep(2)
        elif ch == 3:
            draw_header('WHAT IS SERVER HOPPER?')
            if RICH_ENABLED:
                rich_console.print(Panel(
                    "[bold]Server Hopper[/bold] automatically moves you to a new Roblox server on a timer.\n\n"
                    "[bold]Why use it?[/bold]\n  • Find less-lagged servers\n  • Avoid stale long sessions\n"
                    "  • Stay on active servers\n  • Works while you AFK farm\n\n"
                    "[bold]How it works:[/bold]\n  1. Fetches live servers from Roblox API\n"
                    "  2. Picks a server matching your filter\n  3. Closes Roblox and relaunches\n"
                    "  4. Repeats every X minutes",
                    title='ℹ Info', box=box.ROUNDED, style='bright_blue', padding=(1,3)
                ))
            pause()
        elif ch == 4: return


# ══════════════════════════════════════════════════════════════
#  WEBHOOK SETTINGS MENU  (improved — with commands section)
# ══════════════════════════════════════════════════════════════
def webhook_settings_menu():
    while True:
        cfg = load_config(); ws = cfg.get('settings',{}).get('webhook',{})
        draw_header('DISCORD WEBHOOK','Your webhook — live notifications + remote commands')

        if RICH_ENABLED:
            enabled     = ws.get('enabled', False)
            url_display = ws.get('url','(not set)')
            if len(url_display) > 55: url_display = url_display[:55]+'...'
            cmds_on     = ws.get('commands_enabled', False)
            bot_set     = bool(ws.get('bot_token'))
            chan_set     = bool(ws.get('command_channel_id'))

            # Status panel
            status_color = 'bright_green' if enabled else 'red'
            status_text  = '● Enabled' if enabled else '○ Disabled'
            cmd_text     = (f'[bright_magenta]⌨ Commands: ON[/bright_magenta]'
                            if cmds_on else '[dim]⌨ Commands: OFF[/dim]')
            rich_console.print(Panel(
                Align.center(
                    f"[{status_color}]{status_text}[/{status_color}]   {cmd_text}\n"
                    f"[dim]URL: {url_display if url_display != '(not set)' else '[red](not set)[/red]'}[/dim]\n"
                    f"[dim]Bot Token: {'✓ Set' if bot_set else '✗ Not set'}   "
                    f"Channel ID: {'✓ Set' if chan_set else '✗ Not set'}[/dim]"
                ),
                title='[bold]Webhook Status[/bold]', box=box.ROUNDED,
                style='bright_blue' if enabled else 'dim', padding=(0,4),
            ))
            rich_console.print()

            # Commands help panel
            if RICH_ENABLED:
                rich_console.print(Panel(
                    "[bold bright_magenta]Available Discord Commands:[/bold bright_magenta]\n\n"
                    "  [bold].rejoin[/bold] [instance_name]   — Force-rejoin all or named instance\n"
                    "  [bold].join[/bold] <serverID|placeID> [account] — Join a specific server or game\n"
                    "  [bold].getid[/bold]                    — Get server ID, place ID, user ID for all sessions\n\n"
                    "[dim]Requires: Bot Token + Command Channel ID to be set below.[/dim]",
                    title='[bold]Commands[/bold]', box=box.SIMPLE, style='bright_magenta', padding=(0,3),
                ))
                rich_console.print()

            # Events table
            events = ws.get('events', _DEFAULT_CONFIG['settings']['webhook']['events'])
            tbl = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {current_theme['primary']}")
            tbl.add_column('Event',   style='white', width=26)
            tbl.add_column('Status',  justify='center', width=8)
            tbl.add_column('Event',   style='white', width=26)
            tbl.add_column('Status',  justify='center', width=8)
            ev_list = list(events.items()); half = (len(ev_list)+1)//2
            for i in range(half):
                k1, v1 = ev_list[i]
                if i+half < len(ev_list):
                    k2, v2 = ev_list[i+half]
                    tbl.add_row(
                        k1.replace('_',' ').title(),
                        '[bright_green]ON[/bright_green]' if v1 else '[red]OFF[/red]',
                        k2.replace('_',' ').title(),
                        '[bright_green]ON[/bright_green]' if v2 else '[red]OFF[/red]',
                    )
                else:
                    tbl.add_row(k1.replace('_',' ').title(),
                                '[bright_green]ON[/bright_green]' if v1 else '[red]OFF[/red]','','')
            rich_console.print(tbl); rich_console.print()

        opts = [
            '🔌  Enable / Disable Webhook',
            '🔗  Set / Change Webhook URL',
            '🔀  Toggle Individual Events',
            '🔔  Send Test Message',
            '—  ─── Discord Commands ───',
            '⌨   Enable / Disable Commands',
            '🤖  Set Bot Token',
            '📢  Set Command Channel ID',
            '🧪  Test Commands',
            '⬅   Back',
        ]
        ch = get_choice('Select option', opts)
        if ch == 1:
            ws['enabled'] = not ws.get('enabled', False)
            cfg['settings']['webhook'] = ws; save_config(cfg)
            init_webhook(cfg)
            show_ok(f"Webhook {'enabled ✓' if ws['enabled'] else 'disabled'}."); time.sleep(1.5)
        elif ch == 2:
            draw_header('SET WEBHOOK URL')
            show_info('Go to Discord channel → Edit → Integrations → Webhooks → Copy URL')
            rich_console.print()
            new_url = get_input('Paste Webhook URL').strip()
            if new_url.startswith('https://discord.com/api/webhooks/'):
                ws['url'] = new_url; cfg['settings']['webhook']=ws; save_config(cfg)
                init_webhook(cfg); show_ok('Webhook URL saved!')
                # Auto-enable
                if not ws.get('enabled'):
                    if get_input('Enable webhook now? (y/n)').lower()=='y':
                        ws['enabled']=True; cfg['settings']['webhook']=ws; save_config(cfg); init_webhook(cfg)
                        show_ok('Webhook enabled.')
            else:
                show_err('Invalid URL — must start with https://discord.com/api/webhooks/')
            time.sleep(2)
        elif ch == 3:
            _toggle_webhook_events(cfg, ws)
        elif ch == 4:
            draw_header('SEND TEST MESSAGE')
            if not ws.get('url'):
                show_err('No webhook URL set. Use option 2 first.'); pause(); continue
            show_info('Sending test embed ...')
            ok, msg = webhook.test_connection() if webhook else (False, 'Webhook not initialised')
            if ok: show_ok(msg)
            else:  show_err(msg)
            pause()
        elif ch == 5:
            pass  # section header
        elif ch == 6:
            ws['commands_enabled'] = not ws.get('commands_enabled', False)
            cfg['settings']['webhook']=ws; save_config(cfg)
            show_ok(f"Commands {'enabled' if ws['commands_enabled'] else 'disabled'}.")
            if ws['commands_enabled'] and (not ws.get('bot_token') or not ws.get('command_channel_id')):
                show_warn('Remember to set Bot Token and Channel ID for commands to work.')
            time.sleep(2)
        elif ch == 7:
            draw_header('BOT TOKEN SETUP')
            if RICH_ENABLED:
                rich_console.print(Panel(
                    "[bold]How to get a Bot Token:[/bold]\n\n"
                    "1. Go to [link=https://discord.com/developers/applications]discord.com/developers/applications[/link]\n"
                    "2. Create a new application → Bot tab → Reset Token → Copy\n"
                    "3. Invite bot to your server with [bold]Read Messages[/bold] permission\n\n"
                    "[dim]The bot token lets Pulse read messages in your command channel.[/dim]",
                    box=box.ROUNDED, style='bright_blue', padding=(1,3)
                ))
                rich_console.print()
            tok = get_input('Paste Bot Token (or blank to clear)').strip()
            if tok:
                ws['bot_token']=tok; cfg['settings']['webhook']=ws; save_config(cfg)
                init_webhook(cfg); show_ok('Bot token saved.')
            else:
                ws['bot_token']=''; cfg['settings']['webhook']=ws; save_config(cfg)
                show_ok('Bot token cleared.')
            time.sleep(2)
        elif ch == 8:
            draw_header('COMMAND CHANNEL ID')
            if RICH_ENABLED:
                rich_console.print(Panel(
                    "[bold]How to get a Channel ID:[/bold]\n\n"
                    "1. Enable Developer Mode in Discord (Settings → Advanced)\n"
                    "2. Right-click the channel → Copy Channel ID\n"
                    "3. Paste it below\n\n"
                    "[dim]This is the channel where you type .rejoin / .join / .getid[/dim]",
                    box=box.ROUNDED, style='bright_blue', padding=(1,3)
                ))
                rich_console.print()
            cid = get_input('Paste Channel ID (or blank to clear)').strip()
            if cid:
                ws['command_channel_id']=cid; cfg['settings']['webhook']=ws; save_config(cfg)
                init_webhook(cfg); show_ok(f'Channel ID set: {cid}')
            else:
                ws['command_channel_id']=''; cfg['settings']['webhook']=ws; save_config(cfg)
                show_ok('Channel ID cleared.')
            time.sleep(2)
        elif ch == 9:
            draw_header('TEST COMMANDS')
            if RICH_ENABLED:
                rich_console.print(Panel(
                    "[bold]Command Reference:[/bold]\n\n"
                    "[bold bright_magenta].rejoin[/bold bright_magenta]\n"
                    "  Rejoins all active instances immediately.\n"
                    "  Usage: [dim].rejoin[/dim]  or  [dim].rejoin MyInstance[/dim]\n\n"
                    "[bold bright_magenta].join <serverID|placeID>[/bold bright_magenta]\n"
                    "  Join a specific server by server UUID, or a game by place ID.\n"
                    "  Usage: [dim].join 606849621[/dim]  or  [dim].join abc123-def456-...[/dim]\n"
                    "  Optional: [dim].join 606849621 MyAccountName[/dim]\n\n"
                    "[bold bright_magenta].getid[/bold bright_magenta]\n"
                    "  Returns server ID, place ID, and user ID for all active sessions.\n"
                    "  Usage: [dim].getid[/dim]\n\n"
                    "[dim]Type these in your configured command channel while a session is running.[/dim]",
                    title='[bold]Command Help[/bold]', box=box.ROUNDED, style='bright_magenta', padding=(1,3)
                ))
            # Simulate .getid locally
            result = _cmd_getid([], 'LocalTest')
            show_info('Simulated .getid output:')
            rich_console.print(f'  [dim]{result}[/dim]')
            pause()
        elif ch == 10:
            return

def _toggle_webhook_events(cfg, ws):
    draw_header('TOGGLE WEBHOOK EVENTS')
    events  = ws.setdefault('events', dict(_DEFAULT_CONFIG['settings']['webhook']['events']))
    ev_keys = list(events.keys())
    while True:
        opts = [f"{'✓' if events[k] else '✗'}  {k.replace('_',' ').title()}" for k in ev_keys] + ['Done']
        ch   = get_choice('Toggle event', opts)
        if not ch or ch > len(ev_keys): break
        key = ev_keys[ch-1]; events[key]=not events[key]
        ws['events']=events; cfg['settings']['webhook']=ws; save_config(cfg)
        show_ok(f"{key.replace('_',' ').title()}: {'ON' if events[key] else 'OFF'}")
        time.sleep(0.8)


# ══════════════════════════════════════════════════════════════
#  TITAN MENU
# ══════════════════════════════════════════════════════════════
def titan_menu():
    while True:
        draw_header('TITAN','Spoofer & Configuration Tools')
        avail=titan_is_available(); running=titan_is_running()
        if RICH_ENABLED:
            if not avail:
                status_txt = f"[red]✗  Titan.exe not found[/red]\n[dim]Expected: {TITAN_EXE}[/dim]"
            elif running:
                status_txt = f"[bright_green]● Titan RUNNING  (PID {TITAN_PROC.pid})[/bright_green]"
            else:
                status_txt = "[dim]○  Titan stopped[/dim]"
            dll_txt = ('[bright_green]✓  TITAN.dll found[/bright_green]' if os.path.exists(TITAN_DLL)
                       else '[yellow]⚠  TITAN.dll not found[/yellow]')
            rich_console.print(Panel(
                Align.center(f"{status_txt}\n{dll_txt}"),
                title='[bold]Titan Status[/bold]', box=box.ROUNDED,
                style='bright_blue' if running else ('red' if not avail else 'dim'), padding=(0,4),
            ))
            rich_console.print()
        opts = ['▶   Start Titan','⏹   Stop Titan','🔄  Restart Titan',
                '📋  Check Status','📁  Open Titan Folder','⬅   Back']
        ch = get_choice('Select option', opts)
        if ch == 1:
            draw_header('START TITAN')
            if not avail: show_err(f'Titan.exe not found.'); pause(); continue
            ok, msg = titan_launch()
            if ok: show_ok(msg)
            else:  show_err(msg)
            pause()
        elif ch == 2:
            draw_header('STOP TITAN'); ok,msg=titan_stop()
            if ok: show_ok(msg)
            else:  show_warn(msg)
            pause()
        elif ch == 3:
            draw_header('RESTART TITAN')
            if titan_is_running(): show_info('Stopping ...'); titan_stop(); time.sleep(1.5)
            if not avail: show_err('Titan.exe not found.'); pause(); continue
            ok,msg=titan_launch()
            if ok: show_ok(msg)
            else:  show_err(msg)
            pause()
        elif ch == 4:
            draw_header('TITAN STATUS')
            show_info(f'Titan.exe:  {"Found ✓" if avail else "NOT FOUND ✗"}')
            show_info(f'Path:       {TITAN_EXE}')
            show_info(f'TITAN.dll:  {"Found ✓" if os.path.exists(TITAN_DLL) else "NOT FOUND ✗"}')
            show_info(f'Running:    {"Yes — PID "+str(TITAN_PROC.pid) if running else "No"}')
            if avail: show_info(f'File size:  {os.path.getsize(TITAN_EXE)//1024} KB')
            pause()
        elif ch == 5:
            try: os.startfile(_TITAN_BASE); show_ok(f'Opened: {_TITAN_BASE}')
            except Exception as e: show_err(f'{e}')
            time.sleep(2)
        elif ch == 6: return


# ══════════════════════════════════════════════════════════════
#  SETTINGS MENU
# ══════════════════════════════════════════════════════════════
def settings_menu():
    while True:
        cfg=load_config(); apply_theme(cfg); draw_header('SETTINGS')
        s = cfg.get('settings',{})
        if RICH_ENABLED:
            tbl = Table(box=box.ROUNDED, show_header=False, expand=False)
            tbl.add_column('Setting', style='dim', width=32)
            tbl.add_column('Value',   style='white')
            tbl.add_row('Theme',             s.get('theme','Pulse'))
            tbl.add_row('Freeze Detection',  'On' if s.get('freeze_detection',{}).get('enabled') else 'Off')
            tbl.add_row('Freeze Interval',   f"{s.get('freeze_detection',{}).get('interval',30)}s")
            tbl.add_row('Scheduled Restart', 'On' if s.get('scheduled_restart',{}).get('enabled') else 'Off')
            tbl.add_row('Restart Interval',  f"{s.get('scheduled_restart',{}).get('interval',60)}m")
            tbl.add_row('Session Duration',  'Unlimited' if s.get('session_duration',0)==0 else f"{s.get('session_duration')}h")
            tbl.add_row('Launch Delay',      f"{s.get('launch_delay',5)}s between instances")
            tbl.add_row('Max Crashes/Stop',  'Disabled' if s.get('max_crashes_before_stop',0)==0 else str(s.get('max_crashes_before_stop')))
            tbl.add_row('Webhook',           '[bright_green]Enabled[/bright_green]' if s.get('webhook',{}).get('enabled',False) else '[dim]Disabled[/dim]')
            tbl.add_row('Commands',          '[bright_magenta]Enabled[/bright_magenta]' if s.get('webhook',{}).get('commands_enabled',False) else '[dim]Disabled[/dim]')
            tbl.add_row('Crash Sound',       'Yes' if s.get('crash_sound') else 'No')
            rich_console.print(tbl)
        print()
        opts = ['🎨  Change Theme','❄   Freeze Detection Defaults','🔄  Scheduled Restart Defaults',
                '⏱   Session Duration Default','⏳  Launch Delay Between Instances',
                '🛑  Max Crashes Before Stop','🔊  Crash Sound Alert',
                '📡  Discord Webhook & Commands',
                '💾  Backup Config','♻   Restore Config from Backup','🔁  Reset All Settings','⬅   Back']
        ch = get_choice('Setting', opts)
        if ch == 1:
            tch = get_choice('Theme', list(RICH_THEMES.keys())+['Cancel'])
            if tch and tch <= len(RICH_THEMES):
                cfg['settings']['theme']=list(RICH_THEMES.keys())[tch-1]
                save_config(cfg); apply_theme(cfg)
                show_ok(f"Theme set to '{cfg['settings']['theme']}'."); time.sleep(1)
        elif ch == 2:  _settings_freeze(cfg)
        elif ch == 3:  _settings_restart(cfg)
        elif ch == 4:  _settings_duration(cfg)
        elif ch == 5:
            try:
                v = int(get_input('Delay in seconds (1-30)'))
                if 1 <= v <= 30: cfg['settings']['launch_delay']=v; save_config(cfg); show_ok(f'Delay set to {v}s.')
                else: show_err('Must be 1-30.')
            except ValueError: show_err('Invalid.')
            time.sleep(1.5)
        elif ch == 6:
            try:
                v = int(get_input('Max crashes (0 = unlimited)'))
                if v >= 0:
                    cfg['settings']['max_crashes_before_stop']=v; save_config(cfg)
                    show_ok(f'Set to {"unlimited" if v==0 else v}.')
                else: show_err('Must be 0 or more.')
            except ValueError: show_err('Invalid.')
            time.sleep(1.5)
        elif ch == 7:
            cfg['settings']['crash_sound']=not s.get('crash_sound',False); save_config(cfg)
            show_ok(f"Crash sound: {'ON' if cfg['settings']['crash_sound'] else 'OFF'}."); time.sleep(1.5)
        elif ch == 8:  webhook_settings_menu()
        elif ch == 9:  _backup_config()
        elif ch == 10: _restore_config()
        elif ch == 11: _reset_settings(cfg)
        elif ch == 12: return

def _settings_freeze(cfg):
    draw_header('FREEZE DETECTION')
    opts=['Enable by Default','Disable by Default','Set Interval (10-120s)','Back']
    ch = get_choice('Option', opts)
    if ch==1:   cfg.setdefault('settings',{}).setdefault('freeze_detection',{})['enabled']=True
    elif ch==2: cfg.setdefault('settings',{}).setdefault('freeze_detection',{})['enabled']=False
    elif ch==3:
        try:
            v=int(get_input('Seconds (10-120)'))
            if 10<=v<=120: cfg.setdefault('settings',{}).setdefault('freeze_detection',{})['interval']=v
            else: show_err('Must be 10-120.'); time.sleep(1.5); return
        except ValueError: show_err('Invalid.'); time.sleep(1.5); return
    save_config(cfg); show_ok('Saved.'); time.sleep(1.5)

def _settings_restart(cfg):
    draw_header('SCHEDULED RESTART DEFAULTS')
    opts=['Enable by Default','Disable by Default','30s (testing)','20m','30m','45m','1h','2h','4h','6h','Custom','Back']
    ch=get_choice('Option', opts); rd=cfg.setdefault('settings',{}).setdefault('scheduled_restart',{})
    if ch==1:   rd['enabled']=True
    elif ch==2: rd['enabled']=False
    else:
        m={3:0.5,4:20,5:30,6:45,7:60,8:120,9:240,10:360}
        if ch in m: rd['interval']=m[ch]
        elif ch==11:
            try:
                v=float(get_input('Minutes (min 20)'))
                if v>=20: rd['interval']=v
                else: show_err('Min 20.'); time.sleep(1.5); return
            except ValueError: show_err('Invalid.'); time.sleep(1.5); return
        else: return
    save_config(cfg); show_ok('Saved.'); time.sleep(1.5)

def _settings_duration(cfg):
    draw_header('SESSION DURATION DEFAULT')
    opts=['Unlimited','30s (test)','20m','30m','1h','2h','4h','8h','12h','24h','Custom','Back']
    ch=get_choice('Option',opts); m={1:0,2:30/3600,3:20/60,4:30/60,5:1,6:2,7:4,8:8,9:12,10:24}
    sd=cfg.setdefault('settings',{})
    if ch in m: sd['session_duration']=m[ch]
    elif ch==11:
        try:
            v=float(get_input('Minutes (0=unlimited)'))
            if v<0: show_err('Negative.'); time.sleep(1.5); return
            if 0<v<20: show_err('Min 20m or 0.'); time.sleep(2); return
            sd['session_duration']=0 if v==0 else v/60
        except ValueError: show_err('Invalid.'); time.sleep(1.5); return
    else: return
    save_config(cfg); show_ok('Saved.'); time.sleep(1.5)

def _backup_config():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    dst=os.path.join(BACKUP_DIR, f'pulse_config_{ts}.json')
    try:   shutil.copy2(CONFIG_FILE, dst); show_ok(f'Backup saved: {dst}')
    except Exception as e: show_err(f'Backup failed: {e}')
    pause()

def _restore_config():
    if not os.path.exists(BACKUP_DIR): show_err('No backups found.'); pause(); return
    files=sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.json')], reverse=True)
    if not files: show_err('No backups.'); pause(); return
    ch=get_choice('Restore which backup', files[:10]+['Cancel'])
    if ch and ch <= len(files[:10]):
        src=os.path.join(BACKUP_DIR, files[ch-1])
        try:   shutil.copy2(src, CONFIG_FILE); show_ok(f'Restored from {files[ch-1]}.')
        except Exception as e: show_err(f'Restore failed: {e}')
    pause()

def _reset_settings(cfg):
    if get_input('Reset ALL settings? (y/n)', C.RED).lower() == 'y':
        cfg['settings']=_DEFAULT_CONFIG['settings'].copy(); save_config(cfg); apply_theme(cfg)
        init_webhook(cfg); show_ok('Settings reset to defaults.')
    else: show_info('Cancelled.')
    time.sleep(1.5)


# ══════════════════════════════════════════════════════════════
#  UTILITIES MENU
# ══════════════════════════════════════════════════════════════
def utilities_menu():
    while True:
        draw_header('UTILITIES','Extra tools & diagnostics')
        opts = [
            '📋  Notes / Session Scratch Pad',
            '📊  System Info',
            '📈  Lifetime Stats',
            '🔍  Find Roblox Process Info',
            '🧹  Kill All Roblox Instances',
            '📁  Open Config Folder',
            '📝  View Session Log',
            '🗑   Clear Session Log',
            '🌐  Ping Roblox Servers',
            '🖥   Disk Space',
            '🔄  Restart as Administrator',
            '💾  Backup Config',
            '⚙   Restore Config',
            '🔑  View Cookies (masked)',
            '📶  Check Internet',
            '🕐  System Clock Check',
            '🧪  Test ADB Connection',
            '📤  Export Config as Text',
            '🗂   Open Log Folder',
            '🔢  Count Active Sessions',
            '🧼  Clear Lifetime Stats',
            '📡  Test Discord Webhook',
            '🌐  Get Server Info (all sessions)',
            '⬅   Back',
        ]
        ch=get_choice('Select utility', opts)
        if ch ==  1: _notes()
        elif ch ==  2: _sysinfo()
        elif ch ==  3: _lifetime_stats()
        elif ch ==  4: _roblox_process_info()
        elif ch ==  5: _kill_roblox()
        elif ch ==  6: _open_config_folder()
        elif ch ==  7: _view_log()
        elif ch ==  8: _clear_log()
        elif ch ==  9: _ping_roblox()
        elif ch == 10: _disk_space()
        elif ch == 11: _restart_as_admin()
        elif ch == 12: _backup_config()
        elif ch == 13: _restore_config()
        elif ch == 14: _view_cookies_masked()
        elif ch == 15: _check_internet()
        elif ch == 16: _clock_check()
        elif ch == 17: _test_adb()
        elif ch == 18: _export_config()
        elif ch == 19: _open_log_folder()
        elif ch == 20: _count_sessions()
        elif ch == 21: _clear_stats()
        elif ch == 22:
            draw_header('TEST DISCORD WEBHOOK')
            show_info('Sending test embed ...')
            ok, msg = webhook.test_connection() if webhook else (False, 'Webhook not initialised')
            if ok: show_ok(msg)
            else:  show_err(msg)
            pause()
        elif ch == 23:
            accounts = load_accounts()
            _push_server_info_to_webhook(accounts, list(accounts.keys()))
        elif ch == 24: return

def _notes():
    draw_header('NOTES / SCRATCH PAD')
    existing=''
    if os.path.exists(NOTES_FILE):
        with open(NOTES_FILE,'r',encoding='utf-8') as f: existing=f.read()
    if existing: print_centered(colorize('Current notes:',C.CYAN)); print(); print(existing); print()
    print_centered(colorize("Type new note + Enter. 'CLEAR' to delete. Empty to keep.",C.DIM))
    note=input(f'{C.DIM}  > {C.RESET}')
    if note.strip().upper()=='CLEAR':
        if os.path.exists(NOTES_FILE): os.remove(NOTES_FILE)
        show_ok('Notes cleared.')
    elif note.strip():
        with open(NOTES_FILE,'a',encoding='utf-8') as f:
            f.write(f'[{datetime.datetime.now():%Y-%m-%d %H:%M}] {note.strip()}\n')
        show_ok('Note saved.')
    time.sleep(1.5)

def _sysinfo():
    draw_header('SYSTEM INFORMATION')
    if RICH_ENABLED:
        tbl=Table(box=box.ROUNDED, show_header=False, expand=False)
        tbl.add_column('Key', style='dim', width=28); tbl.add_column('Value', style='white')
        try:
            cpu_pct=psutil.cpu_percent(interval=1); mem=psutil.virtual_memory()
            disk=psutil.disk_usage(os.getcwd())
            tbl.add_row('OS',          platform.system()+' '+platform.release())
            tbl.add_row('CPU',         platform.processor()[:50])
            tbl.add_row('CPU Usage',   f'{cpu_pct}%')
            tbl.add_row('RAM Total',   f'{mem.total//1024//1024:,} MB')
            tbl.add_row('RAM Used',    f'{mem.used//1024//1024:,} MB ({mem.percent}%)')
            tbl.add_row('RAM Free',    f'{mem.available//1024//1024:,} MB')
            tbl.add_row('Disk Free',   f'{disk.free//1024//1024//1024} GB / {disk.total//1024//1024//1024} GB')
            tbl.add_row('Python',      sys.version[:40])
            tbl.add_row('Tool',        f'{TOOL_NAME} v{TOOL_VERSION}')
            tbl.add_row('Admin',       '[green]Yes[/green]' if is_admin() else '[red]No[/red]')
            tbl.add_row('Webhook',     '[green]Connected[/green]' if (webhook and webhook.enabled) else '[dim]Disabled[/dim]')
            tbl.add_row('Commands',    '[bright_magenta]Active[/bright_magenta]' if (webhook and webhook._cmd_active) else '[dim]Inactive[/dim]')
            tbl.add_row('Titan',       '[green]Running[/green]' if titan_is_running() else ('[yellow]Stopped[/yellow]' if titan_is_available() else '[red]Not found[/red]'))
        except Exception as e: tbl.add_row('Error', str(e))
        rich_console.print(tbl)
    else:
        print_centered(f'OS: {platform.system()} {platform.release()}')
        try:
            mem=psutil.virtual_memory()
            print_centered(f'RAM: {mem.used//1024//1024}MB / {mem.total//1024//1024}MB ({mem.percent}%)')
        except: pass
    pause()

def _lifetime_stats():
    draw_header('LIFETIME STATISTICS'); stats=load_stats()
    if RICH_ENABLED:
        tbl=Table(box=box.ROUNDED, show_header=False, expand=False)
        tbl.add_column('Stat', style='dim', width=30); tbl.add_column('Value', style='bold white')
        tbl.add_row('Total Sessions Started', str(stats.get('total_sessions',0)))
        tbl.add_row('Total Crashes Caught',   str(stats.get('total_crashes',0)))
        tbl.add_row('Total Restarts',         str(stats.get('total_restarts',0)))
        tbl.add_row('Total Server Hops',      str(stats.get('total_hops',0)))
        tbl.add_row('First Use',              stats.get('first_use','N/A')[:19])
        tbl.add_row('Last Use',               stats.get('last_use','N/A')[:19])
        rich_console.print(tbl)
    else:
        for k,v in stats.items(): print_centered(f'{k}: {v}')
    pause()

def _roblox_process_info():
    draw_header('ROBLOX PROCESS INFO'); found=False
    try:
        for p in psutil.process_iter(['pid','name','status','memory_info','cpu_percent','create_time']):
            if p.info['name']==ROBLOX_PROCESS_NAME:
                found=True
                ct  = datetime.datetime.fromtimestamp(p.info['create_time']).strftime('%H:%M:%S')
                mb  = p.info['memory_info'].rss//1024//1024 if p.info['memory_info'] else 0
                cpu = p.cpu_percent(interval=0.5)
                show_info(f"PID: {p.info['pid']}  |  RAM: {mb}MB  |  CPU: {cpu}%  |  Started: {ct}")
    except Exception as e: show_err(f'Error: {e}')
    if not found: show_warn('No Roblox instances running.')
    pause()

def _kill_roblox():
    draw_header('KILL ALL ROBLOX INSTANCES')
    if get_input('Kill all Roblox processes? (y/n)', C.RED).lower() != 'y': return
    killed=0
    try:
        for p in psutil.process_iter(['name','pid']):
            if p.info['name']==ROBLOX_PROCESS_NAME:
                try: p.kill(); killed+=1
                except: pass
    except: pass
    if _wh('roblox_killed') and killed: webhook.roblox_killed(killed)
    show_ok(f'Killed {killed} instance(s).'); time.sleep(2)

def _open_config_folder():
    folder=os.path.dirname(CONFIG_FILE)
    try:
        if sys.platform=='win32': os.startfile(folder); show_ok(f'Opened: {folder}')
        else: show_info(f'Path: {folder}')
    except Exception as e: show_err(f'{e}')
    time.sleep(2)

def _view_log():
    draw_header('SESSION LOG')
    if not os.path.exists(SESSION_LOG_FILE): show_warn('No log file yet.'); pause(); return
    try:
        with open(SESSION_LOG_FILE,'r',encoding='utf-8') as f: lines=f.readlines()
        for ln in lines[-60:]: print(ln.rstrip())
    except Exception as e: show_err(f'{e}')
    pause()

def _clear_log():
    if get_input('Clear session log? (y/n)', C.RED).lower()=='y':
        try: open(SESSION_LOG_FILE,'w').close(); show_ok('Log cleared.')
        except Exception as e: show_err(f'{e}')
    time.sleep(1.5)

def _ping_roblox():
    draw_header('PING ROBLOX SERVERS')
    hosts=['www.roblox.com','auth.roblox.com','assetgame.roblox.com','users.roblox.com','apis.roblox.com']
    for host in hosts:
        try:
            start=time.time(); socket.setdefaulttimeout(3)
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); s.connect((host,443)); s.close()
            ms=int((time.time()-start)*1000)
            color='green' if ms<100 else ('yellow' if ms<250 else 'red')
            if RICH_ENABLED: rich_console.print(f"  [{color}]● {host:<40} {ms}ms[/{color}]")
            else: show_ok(f'{host} — {ms}ms')
        except: show_err(f'{host} — UNREACHABLE')
    pause()

def _disk_space():
    draw_header('DISK SPACE')
    try:
        drives=['C:\\','D:\\','E:\\','F:\\'] if sys.platform=='win32' else ['/']
        for d in drives:
            if os.path.exists(d):
                usage=psutil.disk_usage(d); pct=usage.percent
                bar='█'*int(pct/5)+'░'*(20-int(pct/5))
                color=C.RED if pct>85 else (C.ORANGE if pct>70 else C.GREEN)
                print_centered(colorize(f'  {d}  [{bar}] {pct}%  ({usage.free//1024//1024//1024}GB free / {usage.total//1024//1024//1024}GB total)', color))
    except Exception as e: show_err(f'{e}')
    pause()

def _restart_as_admin():
    if is_admin(): show_info('Already running as administrator.'); pause(); return
    if get_input('Restart as administrator? (y/n)').lower()=='y':
        try: ctypes.windll.shell32.ShellExecuteW(None,'runas',sys.executable,' '.join(sys.argv),None,1); sys.exit(0)
        except Exception as e: show_err(f'{e}')
    time.sleep(2)

def _view_cookies_masked():
    draw_header('SAVED COOKIES (MASKED)'); accounts=load_accounts()
    if not accounts: show_warn('No accounts.'); pause(); return
    for name, cookie in accounts.items():
        if cookie: masked=cookie[:8]+'•'*20+cookie[-4:] if len(cookie)>12 else '•'*len(cookie)
        else:      masked='(empty)'
        show_info(f'{name}:  {masked}')
    pause()

def _check_internet():
    draw_header('INTERNET CHECK')
    tests=[('https://www.roblox.com','Roblox'),('https://www.google.com','Google'),
           ('https://discord.com','Discord')]
    for url, label in tests:
        try:
            start=time.time(); r=requests.get(url,timeout=5)
            ms=int((time.time()-start)*1000); show_ok(f'{label} — HTTP {r.status_code}, {ms}ms')
        except Exception as e: show_err(f'{label} — unreachable: {e}')
    pause()

def _clock_check():
    draw_header('SYSTEM CLOCK CHECK')
    try:
        r=requests.get('http://worldtimeapi.org/api/ip',timeout=5)
        if r.status_code==200:
            server=r.json().get('datetime','N/A'); local=datetime.datetime.now().isoformat()
            show_info(f'Server time: {server[:19]}'); show_info(f'Local  time: {local[:19]}')
            show_ok('Clock looks synced!')
        else: show_warn('Could not fetch server time.')
    except Exception as e: show_err(f'Error: {e}')
    pause()

def _test_adb():
    draw_header('TEST ADB CONNECTION')
    ch=get_choice('Emulator',['MuMu','LDPlayer','BlueStacks','Cancel'])
    if not ch or ch>3: return
    adb=get_adb({1:'mumu',2:'ldplayer',3:'bluestacks'}[ch])
    if not adb: show_err('ADB not found.'); pause(); return
    try:
        res=subprocess.run([adb,'devices'],capture_output=True,text=True,timeout=8,
                           creationflags=subprocess.CREATE_NO_WINDOW)
        show_ok(f'ADB responsive: {adb}')
        for ln in res.stdout.strip().splitlines(): show_info(f'  {ln}')
    except Exception as e: show_err(f'ADB error: {e}')
    pause()

def _export_config():
    draw_header('EXPORT CONFIG AS TEXT')
    try:
        cfg=load_config(); safe=json.loads(json.dumps(cfg))
        safe['roblox_accounts']={k:'(hidden)' for k in safe.get('roblox_accounts',{})}
        # Also hide bot token in export
        if 'bot_token' in safe.get('settings',{}).get('webhook',{}):
            safe['settings']['webhook']['bot_token']='(hidden)'
        out=os.path.join(base_path,'pulse_config_export.txt')
        with open(out,'w',encoding='utf-8') as f:
            f.write(f'{TOOL_NAME} v{TOOL_VERSION} — Config Export\n')
            f.write(f'Generated: {datetime.datetime.now()}\n'+'='*60+'\n')
            json.dump(safe, f, indent=4)
        show_ok(f'Exported to {out}')
    except Exception as e: show_err(f'{e}')
    pause()

def _open_log_folder():
    try:
        if sys.platform=='win32': os.startfile(base_path); show_ok(f'Opened: {base_path}')
        else: show_info(f'Path: {base_path}')
    except Exception as e: show_err(f'{e}')
    time.sleep(2)

def _count_sessions():
    draw_header('ACTIVE SESSIONS')
    with STATUS_LOCK: n=len(SESSION_STATUS)
    if n>0:
        show_info(f'{n} session(s) monitored.')
        with STATUS_LOCK:
            for k,d in SESSION_STATUS.items():
                place = d.get('place_id','—')
                show_info(f"  • {d.get('instance_name',k)}  [dim](place: {place})[/dim]"
                          if RICH_ENABLED else f"  • {d.get('instance_name',k)}  (place: {place})")
    else: show_info('No sessions active.')
    pause()

def _clear_stats():
    if get_input('Clear all lifetime stats? (y/n)', C.RED).lower()=='y':
        try: os.remove(STATS_FILE); show_ok('Lifetime stats cleared.')
        except: show_ok('Stats already clear.')
    time.sleep(1.5)


# ══════════════════════════════════════════════════════════════
#  ABOUT / CHANGELOG
# ══════════════════════════════════════════════════════════════
def about_menu():
    draw_header(f'ABOUT {TOOL_NAME}', f'v{TOOL_VERSION}')
    if RICH_ENABLED:
        stats=load_stats()
        wh_on = webhook and webhook.enabled
        txt = f"""
[bold bright_blue]██████╗ ██╗   ██╗██╗     ███████╗███████╗[/bold bright_blue]
[bold blue]██╔══██╗██║   ██║██║     ██╔════╝██╔════╝[/bold blue]
[bold blue]██████╔╝██║   ██║██║     ███████╗█████╗  [/bold blue]
[bold blue]██╔═══╝ ██║   ██║██║     ╚════██║██╔══╝  [/bold blue]
[bold bright_blue]██║     ╚██████╔╝███████╗███████║███████╗[/bold bright_blue]
[bold bright_blue]╚═╝      ╚═════╝ ╚══════╝╚══════╝╚══════╝[/bold bright_blue]
[dim]R E J O I N   T O O L   v{TOOL_VERSION}[/dim]

[bold]What this tool does:[/bold]
  • Auto-rejoins Roblox on crash / disconnect
  • Monitors multiple instances (desktop + emulators)
  • Scheduled restarts & freeze detection
  • Encrypted, machine-bound account storage
  • Server Hopper — auto-switch servers on a timer
  • Titan Spoofer integration
  • [bold]Discord Webhook[/bold] — fully user-controlled, zero hardcoded URLs
    — Live crash, restart & server notifications
    — [bold bright_magenta].rejoin  .join  .getid[/bold bright_magenta] remote commands
  • Lifetime stats tracking

[bold]Supported emulators:[/bold]  MuMu  •  LDPlayer  •  BlueStacks

[bold]Webhook:[/bold]  {'[bright_green]Active[/bright_green]' if wh_on else '[dim]Not configured[/dim]'}   [bold]Commands:[/bold]  {'[bright_magenta]Active[/bright_magenta]' if (webhook and webhook._cmd_active) else '[dim]Inactive[/dim]'}
[bold]Lifetime Stats:[/bold]  {stats.get('total_sessions',0)} sessions  •  {stats.get('total_crashes',0)} crashes  •  {stats.get('total_hops',0)} hops

[bold]Credits:[/bold]  [orange1]{TOOL_CREDIT}[/orange1]
[bold]Discord:[/bold]  [link={TOOL_DISCORD}][bright_blue]{TOOL_DISCORD_SHORT}[/bright_blue][/link]
[bold]Version:[/bold]  {TOOL_VERSION}  |  Build {BUILD_DATE}
"""
        rich_console.print(Panel(txt.strip(), box=box.ROUNDED, style=current_theme['primary'], padding=(1,4)))
    print()
    ch = get_choice('Option',['Join Discord','View Changelog','Back'])
    if ch == 1:
        webbrowser.open(TOOL_DISCORD); show_ok('Opened Discord!'); time.sleep(1.5)
    elif ch == 2:
        draw_header('CHANGELOG')
        if RICH_ENABLED:
            rich_console.print(Panel(
                "[bold]v5.0.0[/bold] — Current\n"
                "  • [bright_green]NEW[/bright_green] Fully user-controlled webhook — zero hardcoded URLs\n"
                "  • [bright_green]NEW[/bright_green] Discord Commands: [bright_magenta].rejoin[/bright_magenta] [bright_magenta].join <serverID/placeID>[/bright_magenta] [bright_magenta].getid[/bright_magenta]\n"
                "  • [bright_green]NEW[/bright_green] Server info events — place ID, server ID, user ID sent to webhook\n"
                "  • [bright_green]NEW[/bright_green] Push server info button in Account Manager\n"
                "  • [bright_green]NEW[/bright_green] Bot token + command channel ID setup in Webhook settings\n"
                "  • [bright_green]NEW[/bright_green] Command reference panel in Webhook settings\n"
                "  • [bright_green]NEW[/bright_green] Aurora & Crimson themes\n"
                "  • [bright_green]NEW[/bright_green] Commands active badge in live dashboard\n"
                "  • [bright_green]NEW[/bright_green] Place ID column added to live dashboard\n"
                "  • [bright_green]NEW[/bright_green] Log forwarding event toggle\n"
                "  • Improved Account Manager with user ID and profile link columns\n"
                "  • Improved Webhook settings UI with two-column event table\n"
                "  • Session status now stores place_id and user_id for .getid\n"
                "  • Server hop webhook now includes server ID\n\n"
                "[bold]v4.x[/bold] — Previous\n"
                "  • Titan Spoofer, lifetime stats, crash sound, multi-theme",
                title='📋 Changelog', box=box.ROUNDED, style='bright_blue', padding=(1,3)
            ))
        pause()


# ══════════════════════════════════════════════════════════════
#  ANIMATED LOADER
# ══════════════════════════════════════════════════════════════
_P_ART = [
    "  ██████╗ ",
    "  ██╔══██╗",
    "  ██████╔╝",
    "  ██╔═══╝ ",
    "  ██║     ",
    "  ╚═╝     ",
]

LOADER_STAGES = [
    ('Initializing core ...',            0.12),
    ('Loading configuration ...',        0.24),
    ('Checking dependencies ...',        0.36),
    ('Starting session manager ...',     0.48),
    ('Loading account vault ...',        0.60),
    ('Configuring webhook ...',          0.74),
    ('Applying theme ...',               0.88),
    ('All systems ready!',               1.00),
]

def _center_raw(text):
    try:    w=os.get_terminal_size().columns
    except: w=80
    vis=len(re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]','',text))
    return ' '*max(0,(w-vis)//2)+text

def _clear_lines(n):
    for _ in range(n): sys.stdout.write('\x1b[1A\x1b[2K')

def _loader_bar(progress, width=44):
    filled=int(width*progress); bar='█'*filled+'░'*(width-filled)
    pct=int(progress*100)
    color='\x1b[38;2;0;180;255m' if progress<1.0 else '\x1b[38;2;0;255;150m'
    return f'{color}[{bar}] {pct:3d}%\x1b[0m'

def _show_ansi_loader():
    clear_screen(); print()
    for i, line in enumerate(_P_ART):
        ratio=i/(len(_P_ART)-1); r=0; g=int(80+ratio*150); b=int(200+ratio*55)
        col=f'\x1b[38;2;{r};{g};{b}m'
        print(_center_raw(f'{col}{line}  PULSE REJOIN TOOL\x1b[0m'))
    print()
    print(_center_raw(f'\x1b[38;2;100;180;255m  v{TOOL_VERSION}  •  Credits: {TOOL_CREDIT}  •  {TOOL_DISCORD_SHORT}\x1b[0m'))
    print()
    for idx, (stage, progress) in enumerate(LOADER_STAGES):
        label = _center_raw(f'\x1b[38;2;150;200;255m{stage:<45}\x1b[0m')
        bar   = _center_raw(_loader_bar(progress))
        if idx > 0: _clear_lines(2)
        print(label); print(bar); sys.stdout.flush()
        if idx < len(LOADER_STAGES)-1: time.sleep(random.uniform(0.18,0.42))
    time.sleep(0.4); _clear_lines(2)
    print(_center_raw('\x1b[1m\x1b[38;2;0;255;150m✓  All systems ready!\x1b[0m'))
    sys.stdout.flush(); time.sleep(0.6); print()
    print(_center_raw('\x1b[2mPress any key to continue ...\x1b[0m')); sys.stdout.flush()
    try:    msvcrt.getch()
    except: input()

def _show_rich_loader():
    rich_console.clear()
    try:
        logo_path=os.path.join(base_path, LOGO_FILE)
        if not os.path.exists(logo_path):
            logo_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), LOGO_FILE)
        if os.path.exists(logo_path):
            pixels=Pixels.from_image_path(logo_path, resize=(72,36))
            rich_console.print(Align.center(pixels))
        else:
            art='\n'.join(f"[bold bright_blue]{ln}[/bold bright_blue]  [bold bright_cyan]PULSE REJOIN TOOL[/bold bright_cyan]"
                          for ln in _P_ART)
            rich_console.print(Align.center(art))
    except:
        rich_console.print(Align.center(f"[bold bright_blue]{TOOL_NAME} v{TOOL_VERSION}[/bold bright_blue]"))
    rich_console.print()
    rich_console.print(Align.center(f"[bold bright_blue]{TOOL_NAME}[/bold bright_blue]  [dim]v{TOOL_VERSION}[/dim]"))
    rich_console.print(Align.center(
        f"[dim]Credits: [orange1]{TOOL_CREDIT}[/orange1]  •  "
        f"[link={TOOL_DISCORD}][bright_blue]{TOOL_DISCORD_SHORT}[/bright_blue][/link][/dim]"
    ))
    rich_console.print()
    with Progress(
        SpinnerColumn(spinner_name='dots', style='bright_blue'),
        TextColumn('[bold bright_cyan]{task.description}[/bold bright_cyan]'),
        BarColumn(bar_width=44, style='bright_blue', complete_style='bright_cyan', finished_style='bright_green'),
        TextColumn('[bold]{task.percentage:>3.0f}%[/bold]'),
        TimeElapsedColumn(),
        console=rich_console, transient=False,
    ) as prog:
        task=prog.add_task('Starting ...', total=100); prev=0
        for stage, target_pct in LOADER_STAGES:
            prog.update(task, description=stage); target=int(target_pct*100)
            for pct in range(prev, target+1):
                prog.update(task, completed=pct); time.sleep(random.uniform(0.006,0.022))
            prev=target; time.sleep(random.uniform(0.04,0.12))
    rich_console.print()
    rich_console.print(Align.center('[bold bright_green]✓  All systems ready![/bold bright_green]'))
    rich_console.print()
    rich_console.print(Align.center('[dim]Press any key to continue ...[/dim]')); sys.stdout.flush()
    try:    msvcrt.getch()
    except: input()

def show_splash():
    if RICH_ENABLED: _show_rich_loader()
    else:            _show_ansi_loader()


# ══════════════════════════════════════════════════════════════
#  MAIN MENU
# ══════════════════════════════════════════════════════════════
def main():
    if not acquire_mutex(): input('\nPress Enter to exit.'); return

    cfg = load_config(); apply_theme(cfg); init_webhook(cfg)
    # Fire tool_started if webhook is configured
    if _wh('tool_start'): webhook.tool_started()

    while True:
        cfg=load_config(); apply_theme(cfg)
        last=cfg.get('last_used_profile'); profs=cfg.get('profiles',{})
        draw_header(f'{TOOL_NAME}', f'v{TOOL_VERSION}  —  by {TOOL_CREDIT}')

        accs   = len(load_accounts())
        prc    = len(profs)
        stats  = load_stats()
        t_run  = titan_is_running()
        t_avail= titan_is_available()
        wh_on  = webhook and webhook.enabled and bool(webhook.webhook_url)
        cmd_on = webhook and webhook._cmd_active

        if RICH_ENABLED:
            titan_badge = ''
            if t_run:     titan_badge = '  [bright_green]⚙ Titan: ON[/bright_green]'
            elif t_avail: titan_badge = '  [dim]⚙ Titan: OFF[/dim]'
            wh_badge  = (f'  [bright_blue]📡 Webhook: ON[/bright_blue]'
                         f'{"  [bright_magenta]⌨ Cmds[/bright_magenta]" if cmd_on else ""}') if wh_on else '  [dim]📡 No Webhook[/dim]'

            rich_console.print(Align.center(
                f"[dim]Accounts: [white]{accs}[/white]  •  Profiles: [white]{prc}[/white]  •  "
                f"Crashes caught: [white]{stats.get('total_crashes',0)}[/white]  •  "
                f"[link={TOOL_DISCORD}][bright_blue]💬 {TOOL_DISCORD_SHORT}[/bright_blue][/link]  •  "
                f"Credits: [orange1]{TOOL_CREDIT}[/orange1][/dim]{titan_badge}{wh_badge}"
            ))
            rich_console.print()

        all_opts=[]; all_actions=[]

        all_opts.append(f"[bold {current_theme['secondary']}]🖥   Monitor All Dashboard[/bold {current_theme['secondary']}]"
                        if RICH_ENABLED else '🖥   Monitor All Dashboard')
        all_actions.append('monitor_all')

        if last and last in profs:
            mode=profs[last].get('mode','?').capitalize()
            all_opts.append(f"▶   Quick Launch: [bold]'{last}'[/bold]  [dim]({mode})[/dim]"
                            if RICH_ENABLED else f"▶   Quick Launch: '{last}' ({mode})")
            all_actions.append('launch_last')

        all_opts.append('👤  Account Manager'); all_actions.append('accounts')
        all_opts.append('📋  Manage Profiles'); all_actions.append('profiles')

        hop_badge=(' [bright_green][ACTIVE][/bright_green]' if SERVER_HOP_ACTIVE else '') if RICH_ENABLED else (colorize(' [ACTIVE]',C.GREEN) if SERVER_HOP_ACTIVE else '')
        all_opts.append(f'🔀  Server Hopper{hop_badge}'); all_actions.append('serverhopper')

        titan_label = '⚙   Titan'
        if RICH_ENABLED:
            if t_run:         titan_label='⚙   Titan  [bright_green][RUNNING][/bright_green]'
            elif not t_avail: titan_label='⚙   Titan  [dim][not found][/dim]'
        all_opts.append(titan_label); all_actions.append('titan')

        wh_label = '📡  Discord Webhook & Commands'
        if RICH_ENABLED:
            wh_label = (f'📡  Discord Webhook & Commands  [bright_blue][ON][/bright_blue]'
                        f'{"  [bright_magenta]⌨ cmds[/bright_magenta]" if cmd_on else ""}' if wh_on
                        else '📡  Discord Webhook & Commands  [dim][OFF][/dim]')
        all_opts.append(wh_label); all_actions.append('webhook')

        all_opts.append('🛠   Settings');       all_actions.append('settings')
        all_opts.append('🔧  Utilities');       all_actions.append('utilities')
        all_opts.append('ℹ   About & Discord'); all_actions.append('about')
        all_opts.append('❌  Exit');            all_actions.append('exit')

        ch=get_choice('Select an option', all_opts)
        if not ch or ch > len(all_actions): continue
        action=all_actions[ch-1]

        if   action == 'monitor_all': monitor_all_dashboard()
        elif action == 'launch_last':
            if not warn_before_launch(): continue
            _profile_launch(profs[last], last)
        elif action == 'accounts':    manage_accounts()
        elif action == 'profiles':    manage_profiles()
        elif action == 'serverhopper':server_hopper_menu()
        elif action == 'titan':       titan_menu()
        elif action == 'webhook':     webhook_settings_menu()
        elif action == 'settings':    settings_menu()
        elif action == 'utilities':   utilities_menu()
        elif action == 'about':       about_menu()
        elif action == 'exit':        break

    # Cleanup
    if titan_is_running(): titan_stop()
    if webhook: webhook.stop_command_listener()
    if _wh('tool_close'): webhook.tool_closed()
    time.sleep(0.8)

    clear_screen()
    if RICH_ENABLED:
        rich_console.print(Align.center(
            f"[bold bright_blue]Thanks for using {TOOL_NAME}!\n"
            f"[/bold bright_blue][dim]Credits: {TOOL_CREDIT}  •  {TOOL_DISCORD_SHORT}[/dim]"
        ))
    else:
        print_centered(colorize(f'Thanks for using {TOOL_NAME}! Credits: {TOOL_CREDIT}', C.BOLD+C.CYAN))
    time.sleep(2)


# ══════════════════════════════════════════════════════════════
#  ENTRY
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    clear_screen()
    if not check_and_install_libraries(REQUIRED_LIBRARIES):
        print('\n[X] Dependency error. Cannot start.')
        input('Press Enter to exit.'); sys.exit(1)
    show_splash()
    try:
        main()
    except KeyboardInterrupt:
        MONITORING_ACTIVE=False
        if titan_is_running(): titan_stop()
        clear_screen()
        print(f'\n  {TOOL_NAME} — stopped by user. Credits: {TOOL_CREDIT}')
        time.sleep(1); sys.exit(0)
    except Exception as e:
        MONITORING_ACTIVE=False; clear_screen()
        tb=traceback.format_exc()
        print(f'\n[CRITICAL] {e}')
        with open(CRITICAL_LOG_FILE,'w') as f: f.write(f'Error: {e}\n{tb}')
        print(f'Log saved: {CRITICAL_LOG_FILE}')
        input('\nPress Enter to exit ...')