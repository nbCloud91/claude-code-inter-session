"""Send a single message to one session, then exit. role=control."""

from __future__ import annotations

# Bootstrap: re-exec under the project's isolated venv if it exists.
import os
import sys
from pathlib import Path
_VENV = Path.home() / ".claude" / "data" / "inter-session" / "venv"
_VENV_PY = _VENV / "bin" / "python"
if (not os.environ.get("INTER_SESSION_NO_REEXEC")
        and _VENV_PY.is_file()
        and Path(sys.prefix).resolve() != _VENV.resolve()):
    os.execv(str(_VENV_PY), [str(_VENV_PY), *sys.argv])

import argparse
import asyncio
import json
import uuid

# Allow running as a script.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import websockets

from bin import shared, discover


async def _run(args) -> int:
    state, state_path = discover.find_listener_state_with_path()
    if state is None:
        print("not connected; run /inter-session in this Claude Code session first",
              file=sys.stderr)
        return 1

    host = state.get("host", "127.0.0.1")
    port = state.get("port", shared.DEFAULT_PORT)
    token = state["token"]
    for_session = state["session_id"]
    nonce = state["nonce"]

    if not shared.verify_server_identity(host, port):
        print(f"server identity check failed ({host}:{port} not held by bin/server.py)",
              file=sys.stderr)
        return 1

    try:
        ws = await websockets.connect(
            f"ws://{host}:{port}/", max_size=shared.WS_FRAME_CAP,
        )
    except OSError as e:
        print(f"connect failed: {e}", file=sys.stderr)
        return 1

    try:
        await ws.send(json.dumps({
            "op": "hello",
            "session_id": str(uuid.uuid4()),
            "name": "",
            "label": "",
            "cwd": os.getcwd(),
            "pid": os.getpid(),
            "role": shared.Role.CONTROL.value,
            "for_session": for_session,
            "nonce": nonce,
            "token": token,
        }))
        welcome_raw = await ws.recv()
        welcome = json.loads(welcome_raw)
        if welcome.get("op") == "error":
            code = welcome.get("code", "")
            if code in (shared.ErrorCode.UNKNOWN_PEER, shared.ErrorCode.UNAUTHORIZED):
                # Stale state file: listener referenced no longer registered.
                # TOCTOU-safe: only unlink if the file still has the same
                # session_id+nonce as we read; otherwise a fresh listener has
                # written new state between our read and now.
                discover.unlink_if_matches(state_path, state)
                print("not connected; run /inter-session in this Claude Code session first",
                      file=sys.stderr)
                return 1
            print(f"hello error: {code} {welcome.get('message', '')}",
                  file=sys.stderr)
            return 1

        op = "broadcast" if args.all else "send"
        payload = {"op": op, "text": args.text}
        if not args.all:
            payload["to"] = args.to
        await ws.send(json.dumps(payload))

        # Wait briefly for an error frame; success is silence.
        try:
            resp_raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            resp = json.loads(resp_raw)
            if resp.get("op") == "error":
                msg = f"error: {resp.get('code', '')}: {resp.get('message', '')}"
                if "matches" in resp:
                    msg += f" (matches: {', '.join(resp['matches'])})"
                if "candidates" in resp:
                    msg += f" (try: {', '.join(resp['candidates'])})"
                print(msg, file=sys.stderr)
                return 1
        except asyncio.TimeoutError:
            pass
    finally:
        await ws.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", help="target name or short session_id (omit with --all)")
    parser.add_argument("--all", action="store_true", help="broadcast to all other sessions")
    parser.add_argument("--text", help="message body (omit to use --text-file or stdin)")
    parser.add_argument(
        "--text-file", metavar="PATH",
        help="read the body from PATH, or '-' for stdin. Keeps the body off the "
             "command line — avoids shell-quoting and pre-tool-use hooks that scan "
             "--text, and preserves the body verbatim (no trailing-newline stripping).")
    args = parser.parse_args()
    if not args.all and not args.to:
        parser.error("--to required unless --all")
    if args.text is not None and args.text_file is not None:
        parser.error("--text and --text-file are mutually exclusive")
    # Resolve the body: explicit --text wins; else --text-file (path or '-'); else
    # read stdin when it is piped (a bare invocation on a tty would otherwise hang).
    if args.text_file is not None:
        if args.text_file == "-":
            args.text = sys.stdin.read()
        else:
            try:
                args.text = Path(args.text_file).read_text()
            except OSError as e:
                parser.error(f"--text-file: {e}")
    elif args.text is None:
        if sys.stdin.isatty():
            parser.error("provide --text, --text-file PATH, or pipe the body on stdin")
        args.text = sys.stdin.read()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    try:
        import websockets  # noqa: F401
    except ImportError:
        print("dependencies missing — run /inter-session install-deps", file=sys.stderr)
        sys.exit(1)
    sys.exit(main())
