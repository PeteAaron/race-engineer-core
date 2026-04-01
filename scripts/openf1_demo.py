"""
OpenF1 historical adapter demo.

Loads a past race session from the OpenF1 public API and prints the
resulting TimingState. Works at any time — no live session required.

Usage:
    python scripts/openf1_demo.py                  # Abu Dhabi 2023 Race (default)
    python scripts/openf1_demo.py --session 9158
    python scripts/openf1_demo.py --year 2024 --type Race --location Bahrain
    python scripts/openf1_demo.py --session 9158 --save session.jsonl

The --save flag writes a JSONL event store that can be replayed later:
    python scripts/openf1_demo.py --replay session.jsonl
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from race_engineer_core.sources.openf1 import OpenF1Client, OpenF1SessionLoader
from race_engineer_core.sources import DataSource, load_state


def _print_state(state) -> None:
    print()
    print("=" * 60)
    print("TimingState")
    print("=" * 60)

    info = state.session_info
    if info:
        print(f"\nSession : {info.get('meeting_name')} — {info.get('name')}")
        print(f"Key     : {info.get('key')}")
        print(f"Offset  : GMT{info.get('gmt_offset', '')}")

    print(f"\nStatus  : {state.session_status}")
    print(f"Track   : {state.track_status}")

    print(f"\nDrivers ({len(state.driver_list)}):")
    for num, d in sorted(state.driver_list.items(), key=lambda x: int(x[0])):
        pos = state.driver_positions.get(num, "?")
        lap = state.latest_laps.get(num, {})
        best = lap.get("best_lap_time", "-")
        print(f"  P{pos:>2}  #{num:>2}  {d.get('tla', '???'):3}  {d.get('team_name', ''):25}  best: {best}")

    print(f"\nPit stops ({len(state.pit_history)}):")
    for pit in state.pit_history[:5]:
        print(f"  #{pit.get('racing_number'):>2}  lap {pit.get('lap')}  {pit.get('duration')}s")
    if len(state.pit_history) > 5:
        print(f"  ... and {len(state.pit_history) - 5} more")

    print(f"\nRace control messages ({len(state.race_control_messages)}):")
    for msg in state.race_control_messages[:5]:
        lap_str = f"L{msg['lap']} " if msg.get("lap") else ""
        print(f"  {lap_str}[{msg.get('flag', msg.get('category', ''))}] {msg.get('message', '')}")
    if len(state.race_control_messages) > 5:
        print(f"  ... and {len(state.race_control_messages) - 5} more")

    print()


def _find_session_key(client: OpenF1Client, year: int, session_type: str, location: str | None) -> int:
    print(f"Searching for {year} {session_type}" + (f" @ {location}" if location else "") + " ...")
    session = client.find_session(year, session_type, location=location)
    if session is None:
        print("No matching session found.", file=sys.stderr)
        sys.exit(1)
    key = session["session_key"]
    print(f"Found: {session.get('meeting_name')} — {session.get('session_name')} (key={key})")
    return key


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenF1 historical session demo")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--session", type=int, default=9158, help="OpenF1 session_key (default: 9158 Abu Dhabi 2023 Race)")
    group.add_argument("--replay", type=Path, help="Replay from a previously saved JSONL store")

    parser.add_argument("--year", type=int, default=None, help="Year (used with --type / --location)")
    parser.add_argument("--type", dest="session_type", default="Race", help="Session type e.g. Race, Qualifying")
    parser.add_argument("--location", default=None, help="Location filter e.g. Bahrain")
    parser.add_argument("--save", type=Path, default=None, help="Write events to JSONL store (enables later --replay)")
    args = parser.parse_args()

    if args.replay:
        print(f"Replaying from {args.replay} ...")
        state = load_state(DataSource.REPLAY, store_path=args.replay)
    else:
        client = OpenF1Client()

        if args.year is not None:
            session_key = _find_session_key(client, args.year, args.session_type, args.location)
        else:
            session_key = args.session
            print(f"Loading session_key={session_key} from OpenF1 API ...")

        state = load_state(
            DataSource.OPENF1,
            session_key=session_key,
            openf1_client=client,
            store_path=args.save,
        )

        if args.save:
            print(f"Events saved to {args.save} — use --replay {args.save} to replay")

    _print_state(state)


if __name__ == "__main__":
    main()
