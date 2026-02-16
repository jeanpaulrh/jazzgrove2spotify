#!/usr/bin/env python3
"""Capture now-playing tracks from The Jazz Groove and add them to a Spotify playlist."""

import argparse
import configparser
import fcntl
import json
import logging
import os
import re
import sqlite3
import struct
import sys
from difflib import SequenceMatcher

import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_FILE = os.path.join(SCRIPT_DIR, ".lock")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.join(SCRIPT_DIR, "config.ini")
    if not config.read(config_path):
        sys.exit(f"Config file not found: {config_path}\nCopy config.ini.example to config.ini and fill in your credentials.")
    return config


def setup_logging(config):
    log_file = config.get("settings", "log_file", fallback="jazzgrove2spotify.log")
    if not os.path.isabs(log_file):
        log_file = os.path.join(SCRIPT_DIR, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )

# ---------------------------------------------------------------------------
# File lock (prevent concurrent cron runs)
# ---------------------------------------------------------------------------

def acquire_lock():
    fp = open(LOCK_FILE, "w")
    try:
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit(0)  # another instance running, exit silently
    return fp  # keep reference alive


def release_lock(fp):
    fcntl.flock(fp, fcntl.LOCK_UN)
    fp.close()
    try:
        os.unlink(LOCK_FILE)
    except OSError:
        pass

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path):
    if not os.path.isabs(db_path):
        db_path = os.path.join(SCRIPT_DIR, db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist TEXT NOT NULL,
            title TEXT NOT NULL,
            spotify_uri TEXT,
            added_to_playlist INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(artist, title)
        )"""
    )
    conn.commit()
    return conn


def track_exists(conn, artist, title):
    row = conn.execute(
        "SELECT id, added_to_playlist FROM tracks WHERE artist = ? AND title = ?",
        (artist, title),
    ).fetchone()
    return row


def save_track(conn, artist, title, spotify_uri, added):
    conn.execute(
        """INSERT OR IGNORE INTO tracks (artist, title, spotify_uri, added_to_playlist)
           VALUES (?, ?, ?, ?)""",
        (artist, title, spotify_uri, int(added)),
    )
    conn.commit()

# ---------------------------------------------------------------------------
# Metadata fetching
# ---------------------------------------------------------------------------

def fetch_sse_metadata(stream_url, timeout=10):
    """Fetch current track from Radio Mast SSE endpoint."""
    url = stream_url.rstrip("/") + "/metadata"
    try:
        with requests.get(url, stream=True, timeout=timeout, headers={"Accept": "text/event-stream"}) as r:
            r.raise_for_status()
            buf = ""
            for chunk in r.iter_content(chunk_size=256, decode_unicode=True):
                buf += chunk
                # SSE events are separated by double newline
                while "\n\n" in buf:
                    event, buf = buf.split("\n\n", 1)
                    for line in event.splitlines():
                        if line.startswith("data:"):
                            data = json.loads(line[5:].strip())
                            title_raw = data.get("title") or data.get("name") or ""
                            if " - " in title_raw:
                                artist, title = title_raw.split(" - ", 1)
                                return artist.strip(), title.strip()
                            # Some SSE payloads have separate fields
                            artist = data.get("artist", "").strip()
                            title = (data.get("title") or data.get("name", "")).strip()
                            if artist and title:
                                return artist, title
    except Exception as e:
        logging.warning("SSE fetch failed: %s", e)
    return None, None


def fetch_icy_metadata(stream_url, timeout=10):
    """Fallback: extract ICY metadata from the audio stream."""
    try:
        headers = {"Icy-MetaData": "1"}
        with requests.get(stream_url, stream=True, timeout=timeout, headers=headers) as r:
            r.raise_for_status()
            metaint = int(r.headers.get("icy-metaint", 0))
            if not metaint:
                return None, None
            # Use r.raw.read() consistently to avoid buffer mismatch
            # Skip audio bytes to reach the metadata block
            r.raw.read(metaint)
            # Read metadata length byte
            length_byte = r.raw.read(1)
            if not length_byte:
                return None, None
            meta_length = struct.unpack("B", length_byte)[0] * 16
            if meta_length == 0:
                return None, None
            meta_data = r.raw.read(meta_length).decode("utf-8", errors="replace").strip("\x00")
            # Prefer JSON metadata (avoids apostrophe issues in StreamTitle)
            json_match = re.search(r"json='({.*?})'", meta_data)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    artist = data.get("artist", "").strip()
                    title = data.get("title", "").strip()
                    if artist and title:
                        return artist, title
                except json.JSONDecodeError:
                    pass
            # Fallback to StreamTitle
            match = re.search(r"StreamTitle='([^']*)'", meta_data)
            if match:
                raw = match.group(1).strip()
                if " - " in raw:
                    artist, title = raw.split(" - ", 1)
                    return artist.strip(), title.strip()
    except Exception as e:
        logging.warning("ICY fetch failed: %s", e)
    return None, None


def get_now_playing(stream_url, timeout=10):
    """Try SSE first, then ICY metadata."""
    artist, title = fetch_sse_metadata(stream_url, timeout)
    if artist and title:
        logging.info("SSE metadata: %s - %s", artist, title)
        return artist, title
    artist, title = fetch_icy_metadata(stream_url, timeout)
    if artist and title:
        logging.info("ICY metadata: %s - %s", artist, title)
        return artist, title
    return None, None

# ---------------------------------------------------------------------------
# Spotify (using new /items API endpoints, Feb 2026)
# ---------------------------------------------------------------------------

def _get_auth_manager(config):
    return SpotifyOAuth(
        client_id=config.get("spotify", "client_id"),
        client_secret=config.get("spotify", "client_secret"),
        redirect_uri=config.get("spotify", "redirect_uri"),
        scope="playlist-modify-public playlist-modify-private playlist-read-private playlist-read-collaborative",
        cache_handler=spotipy.CacheFileHandler(
            cache_path=os.path.join(
                SCRIPT_DIR, config.get("settings", "cache_path", fallback=".cache-spotipy")
            )
        ),
    )


def _get_access_token(config):
    """Get a valid access token, refreshing if needed."""
    auth = _get_auth_manager(config)
    token_info = auth.get_cached_token()
    if not token_info:
        raise RuntimeError("No cached token. Run --setup first.")
    if auth.is_token_expired(token_info):
        token_info = auth.refresh_access_token(token_info["refresh_token"])
    return token_info["access_token"]


def _spotify_headers(config):
    return {
        "Authorization": f"Bearer {_get_access_token(config)}",
        "Content-Type": "application/json",
    }


def get_spotify_client(config):
    """Get spotipy client (used for search and user info which still work)."""
    return spotipy.Spotify(auth_manager=_get_auth_manager(config))


def playlist_add_items(config, playlist_id, uris):
    """Add items to playlist using the new /items endpoint, with retry on 429."""
    import time
    MAX_WAIT = 10
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/items"
    for attempt in range(3):
        r = requests.post(url, headers=_spotify_headers(config), json={"uris": uris})
        if r.status_code == 429:
            try:
                wait = min(int(r.headers.get("Retry-After", 5)), MAX_WAIT)
            except (ValueError, TypeError):
                wait = MAX_WAIT
            logging.warning("Rate limited, waiting %ds (attempt %d/3)...", wait, attempt + 1)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def clean_title(text):
    """Remove parenthetical info, common suffixes, and extra whitespace."""
    text = re.sub(r"\s*[\(\[].*?[\)\]]", "", text)
    text = re.sub(r"\s*feat\.?\s+.*", "", text, flags=re.IGNORECASE)
    # Strip common suffixes (Album Version, Remastered, Live, Bonus Track, etc.)
    text = re.sub(
        r"\s*[-/]?\s*\b(album version|single version|mono|stereo|remaster(ed)?(\s+\d{4})?|"
        r"bonus track|deluxe|extended|short version|long version|live|edit)\b.*",
        "", text, flags=re.IGNORECASE,
    )
    return text.strip()


def clean_artist(text):
    """Remove ensemble suffixes for better matching."""
    text = re.sub(r"\s*[\(\[].*?[\)\]]", "", text)
    text = re.sub(
        r"\s+\b(quartet|quintet|trio|sextet|septet|octet|orchestra|ensemble|band)\b",
        "", text, flags=re.IGNORECASE,
    )
    return text.strip()


def first_artist(text):
    """Extract the first artist from multi-artist strings."""
    for sep in [" and ", " & ", ", ", " with ", " feat. ", " feat "]:
        if sep.lower() in text.lower():
            idx = text.lower().index(sep.lower())
            return text[:idx].strip()
    return text


def similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def search_spotify(sp, artist, title):
    """Multi-level search: structured -> free text -> cleaned title -> cleaned artist."""
    first = first_artist(artist)
    queries = [
        f'artist:"{artist}" track:"{title}"',
        f"{artist} {title}",
        f"{clean_artist(artist)} {clean_title(title)}",
        f'artist:"{clean_artist(artist)}" track:"{clean_title(title)}"',
        f'artist:"{first}" track:"{clean_title(title)}"',
    ]
    seen_queries = set()
    seen_uris = set()
    best = None  # (combined, t_artists, t_title, uri)
    for q in queries:
        if q in seen_queries:
            continue
        seen_queries.add(q)
        results = sp.search(q=q, type="track", limit=3)
        tracks = results.get("tracks", {}).get("items", [])
        for t in tracks:
            if t["uri"] in seen_uris:
                continue
            seen_uris.add(t["uri"])
            # Compare against all Spotify artists combined
            t_artists = ", ".join(a["name"] for a in t["artists"])
            t_title = t["name"]
            # Compare using cleaned versions for scoring
            artist_score = max(
                similarity(artist, t_artists),
                similarity(clean_artist(artist), t_artists),
                similarity(first, t["artists"][0]["name"]) if t["artists"] else 0,
            )
            title_score = max(
                similarity(title, t_title),
                similarity(clean_title(title), t_title),
                similarity(clean_title(title), clean_title(t_title)),
            )
            combined = (artist_score + title_score) / 2
            if combined >= 0.55 and title_score >= 0.4:
                if not best or combined > best[0]:
                    best = (combined, t_artists, t_title, t["uri"])
                    if combined >= 0.95:
                        break  # near-perfect match, stop searching
        if best and best[0] >= 0.95:
            break  # stop querying too
    if best:
        combined, t_artists, t_title, uri = best
        logging.info(
            "Match (%.0f%%): %s - %s -> %s - %s [%s]",
            combined * 100, artist, title, t_artists, t_title, uri,
        )
        return uri
    logging.warning("No Spotify match for: %s - %s", artist, title)
    return None


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_setup(config):
    """Run interactive OAuth authorization."""
    sp = get_spotify_client(config)
    user = sp.current_user()
    print(f"Authenticated as: {user['display_name']} ({user['id']})")
    playlist_id = config.get("spotify", "playlist_id")
    pl = sp.playlist(playlist_id)
    print(f"Playlist: {pl['name']}")
    # Test the new /items endpoint
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/items?limit=1"
    r = requests.get(url, headers=_spotify_headers(config))
    if r.ok:
        print(f"Playlist items API: OK ({r.json().get('total', '?')} tracks)")
    else:
        print(f"Playlist items API: {r.status_code} {r.text}")
    print("Setup complete. You can now run via cron.")


def cmd_stats(config):
    """Print database statistics."""
    db_path = config.get("settings", "db_path", fallback="history.db")
    conn = init_db(db_path)
    total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    added = conn.execute("SELECT COUNT(*) FROM tracks WHERE added_to_playlist = 1").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM tracks WHERE added_to_playlist = 0").fetchone()[0]
    print(f"Total tracks:          {total}")
    print(f"Added to playlist:     {added}")
    print(f"Pending:               {pending}")
    recent = conn.execute(
        "SELECT artist, title, added_to_playlist, created_at FROM tracks ORDER BY id DESC LIMIT 10"
    ).fetchall()
    if recent:
        print("\nRecent tracks:")
        for artist, title, added_flag, created_at in recent:
            status = "+" if added_flag else "-"
            print(f"  [{status}] {artist} - {title}  ({created_at})")
    conn.close()


def cmd_run(config, dry_run=False):
    """Main run: fetch metadata, search Spotify, add to playlist."""
    stream_url = config.get("stream", "stream_url")
    timeout = config.getint("settings", "timeout", fallback=10)

    artist, title = get_now_playing(stream_url, timeout)
    if not artist or not title:
        logging.info("No metadata available.")
        return

    db_path = config.get("settings", "db_path", fallback="history.db")
    conn = init_db(db_path)

    existing = track_exists(conn, artist, title)
    if existing:
        logging.info("Already seen: %s - %s (added=%s)", artist, title, bool(existing[1]))
        conn.close()
        return

    uri = None
    added = False
    try:
        sp = get_spotify_client(config)
        uri = search_spotify(sp, artist, title)
    except Exception as e:
        logging.error("Spotify unavailable: %s", e)

    if dry_run:
        print(f"Now playing: {artist} - {title}")
        print(f"Spotify URI: {uri or 'not found'}")
        print("(dry run, no changes made)")
        conn.close()
        return

    if not uri:
        logging.info("Skipping (no Spotify match): %s - %s", artist, title)
        conn.close()
        return

    playlist_id = config.get("spotify", "playlist_id")
    added = False
    try:
        playlist_add_items(config, playlist_id, [uri])
        added = True
        logging.info("Added to playlist: %s - %s", artist, title)
    except Exception as e:
        logging.error("Failed to add to playlist: %s", e)

    save_track(conn, artist, title, uri, added)
    conn.close()


def cmd_retry(config):
    """Retry: search missing URIs on Spotify and add pending tracks to playlist."""
    import time
    db_path = config.get("settings", "db_path", fallback="history.db")
    conn = init_db(db_path)
    sp = get_spotify_client(config)
    playlist_id = config.get("spotify", "playlist_id")

    # First: search Spotify for tracks saved without a URI, remove if still not found
    no_uri = conn.execute(
        "SELECT id, artist, title FROM tracks WHERE spotify_uri IS NULL"
    ).fetchall()
    if no_uri:
        print(f"Searching Spotify for {len(no_uri)} tracks without URI...")
        for i, (row_id, artist, title) in enumerate(no_uri):
            if i > 0:
                time.sleep(1)  # throttle to avoid 429
            uri = search_spotify(sp, artist, title)
            if uri:
                conn.execute("UPDATE tracks SET spotify_uri = ? WHERE id = ?", (uri, row_id))
                conn.commit()
                print(f"  [found] {artist} - {title}")
            else:
                conn.execute("DELETE FROM tracks WHERE id = ?", (row_id,))
                conn.commit()
                print(f"  [removed] {artist} - {title}")

    # Then: add tracks with URI but not yet in playlist, one at a time with throttle
    pending = conn.execute(
        "SELECT id, artist, title, spotify_uri FROM tracks WHERE spotify_uri IS NOT NULL AND added_to_playlist = 0"
    ).fetchall()
    if not pending:
        print("No pending tracks to add to playlist.")
        conn.close()
        return

    ok, fail = 0, 0
    for i, (row_id, artist, title, uri) in enumerate(pending):
        if i > 0:
            time.sleep(2)  # throttle to avoid 429
        try:
            playlist_add_items(config, playlist_id, [uri])
            conn.execute("UPDATE tracks SET added_to_playlist = 1 WHERE id = ?", (row_id,))
            conn.commit()
            ok += 1
            print(f"  [+] {artist} - {title}")
        except Exception as e:
            fail += 1
            print(f"  [!] {artist} - {title}: {e}")
    conn.close()
    print(f"\nDone: {ok} added, {fail} failed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Jazz Groove to Spotify playlist sync")
    parser.add_argument("--setup", action="store_true", help="Run OAuth setup")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--retry", action="store_true", help="Retry adding pending tracks to playlist")
    parser.add_argument("--dry-run", action="store_true", help="Test without modifying playlist or database")
    args = parser.parse_args()

    config = load_config()
    setup_logging(config)

    if args.setup:
        cmd_setup(config)
        return
    if args.stats:
        cmd_stats(config)
        return
    if args.retry:
        cmd_retry(config)
        return

    lock_fp = acquire_lock()
    try:
        cmd_run(config, dry_run=args.dry_run)
    finally:
        release_lock(lock_fp)


if __name__ == "__main__":
    main()
