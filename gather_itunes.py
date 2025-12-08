"""
gather_itunes.py
SI 201 Final Project - iTunes Search API Data Gathering

This file fetches music data from the iTunes Search API for cross-validation.
Run this file MULTIPLE TIMES to gather 100+ iTunes track matches (25 per run).

API: https://itunes.apple.com/search
Authentication: None required

Analysis: The Collaboration Effect in Music
- Cross-validates Deezer track data with iTunes
- Provides additional metadata (genre, price, release date)

Team Member Responsible: [Member 2 Name]
"""

import sqlite3
import requests
import time
import urllib.parse

DB_NAME = 'music_collab.db'

# Debug mode - set to False for production (25 item limit per SI 201 requirements)
DEBUG_MODE = True
MAX_ITEMS_PER_RUN = 100 if DEBUG_MODE else 25
RATE_LIMIT_DELAY = 2.5  # Seconds between requests (increased to avoid 429 errors)
MAX_RETRIES = 3  # Number of retries for rate limiting


def get_tracks_without_itunes():
    """
    Get tracks from database that don't have iTunes data yet.

    Returns:
        list[tuple]: List of (track_id, title, artist_name)
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT t.track_id, t.title, t.artist_name
        FROM tracks t
        LEFT JOIN itunes_tracks it ON t.track_id = it.track_id
        WHERE it.itunes_id IS NULL
        LIMIT ?
    ''', (MAX_ITEMS_PER_RUN,))

    tracks = cursor.fetchall()
    conn.close()

    return tracks


def search_itunes(title, artist):
    """
    Search iTunes for a track by title and artist.

    API Endpoint: GET https://itunes.apple.com/search
    Parameters: term, media=music, entity=song, limit=1

    Args:
        title (str): Track title
        artist (str): Artist name

    Returns:
        dict: iTunes track data or None if not found
    """
    # Clean up title - remove featuring info for better matching
    clean_title = title.split('(')[0].split('[')[0].strip()
    clean_title = clean_title.split(' feat')[0].split(' ft.')[0].strip()

    # Build search query
    search_term = f"{clean_title} {artist}"
    encoded_term = urllib.parse.quote(search_term)

    url = f"https://itunes.apple.com/search?term={encoded_term}&media=music&entity=song&limit=3"

    headers = {'User-Agent': 'SI201MusicProject/1.0 (Educational)'}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            # Handle rate limiting and forbidden errors with exponential backoff
            if response.status_code in [403, 429]:
                if attempt < MAX_RETRIES - 1:
                    delay = RATE_LIMIT_DELAY * (2 ** attempt)
                    print(f"  ⏳ Rate limited, waiting {delay}s...")
                    time.sleep(delay)
                    continue
                return None

            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            if results:
                # Return first result
                track = results[0]
                return {
                    'itunes_id': track.get('trackId'),
                    'itunes_name': track.get('trackName'),
                    'itunes_artist': track.get('artistName'),
                    'itunes_price': track.get('trackPrice'),
                    'itunes_genre': track.get('primaryGenreName'),
                    'release_date': track.get('releaseDate', '')[:10]  # YYYY-MM-DD
                }

            return None

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = RATE_LIMIT_DELAY * (2 ** attempt)
                print(f"  ⏳ Retry {attempt + 1}/{MAX_RETRIES}, waiting {delay}s...")
                time.sleep(delay)
                continue
            print(f"  ✗ Error searching iTunes: {e}")
            return None

    return None


def store_itunes_track(track_id, itunes_data):
    """
    Store iTunes track data in the database.

    Args:
        track_id (int): Our track ID to link to
        itunes_data (dict): Data from search_itunes()

    Returns:
        bool: True if stored successfully
    """
    if not itunes_data:
        return False

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT OR IGNORE INTO itunes_tracks
            (itunes_id, track_id, itunes_name, itunes_artist,
             itunes_price, itunes_genre, release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            itunes_data['itunes_id'],
            track_id,
            itunes_data['itunes_name'],
            itunes_data['itunes_artist'],
            itunes_data['itunes_price'],
            itunes_data['itunes_genre'],
            itunes_data['release_date']
        ))

        conn.commit()
        success = cursor.rowcount > 0
        conn.close()
        return success

    except sqlite3.IntegrityError:
        conn.close()
        return False


def main():
    """
    Main function to fetch and store iTunes data.
    Run this multiple times to match all tracks.
    """
    print("\n" + "="*60)
    print("ITUNES SEARCH API DATA GATHERING")
    print("Analysis: The Collaboration Effect in Music")
    print(f"Maximum items per run: {MAX_ITEMS_PER_RUN}")
    print("="*60)

    # Get tracks that need iTunes data
    tracks = get_tracks_without_itunes()

    if not tracks:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM tracks')
        total_tracks = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM itunes_tracks')
        itunes_count = cursor.fetchone()[0]
        conn.close()

        if total_tracks == 0:
            print("\n✗ No tracks in database!")
            print("  Run gather_deezer.py first to collect tracks.")
        else:
            print(f"\n✓ All {total_tracks} tracks already have iTunes data")
            print(f"  iTunes matches: {itunes_count}")
        print("="*60)
        return

    print(f"\nSearching iTunes for {len(tracks)} tracks...")

    stored_count = 0
    for track_id, title, artist in tracks:
        print(f"  Searching: {artist} - {title[:30]}...", end=" ")

        itunes_data = search_itunes(title, artist)

        if itunes_data:
            if store_itunes_track(track_id, itunes_data):
                print(f"✓ Found ({itunes_data['itunes_genre']})")
                stored_count += 1
            else:
                print("✗ Duplicate")
        else:
            print("✗ Not found")

        # Rate limiting - iTunes API is strict
        time.sleep(RATE_LIMIT_DELAY)

    # Final status
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM itunes_tracks')
    total_itunes = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM tracks')
    total_tracks = cursor.fetchone()[0]
    conn.close()

    print("\n" + "="*60)
    print(f"Stored {stored_count} new iTunes matches")
    print(f"Total iTunes matches: {total_itunes} / {total_tracks} tracks")

    if total_itunes >= 100:
        print("✓ COMPLETE: 100+ iTunes matches collected")
    else:
        remaining = max(0, 100 - total_itunes)
        if remaining > 0:
            print(f"Need ~{remaining} more matches")
            print("Run this script again to continue")
    print("="*60)


if __name__ == "__main__":
    main()
