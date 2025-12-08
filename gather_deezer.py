"""
gather_deezer.py
SI 201 Final Project - Deezer API Data Gathering

This file fetches music data from the Deezer API and stores it in the database.
Run this file MULTIPLE TIMES to gather 100+ tracks (25 per run).

API: https://api.deezer.com/search
Authentication: None required

Analysis: The Collaboration Effect in Music
- Detects collaborations from track titles (feat., ft., &, x, with)
- Stores collaboration metadata for analysis

Team Member Responsible: [Member 1 Name]
"""

import sqlite3
import requests
import re

DB_NAME = 'music_collab.db'

# Debug mode - set to False for production (25 item limit per SI 201 requirements)
DEBUG_MODE = True
MAX_ITEMS_PER_RUN = 200 if DEBUG_MODE else 25

# Patterns to detect collaborations in track titles
COLLAB_PATTERNS = [
    (r'\(feat\.?\s+([^)]+)\)', 'feat.'),
    (r'\[feat\.?\s+([^\]]+)\]', 'feat.'),
    (r'\s+feat\.?\s+(.+?)(?:\s*[-–]|$)', 'feat.'),
    (r'\s+ft\.?\s+(.+?)(?:\s*[-–]|$)', 'ft.'),
    (r'\s+featuring\s+(.+?)(?:\s*[-–]|$)', 'featuring'),
    (r'\(with\s+([^)]+)\)', 'with'),
    (r'\s+with\s+([A-Z][^,\-]+?)(?:\s*[-–,]|$)', 'with'),
]

# Patterns that indicate collaboration but don't extract artist name
COLLAB_INDICATORS = [
    (r'\s+[x×]\s+', 'x'),
    (r'\s+&\s+', '&'),
]


def detect_collaboration(title):
    """
    Detect if a track title indicates a collaboration.

    Args:
        title (str): Track title

    Returns:
        tuple: (is_collaboration, indicator, featuring_artist)
    """
    title_lower = title.lower()

    # Try patterns that extract featured artist name
    for pattern, indicator in COLLAB_PATTERNS:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            featuring_artist = match.group(1).strip()
            return (1, indicator, featuring_artist)

    # Try patterns that just indicate collaboration
    for pattern, indicator in COLLAB_INDICATORS:
        if re.search(pattern, title):
            return (1, indicator, None)

    return (0, None, None)


def fetch_deezer_charts():
    """
    Fetch track data from Deezer API using multiple search queries.
    Uses different genre/artist searches to collect diverse tracks.

    API Endpoint: GET https://api.deezer.com/search?q=QUERY

    Returns:
        list[dict]: List of track dictionaries, or empty list if error
    """
    # Search queries - collaboration-focused searches FIRST to maximize collab detection
    search_queries = [
        # Collaboration-focused searches (artists known for collabs)
        'DJ Khaled', 'Calvin Harris', 'David Guetta', 'Pitbull',
        'feat', 'featuring', 'ft.', 'remix',
        # Genre searches
        'hip hop', 'pop hits', 'r&b', 'rock', 'country music',
        # Popular artists
        'Drake', 'Taylor Swift', 'Ed Sheeran', 'Beyonce', 'Eminem',
        'The Weeknd', 'Post Malone', 'Bruno Mars', 'Ariana Grande',
        'Justin Bieber', 'Kendrick Lamar', 'Bad Bunny',
        'top 2024', 'hit songs', 'viral tracks'
    ]

    # Check which queries have been used (stored in DB)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tracks')
    track_count = cursor.fetchone()[0]
    conn.close()

    # Pick a query based on current track count to get different results each run
    query_index = (track_count // 10) % len(search_queries)
    query = search_queries[query_index]

    url = f"https://api.deezer.com/search?q={query}&limit=25"

    try:
        print(f"Fetching data from Deezer API (search: '{query}')...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        tracks = []
        track_data = data.get('data', [])

        for track in track_data:
            tracks.append({
                'deezer_id': track['id'],
                'title': track['title'],
                'artist_name': track['artist']['name'],
                'duration': track.get('duration', 0),
                'popularity': track.get('rank', 0)
            })

        print(f"✓ Retrieved {len(tracks)} tracks from Deezer")
        return tracks

    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching from Deezer API: {e}")
        print("  Please check your internet connection and try again.")
        return []


def store_tracks(tracks):
    """
    Store track data in the database with collaboration detection.
    Limits to MAX_ITEMS_PER_RUN (25) per execution.

    Args:
        tracks (list[dict]): Track dictionaries from fetch_deezer_charts()

    Returns:
        int: Number of new tracks stored
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    stored_count = 0
    collab_count = 0

    # Limit to 25 items per run (project requirement)
    for track in tracks[:MAX_ITEMS_PER_RUN]:
        # Detect collaboration from title
        is_collab, indicator, featuring = detect_collaboration(track['title'])

        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tracks
                (deezer_id, title, artist_name, duration, popularity,
                 is_collaboration, collab_indicator, featuring_artist)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                track['deezer_id'],
                track['title'],
                track['artist_name'],
                track['duration'],
                track['popularity'],
                is_collab,
                indicator,
                featuring
            ))

            if cursor.rowcount > 0:
                stored_count += 1
                if is_collab:
                    collab_count += 1

                # If track has genre info (from simulated data), store it
                if 'genre' in track:
                    track_id = cursor.lastrowid
                    store_genre_for_track(cursor, track_id, track['genre'])

        except sqlite3.IntegrityError:
            # Track already exists, skip
            continue

    conn.commit()

    # Get total counts
    cursor.execute('SELECT COUNT(*) FROM tracks')
    total = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM tracks WHERE is_collaboration = 1')
    total_collabs = cursor.fetchone()[0]

    conn.close()

    print(f"Stored {stored_count} new tracks ({collab_count} collaborations)")
    print(f"Total: {total} tracks ({total_collabs} collaborations)")

    return stored_count


def store_genre_for_track(cursor, track_id, genre_name):
    """
    Store genre and link it to track.
    Helper function called from store_tracks().
    
    Args:
        cursor: Database cursor
        track_id (int): Track ID
        genre_name (str): Genre name
    """
    # Insert genre if not exists
    cursor.execute('''
        INSERT OR IGNORE INTO genres (genre_name) VALUES (?)
    ''', (genre_name,))
    
    # Get genre_id
    cursor.execute('SELECT genre_id FROM genres WHERE genre_name = ?', (genre_name,))
    result = cursor.fetchone()
    
    if result:
        genre_id = result[0]
        # Link track to genre
        cursor.execute('''
            INSERT OR IGNORE INTO track_genres (track_id, genre_id)
            VALUES (?, ?)
        ''', (track_id, genre_id))


def main():
    """
    Main function to fetch and store Deezer data.
    Run this multiple times to collect 100+ tracks.
    """
    print("\n" + "="*60)
    print("DEEZER API DATA GATHERING")
    print("Analysis: The Collaboration Effect in Music")
    print(f"Maximum items per run: {MAX_ITEMS_PER_RUN}")
    print("="*60)

    # Fetch data from API
    tracks = fetch_deezer_charts()

    if tracks:
        # Store in database
        store_tracks(tracks)

        # Check status
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM tracks')
        total = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM tracks WHERE is_collaboration = 1')
        collabs = cursor.fetchone()[0]
        conn.close()

        print("\n" + "="*60)
        if total >= 100:
            print(f"COMPLETE: {total} tracks in database (100+ required)")
            print(f"  Solo tracks: {total - collabs}")
            print(f"  Collaborations: {collabs}")
        else:
            remaining = 100 - total
            runs_needed = (remaining // MAX_ITEMS_PER_RUN) + 1
            print(f"Need {remaining} more tracks")
            print(f"Run this script {runs_needed} more time(s)")
        print("="*60)
    else:
        print("No tracks retrieved - check internet connection")


if __name__ == "__main__":
    main()
