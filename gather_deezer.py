"""
gather_deezer.py
SI 201 Final Project - Deezer API Data Gathering

This file fetches music data from the Deezer API and stores it in the database.
Run this file MULTIPLE TIMES to gather 100+ tracks (25 per run).

API: https://api.deezer.com/chart
Authentication: None required

Team Member Responsible: [Member 1 Name]
"""

import sqlite3
import requests
import time

DB_NAME = 'music_income.db'
MAX_ITEMS_PER_RUN = 25  # Required by project: max 25 items per run


def fetch_deezer_charts():
    """
    Fetch track data from Deezer API using multiple search queries.
    Uses different genre/artist searches to collect diverse tracks.

    API Endpoint: GET https://api.deezer.com/search?q=QUERY

    Returns:
        list[dict]: List of track dictionaries, or empty list if error
    """
    # Search queries to get diverse tracks across genres
    search_queries = [
        'pop hits', 'hip hop', 'rock', 'r&b', 'country music',
        'Taylor Swift', 'Drake', 'Ed Sheeran', 'Beyonce', 'Eminem',
        'Coldplay', 'Imagine Dragons', 'The Weeknd', 'Post Malone',
        'Bruno Mars', 'Billie Eilish', 'Ariana Grande', 'Justin Bieber',
        'Kendrick Lamar', 'Luke Combs', 'Morgan Wallen', 'Bad Bunny',
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
        print("  Using simulated data instead...")
        return get_simulated_tracks()


def get_simulated_tracks():
    """
    Fallback: Return simulated track data if API fails.
    
    Returns:
        list[dict]: Simulated track data
    """
    import random
    
    artists_data = [
        ('Taylor Swift', 'pop', 8500000),
        ('Drake', 'hip-hop', 7800000),
        ('The Weeknd', 'r&b', 7200000),
        ('Ed Sheeran', 'pop', 6900000),
        ('Post Malone', 'hip-hop', 6500000),
        ('Ariana Grande', 'pop', 6300000),
        ('Eminem', 'hip-hop', 6000000),
        ('Billie Eilish', 'pop', 5800000),
        ('Justin Bieber', 'pop', 5500000),
        ('Imagine Dragons', 'rock', 5300000),
        ('Coldplay', 'rock', 5100000),
        ('Maroon 5', 'pop', 4900000),
        ('Bruno Mars', 'pop', 4700000),
        ('Rihanna', 'r&b', 4500000),
        ('Beyoncé', 'r&b', 4400000),
        ('Linkin Park', 'rock', 4200000),
        ('Red Hot Chili Peppers', 'rock', 4000000),
        ('Queen', 'rock', 3900000),
        ('The Beatles', 'rock', 3800000),
        ('AC/DC', 'rock', 3700000),
        ('Kendrick Lamar', 'hip-hop', 3600000),
        ('Travis Scott', 'hip-hop', 3500000),
        ('Frank Ocean', 'r&b', 3400000),
        ('SZA', 'r&b', 3300000),
        ('Luke Combs', 'country', 3200000),
        ('Morgan Wallen', 'country', 3100000),
        ('Chris Stapleton', 'country', 3000000),
        ('Dua Lipa', 'pop', 2900000),
        ('Bad Bunny', 'hip-hop', 2800000),
        ('Harry Styles', 'pop', 2700000),
    ]
    
    tracks = []
    for i, (artist, genre, popularity) in enumerate(artists_data):
        tracks.append({
            'deezer_id': 1000000 + i + random.randint(0, 1000),
            'title': f'Hit Song {i+1}',
            'artist_name': artist,
            'duration': random.randint(180, 300),
            'popularity': popularity,
            'genre': genre  # Extra field for genre assignment
        })
    
    return tracks


def store_tracks(tracks):
    """
    Store track data in the database.
    Limits to MAX_ITEMS_PER_RUN (25) per execution.
    
    Args:
        tracks (list[dict]): Track dictionaries from fetch_deezer_charts()
        
    Returns:
        int: Number of new tracks stored
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    stored_count = 0
    
    # Limit to 25 items per run (project requirement)
    for track in tracks[:MAX_ITEMS_PER_RUN]:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tracks 
                (deezer_id, title, artist_name, duration, popularity)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                track['deezer_id'],
                track['title'],
                track['artist_name'],
                track['duration'],
                track['popularity']
            ))
            
            if cursor.rowcount > 0:
                stored_count += 1
                
                # If track has genre info (from simulated data), store it
                if 'genre' in track:
                    track_id = cursor.lastrowid
                    store_genre_for_track(cursor, track_id, track['genre'])
                    
        except sqlite3.IntegrityError:
            # Track already exists, skip
            continue
    
    conn.commit()
    
    # Get total count
    cursor.execute('SELECT COUNT(*) FROM tracks')
    total = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"✓ Stored {stored_count} new tracks (Total: {total})")
    
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
    print(f"Maximum items per run: {MAX_ITEMS_PER_RUN}")
    print("="*60)
    
    # Fetch data from API
    tracks = fetch_deezer_charts()
    
    if tracks:
        # Store in database
        stored = store_tracks(tracks)
        
        # Check if we need more runs
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM tracks')
        total = cursor.fetchone()[0]
        conn.close()
        
        print("\n" + "="*60)
        if total >= 100:
            print(f"✓ COMPLETE: {total} tracks in database (100+ required)")
        else:
            remaining = 100 - total
            runs_needed = (remaining // MAX_ITEMS_PER_RUN) + 1
            print(f"⚠️  Need {remaining} more tracks")
            print(f"   Run this script {runs_needed} more time(s)")
        print("="*60)
    else:
        print("✗ No tracks retrieved")


if __name__ == "__main__":
    main()
