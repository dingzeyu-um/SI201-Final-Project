"""
gather_musicbrainz.py
SI 201 Final Project - MusicBrainz API Data Gathering

This file fetches genre metadata from MusicBrainz API and links it to tracks.
Run this file MULTIPLE TIMES to assign genres to 100+ tracks (25 per run).

API: https://musicbrainz.org/ws/2/artist
Authentication: None required (User-Agent header required)
Rate Limit: 1 request per second

Analysis: The Collaboration Effect in Music

Team Member Responsible: [Member 3 Name]
"""

import sqlite3
import requests
import time

DB_NAME = 'music_collab.db'

# Debug mode - set to False for production (25 item limit per SI 201 requirements)
DEBUG_MODE = True
MAX_ITEMS_PER_RUN = 100 if DEBUG_MODE else 25
RATE_LIMIT_DELAY = 1.1  # Seconds between requests (MusicBrainz requires 1/sec)
MAX_RETRIES = 3  # Number of retries for connection errors


def fetch_genre_for_artist(artist_name):
    """
    Fetch genre/tags for an artist from MusicBrainz API.
    
    API Endpoint: GET https://musicbrainz.org/ws/2/artist
    Parameters: query=artist:{name}&fmt=json&limit=1
    
    Args:
        artist_name (str): Name of the artist
        
    Returns:
        str: Genre name, or None if not found
    """
    url = "https://musicbrainz.org/ws/2/artist/"
    params = {
        'query': f'artist:{artist_name}',
        'fmt': 'json',
        'limit': 1
    }
    headers = {
        'User-Agent': 'SI201FinalProject/1.0 (Educational; University of Michigan)'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            # Respect rate limit
            time.sleep(RATE_LIMIT_DELAY)

            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Extract genre from tags
            if data.get('artists') and len(data['artists']) > 0:
                artist = data['artists'][0]
                tags = artist.get('tags', [])

                if tags:
                    # Return highest-scoring tag
                    sorted_tags = sorted(tags, key=lambda x: x.get('count', 0), reverse=True)
                    return sorted_tags[0]['name']

            return None

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Retry {attempt + 1}/{MAX_RETRIES} for {artist_name}...")
                time.sleep(2)  # Wait before retry
                continue
            print(f"    API error for {artist_name}: {e}")
            return None
        except (KeyError, IndexError):
            return None

    return None


def guess_genre_from_name(artist_name):
    """
    Fallback: Guess genre based on artist name keywords.
    Used when API doesn't return a genre.
    
    Args:
        artist_name (str): Artist name
        
    Returns:
        str: Best guess genre
    """
    artist_lower = artist_name.lower()
    
    # Genre keywords mapping
    genre_hints = {
        'hip-hop': ['drake', 'eminem', 'kendrick', 'travis', 'post malone', 
                    'kanye', 'jay-z', 'lil', 'bad bunny', 'migos', 'cardi'],
        'rock': ['imagine dragons', 'coldplay', 'linkin', 'queen', 'beatles', 
                 'chili', 'nirvana', 'foo', 'green day', 'ac/dc', 'metallica'],
        'r&b': ['weeknd', 'rihanna', 'beyoncé', 'beyonce', 'frank ocean', 
                'sza', 'usher', 'chris brown', 'alicia'],
        'country': ['combs', 'wallen', 'stapleton', 'carrie', 'blake', 
                    'kenny', 'dolly', 'garth'],
        'pop': ['swift', 'sheeran', 'grande', 'bieber', 'eilish', 
                'mars', 'dua lipa', 'harry styles', 'katy perry', 'lady gaga'],
        'electronic': ['daft punk', 'avicii', 'marshmello', 'calvin harris', 
                       'deadmau5', 'skrillex', 'tiesto'],
        'jazz': ['miles davis', 'coltrane', 'monk', 'armstrong', 'ella'],
        'classical': ['beethoven', 'mozart', 'bach', 'chopin', 'vivaldi']
    }
    
    for genre, keywords in genre_hints.items():
        for keyword in keywords:
            if keyword in artist_lower:
                return genre
    
    return 'pop'  # Default fallback


def assign_genres_to_tracks():
    """
    Fetch genres from MusicBrainz and link to tracks in database.
    Processes up to MAX_ITEMS_PER_RUN tracks per execution.
    
    Returns:
        int: Number of tracks assigned genres
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Get tracks without genres (up to 25)
    cursor.execute('''
        SELECT t.track_id, t.artist_name
        FROM tracks t
        LEFT JOIN track_genres tg ON t.track_id = tg.track_id
        WHERE tg.genre_id IS NULL
        LIMIT ?
    ''', (MAX_ITEMS_PER_RUN,))
    
    tracks_to_process = cursor.fetchall()
    
    if not tracks_to_process:
        print("All tracks already have genres assigned.")
        conn.close()
        return 0
    
    print(f"Processing {len(tracks_to_process)} tracks...")
    assigned_count = 0
    
    for track_id, artist_name in tracks_to_process:
        print(f"  Looking up: {artist_name}...", end=" ")
        
        # Try MusicBrainz API first
        genre = fetch_genre_for_artist(artist_name)
        
        # Fallback to guessing if API fails
        if not genre:
            genre = guess_genre_from_name(artist_name)
            print(f"(guessed: {genre})")
        else:
            print(f"(found: {genre})")
        
        # Insert genre into genres table
        cursor.execute('''
            INSERT OR IGNORE INTO genres (genre_name) VALUES (?)
        ''', (genre,))
        
        # Get genre_id
        cursor.execute('SELECT genre_id FROM genres WHERE genre_name = ?', (genre,))
        result = cursor.fetchone()
        
        if result:
            genre_id = result[0]
            
            # Link track to genre
            cursor.execute('''
                INSERT OR IGNORE INTO track_genres (track_id, genre_id)
                VALUES (?, ?)
            ''', (track_id, genre_id))
            
            if cursor.rowcount > 0:
                assigned_count += 1
    
    conn.commit()
    
    # Get counts
    cursor.execute('SELECT COUNT(*) FROM track_genres')
    total_assigned = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM tracks')
    total_tracks = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n✓ Assigned genres to {assigned_count} tracks")
    print(f"  Total tracks with genres: {total_assigned}/{total_tracks}")
    
    return assigned_count


def main():
    """
    Main function to fetch genres and link to tracks.
    Run this multiple times until all tracks have genres.
    """
    print("\n" + "="*60)
    print("MUSICBRAINZ API DATA GATHERING")
    print(f"Maximum items per run: {MAX_ITEMS_PER_RUN}")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY} seconds")
    print("="*60)
    
    # Check if tracks exist
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM tracks')
    track_count = cursor.fetchone()[0]
    conn.close()
    
    if track_count == 0:
        print("\nNo tracks in database!")
        print("  Run gather_deezer.py first to add tracks.")
        return
    
    # Assign genres
    assigned = assign_genres_to_tracks()
    
    # Check status
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*) FROM tracks t
        LEFT JOIN track_genres tg ON t.track_id = tg.track_id
        WHERE tg.genre_id IS NULL
    ''')
    remaining = cursor.fetchone()[0]
    
    conn.close()
    
    print("\n" + "="*60)
    if remaining == 0:
        print("COMPLETE: All tracks have genres assigned")
    else:
        runs_needed = (remaining // MAX_ITEMS_PER_RUN) + 1
        print(f"  {remaining} tracks still need genres")
        print(f"  Run this script {runs_needed} more time(s)")
    print("="*60)


if __name__ == "__main__":
    main()
