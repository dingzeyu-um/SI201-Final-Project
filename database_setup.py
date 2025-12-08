"""
database_setup.py
SI 201 Final Project - Database Setup

This file creates the database and all tables.
Run this ONCE before running any data gathering scripts.

Analysis: The Collaboration Effect in Music
Research Question: Do songs with featured artists have higher popularity?

Team Member Responsible: [Member 1 Name]
"""

import sqlite3

DB_NAME = 'music_collab.db'


def create_database():
    """
    Create the SQLite database and all required tables.

    Tables:
    1. tracks - Deezer music data with collaboration info (100+ rows)
    2. genres - Normalized genre names
    3. track_genres - Junction table linking tracks to genres (100+ rows)
    4. itunes_tracks - iTunes cross-validation data (100+ rows)
    5. collaboration_stats - Aggregated stats for analysis

    Returns:
        None
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Table 1: Tracks (from Deezer API) - with collaboration fields
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            track_id INTEGER PRIMARY KEY AUTOINCREMENT,
            deezer_id INTEGER UNIQUE,
            title TEXT,
            artist_name TEXT,
            duration INTEGER,
            popularity INTEGER,
            is_collaboration INTEGER DEFAULT 0,
            collab_indicator TEXT,
            featuring_artist TEXT
        )
    ''')

    # Table 2: Genres (normalized - no duplicate strings)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE
        )
    ''')

    # Table 3: Track Genres (Junction table - satisfies "2 tables with integer key" requirement)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS track_genres (
            track_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id),
            FOREIGN KEY (genre_id) REFERENCES genres(genre_id),
            PRIMARY KEY (track_id, genre_id)
        )
    ''')

    # Table 4: iTunes Tracks (from iTunes Search API - for cross-validation)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS itunes_tracks (
            itunes_id INTEGER PRIMARY KEY,
            track_id INTEGER,
            itunes_name TEXT,
            itunes_artist TEXT,
            itunes_price REAL,
            itunes_genre TEXT,
            release_date TEXT,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id)
        )
    ''')

    # Table 5: Collaboration Stats (aggregated analysis data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS collaboration_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_id INTEGER,
            is_collaboration INTEGER,
            track_count INTEGER,
            avg_popularity REAL,
            median_popularity REAL,
            FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
        )
    ''')

    conn.commit()
    conn.close()

    print("="*60)
    print("DATABASE SETUP COMPLETE")
    print("="*60)
    print(f"Database: {DB_NAME}")
    print("\nTables created:")
    print("  1. tracks (track_id, deezer_id, title, artist_name, duration, popularity,")
    print("            is_collaboration, collab_indicator, featuring_artist)")
    print("  2. genres (genre_id, genre_name)")
    print("  3. track_genres (track_id, genre_id) - Junction table")
    print("  4. itunes_tracks (itunes_id, track_id, ...) - Cross-validation")
    print("  5. collaboration_stats (stat_id, genre_id, is_collaboration, ...)")
    print("="*60)


def check_database_status():
    """
    Print current row counts for all tables.

    Returns:
        dict: Table names and their row counts
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    tables = ['tracks', 'genres', 'track_genres', 'itunes_tracks', 'collaboration_stats']
    min_requirements = {
        'tracks': 100,
        'genres': 5,
        'track_genres': 100,
        'itunes_tracks': 100,
        'collaboration_stats': 10
    }
    counts = {}

    print("\n" + "="*60)
    print("DATABASE STATUS")
    print("="*60)

    for table in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            counts[table] = count
            min_req = min_requirements.get(table, 100)
            status = "good" if count >= min_req else "need"
            print(f"  {status} {table}: {count} rows (need {min_req}+)")
        except sqlite3.OperationalError:
            counts[table] = 0
            print(f"  ✗ {table}: Table does not exist")

    # Show collaboration breakdown if tracks exist
    try:
        cursor.execute('SELECT is_collaboration, COUNT(*) FROM tracks GROUP BY is_collaboration')
        collab_counts = cursor.fetchall()
        if collab_counts:
            print("\n  Collaboration breakdown:")
            for is_collab, cnt in collab_counts:
                label = "Collaborations" if is_collab else "Solo tracks"
                print(f"    {label}: {cnt}")
    except sqlite3.OperationalError:
        pass

    conn.close()

    print("="*60)
    print("Note: Need 100+ rows in tracks, track_genres, and itunes_tracks")
    print("="*60)

    return counts


if __name__ == "__main__":
    create_database()
    check_database_status()
