"""
database_setup.py
SI 201 Final Project - Database Setup

This file creates the database and all tables.
Run this ONCE before running any data gathering scripts.

Team Member Responsible: [Member 1 Name]
"""

import sqlite3

DB_NAME = 'music_income.db'


def create_database():
    """
    Create the SQLite database and all required tables.
    
    Tables:
    1. regions - Census Bureau income data (100+ rows)
    2. genres - Normalized genre names
    3. tracks - Deezer music data (100+ rows)
    4. track_genres - Junction table linking tracks to genres
    5. regional_popularity - Links regions to tracks (100+ rows)
    
    Returns:
        None
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Table 1: Regions (from Census Bureau API)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regions (
            region_id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT UNIQUE,
            state_code TEXT,
            median_income REAL,
            population INTEGER
        )
    ''')
    
    # Table 2: Genres (normalized - no duplicate strings)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE
        )
    ''')
    
    # Table 3: Tracks (from Deezer API)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            track_id INTEGER PRIMARY KEY AUTOINCREMENT,
            deezer_id INTEGER UNIQUE,
            title TEXT,
            artist_name TEXT,
            duration INTEGER,
            popularity INTEGER
        )
    ''')
    
    # Table 4: Track Genres (Junction table - satisfies "2 tables with integer key" requirement)
    # Links tracks table to genres table via integer foreign keys
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS track_genres (
            track_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id),
            FOREIGN KEY (genre_id) REFERENCES genres(genre_id),
            PRIMARY KEY (track_id, genre_id)
        )
    ''')
    
    # Table 5: Regional Popularity (links regions to tracks)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regional_popularity (
            popularity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_id INTEGER,
            track_id INTEGER,
            rank INTEGER,
            FOREIGN KEY (region_id) REFERENCES regions(region_id),
            FOREIGN KEY (track_id) REFERENCES tracks(track_id)
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("="*60)
    print("DATABASE SETUP COMPLETE")
    print("="*60)
    print(f"Database: {DB_NAME}")
    print("\nTables created:")
    print("  1. regions (region_id, region_name, state_code, median_income, population)")
    print("  2. genres (genre_id, genre_name)")
    print("  3. tracks (track_id, deezer_id, title, artist_name, duration, popularity)")
    print("  4. track_genres (track_id, genre_id) - Junction table")
    print("  5. regional_popularity (popularity_id, region_id, track_id, rank)")
    print("="*60)


def check_database_status():
    """
    Print current row counts for all tables.
    
    Returns:
        dict: Table names and their row counts
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    tables = ['regions', 'genres', 'tracks', 'track_genres', 'regional_popularity']
    counts = {}
    
    print("\n" + "="*60)
    print("DATABASE STATUS")
    print("="*60)
    
    for table in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            counts[table] = count
            status = "✓" if count >= 100 else "⚠️"
            print(f"  {status} {table}: {count} rows")
        except sqlite3.OperationalError:
            counts[table] = 0
            print(f"  ✗ {table}: Table does not exist")
    
    conn.close()
    
    print("="*60)
    print("Note: Need 100+ rows in tracks, regions, and regional_popularity")
    print("="*60)
    
    return counts


if __name__ == "__main__":
    create_database()
    check_database_status()
