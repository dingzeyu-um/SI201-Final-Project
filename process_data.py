"""
process_data.py
SI 201 Final Project - Data Processing

This file creates relationships between regions and tracks,
simulating regional music preferences based on income levels.

Run this AFTER all data has been gathered (100+ in each table).

Team Member Responsible: [Member 1 Name]
"""

import sqlite3
import numpy as np

DB_NAME = 'music_income.db'
MAX_ITEMS_PER_RUN = 25  # For consistency, though this creates many records


def normalize_genre(genre_name):
    """
    Map MusicBrainz tags to standard genre categories.

    Args:
        genre_name (str): Raw genre name from MusicBrainz

    Returns:
        str: Normalized genre category
    """
    genre_lower = genre_name.lower()

    # Define mappings from MusicBrainz tags to standard genres
    mappings = {
        'hip-hop': ['hip hop', 'hip-hop', 'rap', 'trap', 'hip hop soul'],
        'rock': ['rock', 'alternative rock', 'hard rock', 'alternative metal', 'industrial metal'],
        'pop': ['pop', 'art pop', 'folk pop'],
        'r&b': ['r&b', 'contemporary r&b', 'soul'],
        'latin': ['reggaeton', 'latin urban', 'norteño', 'flamenco'],
        'country': ['country', 'canadian'],
        'electronic': ['electronic', 'edm'],
        'jazz': ['jazz'],
        'classical': ['classical'],
    }

    for standard_genre, variants in mappings.items():
        if genre_lower in variants or any(v in genre_lower for v in variants):
            return standard_genre

    return 'other'


def create_regional_preferences():
    """
    Create relationships between regions and tracks based on income.
    This simulates the correlation between income and music preferences.
    
    Income-based preferences:
    - Low income (<$55k): Prefer hip-hop, pop, r&b
    - Middle income ($55k-$70k): Balanced preferences
    - High income (>$70k): Prefer rock, classical, jazz
    
    Returns:
        int: Number of preference records created
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Get all regions with income data
    cursor.execute('SELECT region_id, median_income FROM regions')
    regions = cursor.fetchall()
    
    if not regions:
        print("✗ No regions found! Run gather_census.py first.")
        conn.close()
        return 0
    
    # Get all tracks with their genres
    cursor.execute('''
        SELECT t.track_id, g.genre_name, t.popularity
        FROM tracks t
        JOIN track_genres tg ON t.track_id = tg.track_id
        JOIN genres g ON tg.genre_id = g.genre_id
    ''')
    tracks = cursor.fetchall()
    
    if not tracks:
        print("✗ No tracks with genres found!")
        print("  Run gather_deezer.py and gather_musicbrainz.py first.")
        conn.close()
        return 0
    
    print(f"Processing {len(regions)} regions and {len(tracks)} tracks...")
    
    # Define genre weights by income bracket (using normalized genre names)
    low_income_weights = {
        'hip-hop': 2.5, 'pop': 2.0, 'r&b': 1.8, 'latin': 1.6,
        'country': 1.5, 'rock': 1.0, 'jazz': 0.5,
        'classical': 0.3, 'electronic': 1.2, 'other': 1.0
    }

    middle_income_weights = {
        'pop': 2.2, 'hip-hop': 1.8, 'rock': 1.8, 'latin': 1.5,
        'r&b': 1.5, 'country': 1.3, 'jazz': 0.8,
        'classical': 0.6, 'electronic': 1.5, 'other': 1.0
    }

    high_income_weights = {
        'rock': 2.5, 'pop': 1.8, 'classical': 1.5, 'jazz': 1.5,
        'r&b': 1.2, 'hip-hop': 1.0, 'latin': 1.0,
        'country': 0.8, 'electronic': 1.0, 'other': 1.0
    }
    
    created_count = 0
    
    for region_id, income in regions:
        # Determine income bracket and weights
        if income < 55000:
            weights = low_income_weights
        elif income < 70000:
            weights = middle_income_weights
        else:
            weights = high_income_weights
        
        # For each track, decide if it's popular in this region
        for track_id, genre, popularity in tracks:
            normalized_genre = normalize_genre(genre)
            weight = weights.get(normalized_genre, 1.0)
            
            # Probability of track being popular in this region
            # Higher weight = higher probability
            probability = 0.25 * weight  # Base 25% chance, modified by weight
            
            if np.random.random() < probability:
                # Calculate rank based on popularity and weight
                rank = int(popularity / (1000 * weight))
                
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO regional_popularity 
                        (region_id, track_id, rank)
                        VALUES (?, ?, ?)
                    ''', (region_id, track_id, rank))
                    
                    if cursor.rowcount > 0:
                        created_count += 1
                except sqlite3.IntegrityError:
                    continue
    
    conn.commit()
    
    # Get total count
    cursor.execute('SELECT COUNT(*) FROM regional_popularity')
    total = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"✓ Created {created_count} new preference records (Total: {total})")
    
    return created_count


def check_data_status():
    """
    Check if all required data is present before processing.
    
    Returns:
        bool: True if ready to process, False otherwise
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("DATA STATUS CHECK")
    print("="*60)
    
    # Check each table
    tables = {
        'tracks': 100,
        'regions': 50,  # Only ~52 states available
        'track_genres': 100,
        'genres': 1
    }
    
    all_ready = True
    
    for table, min_required in tables.items():
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        
        if count >= min_required:
            print(f"  ✓ {table}: {count} rows")
        else:
            print(f"  ✗ {table}: {count} rows (need {min_required}+)")
            all_ready = False
    
    conn.close()
    
    print("="*60)
    
    return all_ready


def main():
    """
    Main function to create regional preferences.
    """
    print("\n" + "="*60)
    print("DATA PROCESSING - REGIONAL PREFERENCES")
    print("="*60)
    
    # Check if data is ready
    if not check_data_status():
        print("\n✗ Not enough data to process!")
        print("  Please run the gather scripts first:")
        print("    1. python gather_deezer.py (run 4+ times)")
        print("    2. python gather_census.py (run 2-3 times)")
        print("    3. python gather_musicbrainz.py (run 4+ times)")
        return
    
    # Create regional preferences
    print("\nCreating regional music preferences...")
    created = create_regional_preferences()
    
    # Final status
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM regional_popularity')
    total = cursor.fetchone()[0]
    conn.close()
    
    print("\n" + "="*60)
    if total >= 100:
        print(f"✓ COMPLETE: {total} regional preferences created")
        print("  Ready to run analyze_visualize.py")
    else:
        print(f"⚠️  Only {total} preferences created")
        print("  May need more data in tracks/regions tables")
    print("="*60)


if __name__ == "__main__":
    main()
