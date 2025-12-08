# Dingze
"""
process_data.py
SI 201 Final Project - Data Processing

This file processes the collected data and calculates collaboration statistics.
Run this AFTER all gather scripts have collected sufficient data.

Analysis: The Collaboration Effect in Music
- Calculates aggregate statistics by genre and collaboration status
- Prepares data for statistical analysis

Team Member Responsible: [Member 3 Name]
"""

import sqlite3
import numpy as np

DB_NAME = "music_collab.db"


def normalize_genre(genre_name):
    """
    Map genre names to standard categories.

    Args:
        genre_name (str): Raw genre name from MusicBrainz or iTunes

    Returns:
        str: Normalized genre category
    """
    if not genre_name:
        return "other"

    genre_lower = genre_name.lower()

    # Define mappings to standard genres
    mappings = {
        "hip-hop": ["hip hop", "hip-hop", "rap", "trap", "hip hop soul"],
        "rock": [
            "rock",
            "alternative rock",
            "hard rock",
            "alternative metal",
            "industrial metal",
            "alternative",
        ],
        "pop": ["pop", "art pop", "folk pop", "dance", "electropop"],
        "r&b": ["r&b", "contemporary r&b", "soul", "r&b/soul"],
        "latin": ["reggaeton", "latin urban", "latin", "música mexicana"],
        "country": ["country"],
        "electronic": ["electronic", "edm", "house", "techno"],
        "jazz": ["jazz"],
        "classical": ["classical"],
    }

    for standard_genre, variants in mappings.items():
        if genre_lower in variants or any(v in genre_lower for v in variants):
            return standard_genre

    return "other"


def calculate_collaboration_stats():
    """
    Calculate aggregate statistics for collaboration analysis.
    Groups tracks by genre and collaboration status.

    Returns:
        int: Number of stat records created
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Clear existing stats
    cursor.execute("DELETE FROM collaboration_stats")
    print("✓ Cleared existing collaboration stats")

    # Get tracks with genres (prefer iTunes genre, fall back to MusicBrainz)
    cursor.execute("""
        SELECT
            t.track_id,
            t.popularity,
            t.is_collaboration,
            COALESCE(it.itunes_genre, g.genre_name) as genre
        FROM tracks t
        LEFT JOIN itunes_tracks it ON t.track_id = it.track_id
        LEFT JOIN track_genres tg ON t.track_id = tg.track_id
        LEFT JOIN genres g ON tg.genre_id = g.genre_id
    """)

    tracks = cursor.fetchall()

    if not tracks:
        print("✗ No tracks found! Run gather scripts first.")
        conn.close()
        return 0

    print(f"Processing {len(tracks)} tracks...")

    # Group by normalized genre and collaboration status
    stats = {}
    for track_id, popularity, is_collab, genre in tracks:
        normalized = normalize_genre(genre)
        key = (normalized, is_collab)

        if key not in stats:
            stats[key] = []
        stats[key].append(popularity)

    # Calculate aggregates and store
    created_count = 0

    for (genre, is_collab), popularities in stats.items():
        # Get or create genre
        cursor.execute("SELECT genre_id FROM genres WHERE genre_name = ?", (genre,))
        result = cursor.fetchone()

        if result:
            genre_id = result[0]
        else:
            cursor.execute("INSERT INTO genres (genre_name) VALUES (?)", (genre,))
            genre_id = cursor.lastrowid

        # Calculate stats
        track_count = len(popularities)
        avg_pop = np.mean(popularities) if popularities else 0
        median_pop = np.median(popularities) if popularities else 0

        # Store
        cursor.execute("""
            INSERT INTO collaboration_stats
            (genre_id, is_collaboration, track_count, avg_popularity, median_popularity)
            VALUES (?, ?, ?, ?, ?)
        """, (genre_id, is_collab, track_count, avg_pop, median_pop))

        created_count += 1

    conn.commit()
    conn.close()

    print(f"✓ Created {created_count} collaboration stat records")
    return created_count


def check_data_status():
    """
    Check if all required data is present before processing.

    Returns:
        bool: True if ready to process, False otherwise
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("DATA STATUS CHECK")
    print("=" * 60)

    # Check each table
    requirements = {
        "tracks": 100,
        "genres": 1,
        "track_genres": 50,
        "itunes_tracks": 50,
    }

    all_ready = True

    for table, min_required in requirements.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            if count >= min_required:
                print(f"   {table}: {count} rows")
            else:
                print(f"   {table}: {count} rows (need {min_required}+)")
                all_ready = False
        except sqlite3.OperationalError:
            print(f"   ✗ {table}: Table does not exist")
            all_ready = False

    # Check collaboration breakdown
    try:
        cursor.execute("""
            SELECT is_collaboration, COUNT(*)
            FROM tracks
            GROUP BY is_collaboration
        """)
        collab_breakdown = cursor.fetchall()

        if collab_breakdown:
            print("\n  Collaboration breakdown:")
            for is_collab, count in collab_breakdown:
                label = "Collaborations" if is_collab else "Solo tracks"
                print(f"    {label}: {count}")
    except sqlite3.OperationalError:
        pass

    conn.close()

    print("=" * 60)
    return all_ready


def print_stats_summary():
    """
    Print a summary of the calculated statistics.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("COLLABORATION STATISTICS SUMMARY")
    print("=" * 60)

    cursor.execute("""
        SELECT
            g.genre_name,
            cs.is_collaboration,
            cs.track_count,
            cs.avg_popularity,
            cs.median_popularity
        FROM collaboration_stats cs
        JOIN genres g ON cs.genre_id = g.genre_id
        ORDER BY g.genre_name, cs.is_collaboration
    """)

    results = cursor.fetchall()

    if not results:
        print("No statistics calculated yet.")
        conn.close()
        return

    current_genre = None
    for genre, is_collab, count, avg_pop, median_pop in results:
        if genre != current_genre:
            print(f"\n  {genre.upper()}")
            current_genre = genre

        label = "Collab" if is_collab else "Solo"
        print(f"    {label}: {count} tracks, avg popularity: {avg_pop:,.0f}")

    conn.close()
    print("\n" + "=" * 60)


def main():
    """
    Main function to process data and calculate statistics.
    """
    print("\n" + "=" * 60)
    print("DATA PROCESSING - COLLABORATION STATISTICS")
    print("=" * 60)

    # Check if data is ready
    if not check_data_status():
        print("\n✗ Not enough data to process!")
        print("  Please run the gather scripts first:")
        print("    1. python gather_deezer.py (run 4+ times)")
        print("    2. python gather_musicbrainz.py (run 4+ times)")
        print("    3. python gather_itunes.py (run 4+ times)")
        return

    # Calculate collaboration statistics
    print("\nCalculating collaboration statistics...")
    created = calculate_collaboration_stats()

    if created > 0:
        print_stats_summary()

    # Final status
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM collaboration_stats")
    total = cursor.fetchone()[0]
    conn.close()

    print("\n" + "=" * 60)
    if total >= 10:
        print(f"✓ COMPLETE: {total} stat records created")
        print("  Ready to run analyze_visualize.py")
    else:
        print(f"⚠️  Only {total} stat records created")
        print("  May need more data in tracks table")
    print("=" * 60)


if __name__ == "__main__":
    main()
