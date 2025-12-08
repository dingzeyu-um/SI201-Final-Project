"""
run_all.py
SI 201 Final Project - Master Runner Script

THE COLLABORATION EFFECT IN MUSIC
Research Question: Do songs with featured artists have higher popularity?

This script runs all components in the correct order.
Use this for testing or final execution.

APIs Used:
1. Deezer API - Track data with collaboration detection
2. MusicBrainz API - Genre/tag metadata
3. iTunes Search API - Cross-validation data

Usage:
    python run_all.py
"""

# Debug mode - set to False for production (fewer runs, faster execution)
DEBUG_MODE = True

# Number of runs for each data gathering step
DEEZER_RUNS = 4 if DEBUG_MODE else 5
MUSICBRAINZ_RUNS = 2 if DEBUG_MODE else 5
ITUNES_RUNS = 2 if DEBUG_MODE else 5


def main():
    print("=" * 70)
    print("SI 201 FINAL PROJECT")
    print("THE COLLABORATION EFFECT IN MUSIC")
    print("=" * 70)
    print("\nResearch Question:")
    print("  Do songs with featured artists have higher popularity?")
    print("\nThis script will run all components in order.\n")

    # Step 1: Database Setup
    print("\n" + "=" * 70)
    print("STEP 1: DATABASE SETUP")
    print("=" * 70)
    import database_setup
    database_setup.create_database()

    # Step 2: Gather Deezer Data
    print("\n" + "=" * 70)
    print("STEP 2: GATHER DEEZER DATA (with collaboration detection)")
    print(f"Mode: {'DEBUG' if DEBUG_MODE else 'PRODUCTION'} ({DEEZER_RUNS} runs)")
    print("=" * 70)
    import gather_deezer
    for i in range(DEEZER_RUNS):
        print(f"\n--- Run {i+1}/{DEEZER_RUNS} ---")
        gather_deezer.main()

    # Step 3: Gather MusicBrainz Genres
    print("\n" + "=" * 70)
    print("STEP 3: GATHER MUSICBRAINZ GENRES")
    print(f"Mode: {'DEBUG' if DEBUG_MODE else 'PRODUCTION'} ({MUSICBRAINZ_RUNS} runs)")
    print("=" * 70)
    import gather_musicbrainz
    for i in range(MUSICBRAINZ_RUNS):
        print(f"\n--- Run {i+1}/{MUSICBRAINZ_RUNS} ---")
        gather_musicbrainz.main()

    # Step 4: Gather iTunes Data for cross-validation
    print("\n" + "=" * 70)
    print("STEP 4: GATHER ITUNES DATA (cross-validation)")
    print(f"Mode: {'DEBUG' if DEBUG_MODE else 'PRODUCTION'} ({ITUNES_RUNS} runs)")
    print("=" * 70)
    import gather_itunes
    for i in range(ITUNES_RUNS):
        print(f"\n--- Run {i+1}/{ITUNES_RUNS} ---")
        gather_itunes.main()

    # Step 5: Process Data
    print("\n" + "=" * 70)
    print("STEP 5: PROCESS DATA (calculate collaboration stats)")
    print("=" * 70)
    import process_data
    process_data.main()

    # Step 6: Analyze and Visualize
    print("\n" + "=" * 70)
    print("STEP 6: ANALYZE AND VISUALIZE")
    print("=" * 70)
    import analyze_visualize
    analyze_visualize.main()

    # Final Status
    print("\n" + "=" * 70)
    print("PROJECT EXECUTION COMPLETE!")
    print("=" * 70)
    print("\nGenerated files:")
    print("  Database: music_collab.db")
    print("  Visualizations:")
    print("    - viz1_boxplot_popularity.png (Solo vs Collab popularity)")
    print("    - viz2_collab_by_genre.png (Collaboration rate by genre)")
    print("    - viz3_heatmap_genre_collab.png (Genre x Collab heatmap)")
    print("    - viz4_chi_square_heatmap.png (Chi-square results)")
    print("  Results: analysis_results.txt")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
