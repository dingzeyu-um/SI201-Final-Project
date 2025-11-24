"""
run_all.py
SI 201 Final Project - Master Runner Script

This script runs all components in the correct order.
Use this for testing or final execution.

IMPORTANT: The project requires running gather scripts MULTIPLE TIMES
to collect 100+ items. This script demonstrates one complete run.

Usage:
    python run_all.py

For production (to meet 100+ items requirement):
    Run each gather script 4-5 times manually
"""

import os
import sys

def main():
    print("="*70)
    print("SI 201 FINAL PROJECT - MUSIC TRENDS VS INCOME ANALYSIS")
    print("="*70)
    print("\nThis script will run all components in order.")
    print("NOTE: For 100+ items, run gather scripts multiple times.\n")
    
    # Step 1: Database Setup
    print("\n" + "="*70)
    print("STEP 1: DATABASE SETUP")
    print("="*70)
    import database_setup
    database_setup.create_database()
    
    # Step 2: Gather Deezer Data (run 4+ times for 100+ tracks)
    print("\n" + "="*70)
    print("STEP 2: GATHER DEEZER DATA")
    print("="*70)
    import gather_deezer
    for i in range(5):  # Run 5 times to simulate multiple executions
        print(f"\n--- Run {i+1}/5 ---")
        gather_deezer.main()
    
    # Step 3: Gather Census Data (run 2-3 times for all states)
    print("\n" + "="*70)
    print("STEP 3: GATHER CENSUS DATA")
    print("="*70)
    import gather_census
    for i in range(3):  # Run 3 times
        print(f"\n--- Run {i+1}/3 ---")
        gather_census.main()
    
    # Step 4: Gather MusicBrainz Genres (run 4+ times)
    print("\n" + "="*70)
    print("STEP 4: GATHER MUSICBRAINZ GENRES")
    print("="*70)
    import gather_musicbrainz
    for i in range(5):  # Run 5 times
        print(f"\n--- Run {i+1}/5 ---")
        gather_musicbrainz.main()
    
    # Step 5: Process Data
    print("\n" + "="*70)
    print("STEP 5: PROCESS DATA")
    print("="*70)
    import process_data
    process_data.main()
    
    # Step 6: Analyze and Visualize
    print("\n" + "="*70)
    print("STEP 6: ANALYZE AND VISUALIZE")
    print("="*70)
    import analyze_visualize
    analyze_visualize.main()
    
    # Final Status
    print("\n" + "="*70)
    print("PROJECT EXECUTION COMPLETE!")
    print("="*70)
    print("\nGenerated files:")
    print("  Database: music_income.db")
    print("  Visualizations: viz1_*.png, viz2_*.png, viz3_*.png, viz4_*.png")
    print("  Results: analysis_results.txt")
    print("\nCheck database_setup.py's check_database_status() for row counts.")
    print("="*70)


if __name__ == "__main__":
    main()
