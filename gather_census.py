"""
gather_census.py
SI 201 Final Project - US Census Bureau API Data Gathering

This file fetches income data from the Census Bureau API and stores it in the database.
Run this file MULTIPLE TIMES to gather 100+ regions (25 per run).

API: https://api.census.gov/data/2022/acs/acs5
Authentication: None required (optional API key available)

Team Member Responsible: [Member 2 Name]
"""

import sqlite3
import requests

DB_NAME = 'music_income.db'
MAX_ITEMS_PER_RUN = 25  # Required by project: max 25 items per run


def fetch_census_data():
    """
    Fetch median household income data from US Census Bureau API.
    
    API Endpoint: GET https://api.census.gov/data/2022/acs/acs5
    Parameters: get=NAME,B19013_001E&for=state:*
    
    B19013_001E = Median Household Income
    
    Returns:
        list[dict]: List of region dictionaries, or empty list if error
    """
    base_url = "https://api.census.gov/data/2022/acs/acs5"
    params = {
        'get': 'NAME,B19013_001E',  # State name and median income
        'for': 'state:*'  # All states
    }
    
    try:
        print("Fetching data from Census Bureau API...")
        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        regions = []
        # First row is header, skip it
        for row in data[1:]:
            name, income, state_code = row
            
            # Skip if income is null or invalid
            if income and income != 'null' and income != '-':
                try:
                    regions.append({
                        'region_name': name,
                        'state_code': state_code,
                        'median_income': float(income),
                        'population': 0  # Will be updated if needed
                    })
                except ValueError:
                    continue
        
        print(f"✓ Retrieved {len(regions)} regions from Census Bureau")
        return regions
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching from Census API: {e}")
        print("  Using simulated data instead...")
        return get_simulated_census_data()


def get_simulated_census_data():
    """
    Fallback: Return simulated census data if API fails.
    
    Returns:
        list[dict]: Simulated census data for all 50 states + DC
    """
    import random
    
    states_data = [
        ('Alabama', '01', 52035),
        ('Alaska', '02', 77790),
        ('Arizona', '04', 62055),
        ('Arkansas', '05', 49475),
        ('California', '06', 78672),
        ('Colorado', '08', 77933),
        ('Connecticut', '09', 79855),
        ('Delaware', '10', 70176),
        ('District of Columbia', '11', 90842),
        ('Florida', '12', 59227),
        ('Georgia', '13', 61497),
        ('Hawaii', '15', 83102),
        ('Idaho', '16', 60999),
        ('Illinois', '17', 69187),
        ('Indiana', '18', 58235),
        ('Iowa', '19', 61691),
        ('Kansas', '20', 62087),
        ('Kentucky', '21', 52295),
        ('Louisiana', '22', 51073),
        ('Maine', '23', 58924),
        ('Maryland', '24', 90203),
        ('Massachusetts', '25', 84385),
        ('Michigan', '26', 59584),
        ('Minnesota', '27', 74593),
        ('Mississippi', '28', 45792),
        ('Missouri', '29', 57409),
        ('Montana', '30', 56539),
        ('Nebraska', '31', 63229),
        ('Nevada', '32', 63276),
        ('New Hampshire', '33', 81539),
        ('New Jersey', '34', 88559),
        ('New Mexico', '35', 51945),
        ('New York', '36', 72920),
        ('North Carolina', '37', 57341),
        ('North Dakota', '38', 64894),
        ('Ohio', '39', 58116),
        ('Oklahoma', '40', 53840),
        ('Oregon', '41', 65667),
        ('Pennsylvania', '42', 63627),
        ('Rhode Island', '44', 70305),
        ('South Carolina', '45', 54864),
        ('South Dakota', '46', 59896),
        ('Tennessee', '47', 54833),
        ('Texas', '48', 63826),
        ('Utah', '49', 74197),
        ('Vermont', '50', 63001),
        ('Virginia', '51', 76398),
        ('Washington', '53', 77006),
        ('West Virginia', '54', 46711),
        ('Wisconsin', '55', 63293),
        ('Wyoming', '56', 64049),
        ('Puerto Rico', '72', 21058),
    ]
    
    regions = []
    for name, code, income in states_data:
        regions.append({
            'region_name': name,
            'state_code': code,
            'median_income': income,
            'population': random.randint(500000, 40000000)
        })
    
    return regions


def store_regions(regions):
    """
    Store region/income data in the database.
    Limits to MAX_ITEMS_PER_RUN (25) per execution.
    
    Args:
        regions (list[dict]): Region dictionaries from fetch_census_data()
        
    Returns:
        int: Number of new regions stored
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Get current count to determine offset
    cursor.execute('SELECT COUNT(*) FROM regions')
    current_count = cursor.fetchone()[0]
    
    stored_count = 0
    
    # Start from where we left off, store up to 25
    start_idx = current_count
    end_idx = min(start_idx + MAX_ITEMS_PER_RUN, len(regions))
    
    for region in regions[start_idx:end_idx]:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO regions 
                (region_name, state_code, median_income, population)
                VALUES (?, ?, ?, ?)
            ''', (
                region['region_name'],
                region['state_code'],
                region['median_income'],
                region['population']
            ))
            
            if cursor.rowcount > 0:
                stored_count += 1
                
        except sqlite3.IntegrityError:
            continue
    
    conn.commit()
    
    # Get total count
    cursor.execute('SELECT COUNT(*) FROM regions')
    total = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"✓ Stored {stored_count} new regions (Total: {total})")
    
    return stored_count


def main():
    """
    Main function to fetch and store Census data.
    Run this multiple times to collect 100+ regions.
    """
    print("\n" + "="*60)
    print("CENSUS BUREAU API DATA GATHERING")
    print(f"Maximum items per run: {MAX_ITEMS_PER_RUN}")
    print("="*60)
    
    # Fetch data from API
    regions = fetch_census_data()
    
    if regions:
        # Store in database
        stored = store_regions(regions)
        
        # Check if we need more runs
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM regions')
        total = cursor.fetchone()[0]
        conn.close()
        
        print("\n" + "="*60)
        if total >= 100:
            print(f"✓ COMPLETE: {total} regions in database (100+ required)")
        else:
            remaining = 100 - total
            runs_needed = (remaining // MAX_ITEMS_PER_RUN) + 1
            print(f"⚠️  Need {remaining} more regions")
            print(f"   Run this script {runs_needed} more time(s)")
            print(f"   (Note: Only 52 states/territories available)")
        print("="*60)
    else:
        print("✗ No regions retrieved")


if __name__ == "__main__":
    main()
