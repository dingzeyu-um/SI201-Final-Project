"""
SI 201 FINAL PROJECT: Music Trends vs Income Level Analysis
Team Name: [YOUR TEAM NAME HERE]
Team Members:
    - [Member 1 Name] - [Email] - Responsible for: API 1, Database setup
    - [Member 2 Name] - [Email] - Responsible for: API 2, Analysis
    - [Member 3 Name] - [Email] - Responsible for: API 3, Visualizations

APIs Used:
1. Deezer API (https://api.deezer.com/chart)
2. US Census Bureau API (https://api.census.gov/data/2022/acs/acs5)
3. MusicBrainz API (https://musicbrainz.org/ws/2/artist)

Required Libraries:
requests pandas numpy scipy matplotlib seaborn
"""

import sqlite3
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import time

# Configure visualization settings
plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")


class MusicIncomeAnalyzer:
    """
    Main class for analyzing music trends vs income levels.
    """

    def __init__(self, db_name="music_income.db"):
        """
        Initialize the analyzer with database connection.

        Args:
            db_name (str): Name of the SQLite database file
        """
        # TODO: Initialize database connection
        # HINT: Use sqlite3.connect()
        self.db_name = db_name
        self.conn = None  # TODO: Connect to database
        self.cursor = None  # TODO: Create cursor

        # Call setup_database to create tables
        # TODO: Uncomment the line below after implementing setup_database()
        # self.setup_database()

    # ==========================================================================
    # DATABASE SETUP (Team Member 1)
    # ==========================================================================

    def setup_database(self):
        """
        Create all database tables with proper schema.

        Tables to create:
        1. regions - stores Census Bureau income data
        2. genres - stores genre names (normalized)
        3. tracks - stores Deezer music data
        4. track_genres - junction table (track_id, genre_id)
        5. regional_popularity - links regions to tracks

        Returns:
            None
        """
        # TODO: Create regions table
        # Columns: region_id (PK), region_name (UNIQUE), state, median_income, population

        # TODO: Create genres table
        # Columns: genre_id (PK AUTOINCREMENT), genre_name (UNIQUE)

        # TODO: Create tracks table
        # Columns: track_id (PK AUTOINCREMENT), deezer_id (UNIQUE), title, artist_name, duration, popularity

        # TODO: Create track_genres junction table
        # Columns: track_id (FK), genre_id (FK), PRIMARY KEY (track_id, genre_id)

        # TODO: Create regional_popularity table
        # Columns: popularity_id (PK AUTOINCREMENT), region_id (FK), track_id (FK), rank

        # TODO: Don't forget to commit!
        pass

    # ==========================================================================
    # API DATA COLLECTION (Team Members 1, 2, 3)
    # ==========================================================================

    def fetch_deezer_charts(self, limit=25):
        """
        Fetch music data from Deezer API.
        Team Member 1 - RESPONSIBLE

        API Endpoint: GET https://api.deezer.com/chart
        Authentication: None required

        Args:
            limit (int): Maximum number of tracks to fetch (max 25 per run)

        Returns:
            list[dict]: List of track dictionaries with keys:
                - deezer_id (int)
                - title (str)
                - artist_name (str)
                - duration (int)
                - popularity (int)

        Example return:
        [
            {
                'deezer_id': 123456,
                'title': 'Song Title',
                'artist_name': 'Artist Name',
                'duration': 210,
                'popularity': 8500000
            },
            ...
        ]
        """
        # TODO: Make GET request to Deezer API
        # HINT: url = "https://api.deezer.com/chart"
        # HINT: response = requests.get(url, timeout=10)
        # HINT: data = response.json()

        # TODO: Extract tracks from response
        # HINT: Look for data['tracks']['data']

        # TODO: Loop through tracks and extract needed fields
        # HINT: track['id'], track['title'], track['artist']['name'], etc.

        # TODO: Limit to specified number of tracks
        # HINT: Use [:limit] on the list

        # TODO: Return list of track dictionaries

        print(f"TODO: Fetching {limit} tracks from Deezer...")
        return []  # Replace with actual implementation

    def fetch_census_data(self):
        """
        Fetch income data from US Census Bureau API.
        Team Member 2 - RESPONSIBLE

        API Endpoint: GET https://api.census.gov/data/2022/acs/acs5
        Parameters: get=NAME,B19013_001E&for=state:*
        Authentication: None required (optional API key)

        Returns:
            list[dict]: List of region dictionaries with keys:
                - region_name (str)
                - state (str)
                - median_income (float)
                - population (int)

        Example return:
        [
            {
                'region_name': 'Alabama',
                'state': '01',
                'median_income': 52035.0,
                'population': 5024279
            },
            ...
        ]
        """
        # TODO: Set up API endpoint and parameters
        # HINT: base_url = "https://api.census.gov/data/2022/acs/acs5"
        # HINT: params = {'get': 'NAME,B19013_001E', 'for': 'state:*'}

        # TODO: Make GET request
        # HINT: response = requests.get(base_url, params=params, timeout=10)

        # TODO: Parse JSON response
        # HINT: data = response.json()
        # HINT: First row is headers, skip it with data[1:]

        # TODO: Loop through rows and extract data
        # HINT: Each row is [name, income, state_code]

        # TODO: Convert income to float, handle null values

        # TODO: Return list of region dictionaries

        print("TODO: Fetching data from Census Bureau...")
        return []  # Replace with actual implementation

    def fetch_musicbrainz_genre(self, artist_name):
        """
        Fetch genre for an artist from MusicBrainz API.
        Team Member 3 - RESPONSIBLE

        API Endpoint: GET https://musicbrainz.org/ws/2/artist
        Parameters: query=artist:{artist_name}&fmt=json&limit=1
        Authentication: None (User-Agent header required)
        Rate Limit: 1 request per second

        Args:
            artist_name (str): Name of the artist to look up

        Returns:
            str: Genre name (e.g., "pop", "rock", "hip-hop") or None if not found

        Example return: "pop"
        """
        # TODO: Add rate limiting delay
        # HINT: time.sleep(1.1) to respect 1 req/sec limit

        # TODO: Set up API endpoint and parameters
        # HINT: url = "https://musicbrainz.org/ws/2/artist/"
        # HINT: params = {'query': f'artist:{artist_name}', 'fmt': 'json', 'limit': 1}

        # TODO: Add User-Agent header (REQUIRED by MusicBrainz)
        # HINT: headers = {'User-Agent': 'SI201Project/1.0 (Educational)'}

        # TODO: Make GET request
        # HINT: response = requests.get(url, params=params, headers=headers, timeout=10)

        # TODO: Parse response and extract genre
        # HINT: Check if data['artists'] exists and has items
        # HINT: Look for data['artists'][0]['tags'][0]['name']

        # TODO: Return genre name or None

        print(f"TODO: Fetching genre for {artist_name}...")
        return None  # Replace with actual implementation

    # ==========================================================================
    # DATABASE STORAGE (Team Members 1, 2, 3)
    # ==========================================================================

    def store_tracks(self, tracks):
        """
        Store track data in database.
        Team Member 1 - RESPONSIBLE

        Args:
            tracks (list[dict]): List of track dictionaries from fetch_deezer_charts()

        Returns:
            int: Number of new tracks stored
        """
        # TODO: Loop through tracks list

        # TODO: For each track, INSERT OR IGNORE into tracks table
        # HINT: Use parameterized query to prevent SQL injection
        # HINT: INSERT OR IGNORE INTO tracks (deezer_id, title, ...) VALUES (?, ?, ...)

        # TODO: Check if insert was successful
        # HINT: Use cursor.rowcount to see if row was inserted

        # TODO: Count number of new tracks stored

        # TODO: Commit changes
        # HINT: self.conn.commit()

        print("TODO: Storing tracks in database...")
        return 0  # Replace with actual count

    def store_regions(self, regions):
        """
        Store region/income data in database.
        Team Member 2 - RESPONSIBLE

        Args:
            regions (list[dict]): List of region dictionaries from fetch_census_data()

        Returns:
            int: Number of new regions stored
        """
        # TODO: Loop through regions list

        # TODO: For each region, INSERT OR IGNORE into regions table
        # HINT: Use parameterized query
        # HINT: INSERT OR IGNORE INTO regions (region_name, state, ...) VALUES (?, ?, ...)

        # TODO: Check if insert was successful

        # TODO: Count number of new regions stored

        # TODO: Commit changes

        print("TODO: Storing regions in database...")
        return 0  # Replace with actual count

    def assign_genres_to_tracks(self):
        """
        Fetch genres for tracks and link them in database.
        Team Member 3 - RESPONSIBLE

        Steps:
        1. Get all tracks that don't have genres yet
        2. For each track, call fetch_musicbrainz_genre()
        3. Insert genre into genres table (if not exists)
        4. Link track to genre in track_genres table

        Returns:
            int: Number of tracks assigned genres
        """
        # TODO: Query tracks that don't have genres yet
        # HINT: SELECT t.track_id, t.artist_name FROM tracks t
        #       LEFT JOIN track_genres tg ON t.track_id = tg.track_id
        #       WHERE tg.genre_id IS NULL LIMIT 25

        # TODO: Loop through tracks

        # TODO: For each track, fetch genre using fetch_musicbrainz_genre()

        # TODO: If genre is None, assign a default genre (e.g., "pop")

        # TODO: Insert genre into genres table (INSERT OR IGNORE)

        # TODO: Get genre_id
        # HINT: SELECT genre_id FROM genres WHERE genre_name = ?

        # TODO: Link track to genre in track_genres table
        # HINT: INSERT OR IGNORE INTO track_genres (track_id, genre_id) VALUES (?, ?)

        # TODO: Count number of tracks processed

        # TODO: Commit changes

        print("TODO: Assigning genres to tracks...")
        return 0  # Replace with actual count

    # ==========================================================================
    # DATA PROCESSING (Team Member 1)
    # ==========================================================================

    def simulate_regional_preferences(self):
        """
        Create relationships between regions and tracks based on income.
        Team Member 1 - RESPONSIBLE

        This simulates the correlation between income and genre preference.
        Low income → prefer hip-hop, pop
        Middle income → balanced
        High income → prefer rock, classical

        Returns:
            int: Number of preference records created
        """
        # TODO: Get all regions with their income levels
        # HINT: SELECT region_id, median_income FROM regions

        # TODO: Get all tracks with their genres
        # HINT: SELECT t.track_id, g.genre_name, t.popularity
        #       FROM tracks t
        #       JOIN track_genres tg ON t.track_id = tg.track_id
        #       JOIN genres g ON tg.genre_id = g.genre_id

        # TODO: For each region, determine income bracket
        # HINT: if income < 55000: low
        #       elif income < 70000: middle
        #       else: high

        # TODO: Define genre weights for each income bracket
        # HINT: low_income_weights = {'hip-hop': 2.5, 'pop': 2.0, ...}

        # TODO: For each track, decide if it's popular in this region
        # HINT: Use np.random.random() with genre weights

        # TODO: Insert into regional_popularity table
        # HINT: INSERT OR IGNORE INTO regional_popularity (region_id, track_id, rank) VALUES (?, ?, ?)

        # TODO: Commit changes

        print("TODO: Creating regional preferences...")
        return 0  # Replace with actual count

    # ==========================================================================
    # STATISTICAL ANALYSIS (Team Member 2)
    # ==========================================================================

    def perform_analysis(self):
        """
        Perform chi-square statistical analysis.
        Team Member 2 - RESPONSIBLE

        Steps:
        1. Query data with complex JOIN
        2. Create contingency table
        3. Perform chi-square test
        4. Calculate Cramér's V
        5. Print results

        Returns:
            tuple: (contingency, chi2, p_value, expected) or None if no data
        """
        # TODO: Write complex JOIN query
        # HINT: SELECT
        #       CASE WHEN r.median_income < 55000 THEN 'Low (<$55k)'
        #            WHEN r.median_income < 70000 THEN 'Middle ($55k-$70k)'
        #            ELSE 'High (>$70k)' END as income_bracket,
        #       g.genre_name,
        #       COUNT(*) as count
        #       FROM regional_popularity rp
        #       JOIN regions r ON rp.region_id = r.region_id
        #       JOIN tracks t ON rp.track_id = t.track_id
        #       JOIN track_genres tg ON t.track_id = tg.track_id
        #       JOIN genres g ON tg.genre_id = g.genre_id
        #       GROUP BY income_bracket, g.genre_name

        # TODO: Load query results into pandas DataFrame
        # HINT: df = pd.read_sql_query(query, self.conn)

        # TODO: Create contingency table (pivot table)
        # HINT: contingency = df.pivot_table(values='count',
        #                                     index='income_bracket',
        #                                     columns='genre_name',
        #                                     fill_value=0)

        # TODO: Perform chi-square test
        # HINT: chi2, p_value, dof, expected = chi2_contingency(contingency)

        # TODO: Calculate Cramér's V
        # HINT: n = contingency.sum().sum()
        #       min_dim = min(contingency.shape) - 1
        #       cramers_v = np.sqrt(chi2 / (n * min_dim))

        # TODO: Print results (chi2, p_value, cramers_v)

        # TODO: Return results

        print("TODO: Performing statistical analysis...")
        return None  # Replace with (contingency, chi2, p_value, expected)

    # ==========================================================================
    # VISUALIZATIONS (Team Member 3)
    # ==========================================================================

    def create_visualizations(self, contingency, chi2, p_value, expected):
        """
        Create all required visualizations.
        Team Member 3 - RESPONSIBLE

        Args:
            contingency (DataFrame): Observed frequency table
            chi2 (float): Chi-square statistic
            p_value (float): P-value
            expected (array): Expected frequencies

        Returns:
            None (saves PNG files)
        """
        # TODO: Call all visualization functions
        # self._create_observed_heatmap(contingency, chi2, p_value)
        # self._create_percentage_heatmap(contingency)
        # self._create_bar_chart(contingency)
        # self._create_residuals_heatmap(contingency, expected)  # BONUS

        print("TODO: Creating visualizations...")

    def _create_observed_heatmap(self, contingency, chi2, p_value):
        """Create heatmap of observed frequencies."""
        # TODO: Create figure and axis
        # HINT: fig, ax = plt.subplots(figsize=(12, 8))

        # TODO: Create heatmap using seaborn
        # HINT: sns.heatmap(contingency, annot=True, fmt='g', cmap='YlOrRd', ax=ax)

        # TODO: Add title with chi2 and p_value
        # HINT: plt.title(f'Music Genre Preferences\nχ² = {chi2:.2f}, p = {p_value:.4f}')

        # TODO: Add axis labels

        # TODO: Save figure
        # HINT: plt.savefig('heatmap_observed.png', dpi=300, bbox_inches='tight')

        # TODO: Close figure
        # HINT: plt.close()

        pass

    def _create_percentage_heatmap(self, contingency):
        """Create heatmap of percentage distribution."""
        # TODO: Calculate percentages
        # HINT: percentages = contingency.div(contingency.sum(axis=1), axis=0) * 100

        # TODO: Create heatmap
        # HINT: Similar to observed heatmap but with fmt='.1f' and different colormap

        # TODO: Save as 'heatmap_percentage.png'

        pass

    def _create_bar_chart(self, contingency):
        """Create grouped bar chart."""
        # TODO: Create figure

        # TODO: Create bar chart
        # HINT: contingency.plot(kind='bar', ax=ax)

        # TODO: Add labels, title, legend

        # TODO: Add value labels on bars
        # HINT: for container in ax.containers:
        #           ax.bar_label(container, fmt='%.0f')

        # TODO: Save as 'bar_chart.png'

        pass

    def _create_residuals_heatmap(self, contingency, expected):
        """Create residuals heatmap (BONUS)."""
        # TODO: Calculate standardized residuals
        # HINT: residuals = (contingency - expected) / np.sqrt(expected)

        # TODO: Create diverging heatmap
        # HINT: Use cmap='RdBu_r', center=0, vmin=-3, vmax=3

        # TODO: Save as 'heatmap_residuals.png'

        pass

    # ==========================================================================
    # EXPORT RESULTS (Team Member 2)
    # ==========================================================================

    def export_results(self):
        """
        Export analysis results to text file.
        Team Member 2 - RESPONSIBLE

        Returns:
            None (writes to analysis_results.txt)
        """
        # TODO: Open file for writing
        # HINT: with open('analysis_results.txt', 'w') as f:

        # TODO: Write header

        # TODO: Query and write summary statistics
        # HINT: SELECT COUNT(*) FROM tracks, regions, etc.

        # TODO: Query and write detailed results
        # HINT: Use the same JOIN query from perform_analysis()

        # TODO: Write to file
        # HINT: f.write(df.to_string(index=False))

        print("TODO: Exporting results...")

    # ==========================================================================
    # UTILITY FUNCTIONS
    # ==========================================================================

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# ==============================================================================
# MAIN EXECUTION FUNCTION
# ==============================================================================


def main():
    """
    Main function to run the entire analysis.

    This function should be run MULTIPLE TIMES to collect 100+ items.
    Each run stores max 25 items from APIs.
    """
    print("\n" + "=" * 70)
    print("SI 201 FINAL PROJECT: MUSIC TRENDS VS INCOME ANALYSIS")
    print("=" * 70)

    # TODO: Initialize analyzer
    # analyzer = MusicIncomeAnalyzer()

    # TODO: PHASE 1 - Data Collection (run this 4+ times)
    # Step 1: Fetch data from APIs
    # tracks = analyzer.fetch_deezer_charts(limit=25)
    # regions = analyzer.fetch_census_data()

    # Step 2: Store in database
    # analyzer.store_tracks(tracks)
    # analyzer.store_regions(regions)
    # analyzer.assign_genres_to_tracks()

    # TODO: PHASE 2 - Data Processing (run after collecting 100+ items)
    # analyzer.simulate_regional_preferences()

    # TODO: PHASE 3 - Statistical Analysis
    # results = analyzer.perform_analysis()

    # TODO: PHASE 4 - Visualizations
    # if results:
    #     contingency, chi2, p_value, expected = results
    #     analyzer.create_visualizations(contingency, chi2, p_value, expected)

    # TODO: PHASE 5 - Export Results
    # analyzer.export_results()

    # TODO: Close database
    # analyzer.close()

    print("\n" + "=" * 70)
    print("TODO: Complete all function implementations!")
    print("=" * 70)


if __name__ == "__main__":
    main()
