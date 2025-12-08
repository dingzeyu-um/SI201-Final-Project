# Dingze
"""
analyze_visualize.py
SI 201 Final Project - Analysis and Visualization

This file performs statistical analysis and creates visualizations.
Run this AFTER process_data.py has calculated collaboration statistics.

Analysis: The Collaboration Effect in Music
Research Question: Do songs with featured artists have higher popularity?

Statistical Tests:
- Mann-Whitney U Test: Compare popularity distributions
- Chi-Square Test: Collaboration status vs popularity tier independence

Team Member Responsible: [Member 4 Name]
"""

import sqlite3
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, mannwhitneyu
import matplotlib.pyplot as plt
import seaborn as sns

DB_NAME = "music_collab.db"

# Configure visualization style
plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")


# ==============================================================================
# DATA RETRIEVAL FUNCTIONS
# ==============================================================================


def get_track_data():
    """
    Get all track data with collaboration status and genre.
    Uses database JOINs to combine tracks with iTunes/MusicBrainz genres.

    Returns:
        pandas.DataFrame: Track data with columns:
            track_id, title, artist_name, popularity, is_collaboration,
            collab_indicator, genre
    """
    conn = sqlite3.connect(DB_NAME)

    # Complex JOIN query across multiple tables
    query = """
        SELECT
            t.track_id,
            t.title,
            t.artist_name,
            t.popularity,
            t.is_collaboration,
            t.collab_indicator,
            t.featuring_artist,
            COALESCE(it.itunes_genre, g.genre_name, 'Unknown') as genre
        FROM tracks t
        LEFT JOIN itunes_tracks it ON t.track_id = it.track_id
        LEFT JOIN track_genres tg ON t.track_id = tg.track_id
        LEFT JOIN genres g ON tg.genre_id = g.genre_id
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df


def normalize_genre(genre_name):
    """
    Map genre names to standard categories.

    Args:
        genre_name (str): Raw genre name

    Returns:
        str: Normalized genre category
    """
    if not genre_name or genre_name == 'Unknown':
        return "other"

    genre_lower = genre_name.lower()

    mappings = {
        "hip-hop": ["hip hop", "hip-hop", "rap", "trap"],
        "rock": ["rock", "alternative rock", "hard rock", "alternative"],
        "pop": ["pop", "dance", "electropop"],
        "r&b": ["r&b", "contemporary r&b", "soul", "r&b/soul"],
        "latin": ["reggaeton", "latin", "música mexicana"],
        "country": ["country"],
        "electronic": ["electronic", "edm", "house"],
    }

    for standard_genre, variants in mappings.items():
        if genre_lower in variants or any(v in genre_lower for v in variants):
            return standard_genre

    return "other"


# ==============================================================================
# STATISTICAL ANALYSIS FUNCTIONS
# ==============================================================================


def perform_mann_whitney_test(df):
    """
    Perform Mann-Whitney U test comparing popularity between
    solo tracks and collaborations.

    This non-parametric test is appropriate when data may not be
    normally distributed.

    Args:
        df (DataFrame): Track data

    Returns:
        tuple: (u_statistic, p_value, solo_median, collab_median)
    """
    solo = df[df['is_collaboration'] == 0]['popularity']
    collab = df[df['is_collaboration'] == 1]['popularity']

    if len(solo) < 2 or len(collab) < 2:
        return None, None, None, None

    u_stat, p_value = mannwhitneyu(solo, collab, alternative='two-sided')

    return u_stat, p_value, solo.median(), collab.median()


def perform_chi_square_test(df):
    """
    Perform chi-square test of independence.
    Tests if collaboration status and popularity tier are independent.

    Args:
        df (DataFrame): Track data

    Returns:
        tuple: (contingency, chi2, p_value, dof, expected, cramers_v)
    """
    # Create popularity tiers
    df = df.copy()
    df['popularity_tier'] = pd.qcut(
        df['popularity'],
        q=3,
        labels=['Low', 'Medium', 'High'],
        duplicates='drop'
    )

    df['collab_status'] = df['is_collaboration'].map({0: 'Solo', 1: 'Collaboration'})

    # Create contingency table
    contingency = pd.crosstab(df['collab_status'], df['popularity_tier'])

    # Perform chi-square test
    chi2, p_value, dof, expected = chi2_contingency(contingency)

    # Calculate Cramér's V (effect size)
    n = contingency.sum().sum()
    min_dim = min(contingency.shape) - 1
    cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 else 0

    return contingency, chi2, p_value, dof, expected, cramers_v


def print_analysis_results(df, mw_results, chi_results):
    """
    Print formatted analysis results to console.

    Args:
        df: Track data
        mw_results: Mann-Whitney test results
        chi_results: Chi-square test results
    """
    u_stat, mw_p, solo_med, collab_med = mw_results
    contingency, chi2, chi_p, dof, expected, cramers_v = chi_results

    print("\n" + "=" * 70)
    print("THE COLLABORATION EFFECT IN MUSIC - ANALYSIS RESULTS")
    print("=" * 70)

    # Data summary
    total = len(df)
    solo_count = len(df[df['is_collaboration'] == 0])
    collab_count = len(df[df['is_collaboration'] == 1])

    print(f"\nDATA SUMMARY:")
    print(f"  Total tracks: {total}")
    print(f"  Solo tracks: {solo_count} ({100*solo_count/total:.1f}%)")
    print(f"  Collaborations: {collab_count} ({100*collab_count/total:.1f}%)")

    # Mann-Whitney results
    print("\n" + "=" * 70)
    print("MANN-WHITNEY U TEST")
    print("H0: Popularity distributions are the same for solo and collab tracks")
    print("=" * 70)

    if u_stat is not None:
        print(f"U Statistic: {u_stat:,.0f}")
        print(f"P-value: {mw_p:.6f}")
        print(f"\nMedian Popularity:")
        print(f"  Solo tracks: {solo_med:,.0f}")
        print(f"  Collaborations: {collab_med:,.0f}")
        print(f"  Difference: {collab_med - solo_med:+,.0f}")

        print("\nINTERPRETATION:")
        if mw_p < 0.05:
            print("  ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
            if collab_med > solo_med:
                print("  → Collaborations have HIGHER popularity than solo tracks")
            else:
                print("  → Solo tracks have HIGHER popularity than collaborations")
        else:
            print("  ✗ NOT STATISTICALLY SIGNIFICANT (p ≥ 0.05)")
            print("  → No significant difference in popularity distributions")

    # Chi-square results
    print("\n" + "=" * 70)
    print("CHI-SQUARE TEST OF INDEPENDENCE")
    print("H0: Collaboration status and popularity tier are independent")
    print("=" * 70)

    print("\nContingency Table:")
    print(contingency)

    print(f"\nChi-Square Statistic: {chi2:.4f}")
    print(f"P-value: {chi_p:.6f}")
    print(f"Degrees of Freedom: {dof}")

    print("\nINTERPRETATION:")
    if chi_p < 0.05:
        print("  ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
        print("  → Collaboration status and popularity tier ARE related")
    else:
        print("  ✗ NOT STATISTICALLY SIGNIFICANT (p ≥ 0.05)")
        print("  → No strong evidence of relationship")

    print(f"\nEFFECT SIZE (Cramér's V): {cramers_v:.3f}")
    if cramers_v < 0.1:
        print("  → Small effect (V < 0.1)")
    elif cramers_v < 0.3:
        print("  → Medium effect (0.1 ≤ V < 0.3)")
    else:
        print("  → Large effect (V ≥ 0.3)")

    print("=" * 70)


def export_results_to_file(df, mw_results, chi_results):
    """
    Export all results to a text file.

    Args:
        df: Track data
        mw_results: Mann-Whitney test results
        chi_results: Chi-square test results
    """
    filename = "analysis_results.txt"
    u_stat, mw_p, solo_med, collab_med = mw_results
    contingency, chi2, chi_p, dof, expected, cramers_v = chi_results

    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("THE COLLABORATION EFFECT IN MUSIC - ANALYSIS RESULTS\n")
        f.write("SI 201 Final Project\n")
        f.write("=" * 70 + "\n\n")

        # Research question
        f.write("RESEARCH QUESTION:\n")
        f.write("Do songs with featured artists have higher popularity than solo tracks?\n\n")

        # Data summary
        total = len(df)
        solo_count = len(df[df['is_collaboration'] == 0])
        collab_count = len(df[df['is_collaboration'] == 1])

        f.write("DATA SUMMARY\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total tracks analyzed: {total}\n")
        f.write(f"Solo tracks: {solo_count} ({100*solo_count/total:.1f}%)\n")
        f.write(f"Collaborations: {collab_count} ({100*collab_count/total:.1f}%)\n\n")

        # Database info
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        for table in ['tracks', 'genres', 'track_genres', 'itunes_tracks']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            f.write(f"{table}: {cursor.fetchone()[0]} rows\n")
        conn.close()

        # Mann-Whitney results
        f.write("\n" + "=" * 70 + "\n")
        f.write("MANN-WHITNEY U TEST\n")
        f.write("-" * 70 + "\n")
        if u_stat is not None:
            f.write(f"U Statistic: {u_stat:,.0f}\n")
            f.write(f"P-value: {mw_p:.6f}\n")
            f.write(f"Solo Median Popularity: {solo_med:,.0f}\n")
            f.write(f"Collaboration Median Popularity: {collab_med:,.0f}\n")
            f.write(f"Significant at α=0.05: {'Yes' if mw_p < 0.05 else 'No'}\n")

        # Chi-square results
        f.write("\n" + "=" * 70 + "\n")
        f.write("CHI-SQUARE TEST OF INDEPENDENCE\n")
        f.write("-" * 70 + "\n")
        f.write("Contingency Table:\n")
        f.write(contingency.to_string() + "\n\n")
        f.write(f"Chi-Square Statistic: {chi2:.4f}\n")
        f.write(f"P-value: {chi_p:.6f}\n")
        f.write(f"Degrees of Freedom: {dof}\n")
        f.write(f"Cramér's V (Effect Size): {cramers_v:.3f}\n")
        f.write(f"Significant at α=0.05: {'Yes' if chi_p < 0.05 else 'No'}\n")

        f.write("\n" + "=" * 70 + "\n")

    print(f"✓ Results exported to {filename}")


# ==============================================================================
# VISUALIZATION FUNCTIONS
# ==============================================================================


def create_boxplot_popularity(df):
    """
    Visualization 1: Box plot comparing popularity by collaboration status.

    Args:
        df: Track data
    """
    fig, ax = plt.subplots(figsize=(10, 7))

    df_plot = df.copy()
    df_plot['Collaboration Status'] = df_plot['is_collaboration'].map(
        {0: 'Solo', 1: 'Collaboration'}
    )

    sns.boxplot(
        data=df_plot,
        x='Collaboration Status',
        y='popularity',
        hue='Collaboration Status',
        palette=['#3498db', '#e74c3c'],
        ax=ax,
        legend=False
    )

    # Add mean markers
    means = df_plot.groupby('Collaboration Status')['popularity'].mean()
    for i, status in enumerate(['Solo', 'Collaboration']):
        ax.scatter(i, means[status], color='white', s=100, zorder=10,
                   edgecolors='black', linewidth=2, marker='D')

    plt.title(
        "Track Popularity: Solo vs Collaboration\n(Diamond = Mean)",
        fontsize=14,
        fontweight="bold",
        pad=20
    )
    plt.xlabel("Collaboration Status", fontsize=12, fontweight="bold")
    plt.ylabel("Popularity (Deezer Rank)", fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.savefig("viz1_boxplot_popularity.png", dpi=300, bbox_inches="tight")
    plt.close()

    print("✓ Saved: viz1_boxplot_popularity.png")


def create_bar_collab_by_genre(df):
    """
    Visualization 2: Grouped bar chart showing collaboration rate by genre.

    Args:
        df: Track data
    """
    fig, ax = plt.subplots(figsize=(12, 7))

    df_plot = df.copy()
    df_plot['normalized_genre'] = df_plot['genre'].apply(normalize_genre)

    # Calculate collaboration rate by genre
    genre_stats = df_plot.groupby('normalized_genre').agg(
        total=('track_id', 'count'),
        collabs=('is_collaboration', 'sum')
    ).reset_index()

    genre_stats['collab_rate'] = 100 * genre_stats['collabs'] / genre_stats['total']
    genre_stats = genre_stats.sort_values('collab_rate', ascending=True)

    # Create horizontal bar chart
    colors = plt.cm.RdYlGn(genre_stats['collab_rate'] / 100)

    bars = ax.barh(
        genre_stats['normalized_genre'],
        genre_stats['collab_rate'],
        color=colors,
        edgecolor='black',
        linewidth=0.5
    )

    # Add value labels
    for bar, rate, total in zip(bars, genre_stats['collab_rate'], genre_stats['total']):
        ax.text(
            bar.get_width() + 1,
            bar.get_y() + bar.get_height()/2,
            f'{rate:.1f}% (n={total})',
            va='center',
            fontsize=10
        )

    plt.title(
        "Collaboration Rate by Genre",
        fontsize=14,
        fontweight="bold",
        pad=20
    )
    plt.xlabel("Collaboration Rate (%)", fontsize=12, fontweight="bold")
    plt.ylabel("Genre", fontsize=12, fontweight="bold")
    plt.xlim(0, max(genre_stats['collab_rate']) + 15)

    plt.tight_layout()
    plt.savefig("viz2_collab_by_genre.png", dpi=300, bbox_inches="tight")
    plt.close()

    print("✓ Saved: viz2_collab_by_genre.png")


def create_heatmap_genre_collab(df):
    """
    Visualization 3: Heatmap showing average popularity by genre and collab status.

    Args:
        df: Track data
    """
    fig, ax = plt.subplots(figsize=(10, 8))

    df_plot = df.copy()
    df_plot['normalized_genre'] = df_plot['genre'].apply(normalize_genre)
    df_plot['collab_status'] = df_plot['is_collaboration'].map({0: 'Solo', 1: 'Collab'})

    # Create pivot table
    pivot = df_plot.pivot_table(
        values='popularity',
        index='normalized_genre',
        columns='collab_status',
        aggfunc='mean'
    )

    # Reorder columns
    if 'Solo' in pivot.columns and 'Collab' in pivot.columns:
        pivot = pivot[['Solo', 'Collab']]

    sns.heatmap(
        pivot,
        annot=True,
        fmt='.0f',
        cmap='YlOrRd',
        cbar_kws={'label': 'Average Popularity'},
        linewidths=1,
        linecolor='white',
        ax=ax
    )

    plt.title(
        "Average Popularity: Genre × Collaboration Status",
        fontsize=14,
        fontweight="bold",
        pad=20
    )
    plt.xlabel("Collaboration Status", fontsize=12, fontweight="bold")
    plt.ylabel("Genre", fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.savefig("viz3_heatmap_genre_collab.png", dpi=300, bbox_inches="tight")
    plt.close()

    print("✓ Saved: viz3_heatmap_genre_collab.png")


def create_chi_square_visualization(contingency, chi2, p_value):
    """
    Visualization 4: Chi-square contingency table heatmap.

    Args:
        contingency: Contingency table from chi-square test
        chi2: Chi-square statistic
        p_value: P-value
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    # Calculate percentages
    percentages = contingency.div(contingency.sum(axis=1), axis=0) * 100

    sns.heatmap(
        percentages,
        annot=True,
        fmt='.1f',
        cmap='RdYlGn',
        cbar_kws={'label': 'Percentage (%)'},
        linewidths=1,
        linecolor='white',
        ax=ax,
        vmin=0,
        vmax=100
    )

    significance = "(Significant)" if p_value < 0.05 else "(Not Significant)"
    plt.title(
        f"Popularity Tier Distribution by Collaboration Status\n"
        f"χ² = {chi2:.2f}, p = {p_value:.4f} {significance}",
        fontsize=14,
        fontweight="bold",
        pad=20
    )
    plt.xlabel("Popularity Tier", fontsize=12, fontweight="bold")
    plt.ylabel("Collaboration Status", fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.savefig("viz4_chi_square_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close()

    print("✓ Saved: viz4_chi_square_heatmap.png")


# ==============================================================================
# MAIN FUNCTION
# ==============================================================================


def main():
    """
    Main function to perform analysis and create visualizations.
    """
    print("\n" + "=" * 70)
    print("THE COLLABORATION EFFECT IN MUSIC")
    print("Analysis and Visualization")
    print("=" * 70)

    # Step 1: Get data
    print("\n[1] Loading track data from database...")
    df = get_track_data()

    if df.empty:
        print("✗ No data found! Please run all gather scripts first.")
        return

    print(f"✓ Loaded {len(df)} tracks")

    solo_count = len(df[df['is_collaboration'] == 0])
    collab_count = len(df[df['is_collaboration'] == 1])
    print(f"  Solo: {solo_count}, Collaborations: {collab_count}")

    if collab_count == 0:
        print("\n✗ No collaborations detected in data!")
        print("  The analysis requires both solo and collaboration tracks.")
        return

    # Step 2: Perform statistical tests
    print("\n[2] Performing statistical analysis...")

    mw_results = perform_mann_whitney_test(df)
    chi_results = perform_chi_square_test(df)

    print_analysis_results(df, mw_results, chi_results)

    # Step 3: Create visualizations
    print("\n[3] Creating visualizations...")
    create_boxplot_popularity(df)
    create_bar_collab_by_genre(df)
    create_heatmap_genre_collab(df)

    contingency, chi2, p_value, _, _, _ = chi_results
    create_chi_square_visualization(contingency, chi2, p_value)

    # Step 4: Export results
    print("\n[4] Exporting results to file...")
    export_results_to_file(df, mw_results, chi_results)

    # Final summary
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE!")
    print("=" * 70)
    print("\nGenerated files:")
    print("  • viz1_boxplot_popularity.png - Popularity comparison")
    print("  • viz2_collab_by_genre.png - Collaboration rate by genre")
    print("  • viz3_heatmap_genre_collab.png - Genre × collab heatmap")
    print("  • viz4_chi_square_heatmap.png - Chi-square results")
    print("  • analysis_results.txt - Full analysis report")
    print("=" * 70)


if __name__ == "__main__":
    main()
