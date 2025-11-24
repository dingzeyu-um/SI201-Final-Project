"""
analyze_visualize.py
SI 201 Final Project - Analysis and Visualization

This file:
1. Selects data from ALL tables using JOINs
2. Performs statistical calculations (chi-square test)
3. Creates visualizations (3 required + 1 bonus)
4. Exports results to text file

Run this AFTER all data gathering and processing is complete.

Team Members Responsible: 
- Analysis/Calculations: [Member 2 Name]
- Visualizations: [Member 3 Name]
"""

import sqlite3
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency
import matplotlib.pyplot as plt
import seaborn as sns

DB_NAME = 'music_income.db'

# Configure visualization style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


# ==============================================================================
# ANALYSIS FUNCTIONS (Team Member 2)
# ==============================================================================

def select_data_with_join():
    """
    Select data from ALL tables using database JOINs.
    This satisfies the "at least one database join" requirement.
    
    Returns:
        pandas.DataFrame: Query results
    """
    conn = sqlite3.connect(DB_NAME)
    
    # Complex JOIN query across all 5 tables
    query = '''
        SELECT 
            CASE 
                WHEN r.median_income < 55000 THEN 'Low (<$55k)'
                WHEN r.median_income < 70000 THEN 'Middle ($55k-$70k)'
                ELSE 'High (>$70k)'
            END as income_bracket,
            g.genre_name,
            COUNT(*) as count,
            AVG(r.median_income) as avg_income,
            AVG(t.popularity) as avg_popularity
        FROM regional_popularity rp
        JOIN regions r ON rp.region_id = r.region_id
        JOIN tracks t ON rp.track_id = t.track_id
        JOIN track_genres tg ON t.track_id = tg.track_id
        JOIN genres g ON tg.genre_id = g.genre_id
        GROUP BY income_bracket, g.genre_name
        HAVING count > 3
        ORDER BY income_bracket, count DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def perform_chi_square_test(df):
    """
    Perform chi-square test of independence.
    Tests if income level and genre preference are independent.
    
    Args:
        df (DataFrame): Data from select_data_with_join()
        
    Returns:
        tuple: (contingency, chi2, p_value, dof, expected, cramers_v)
    """
    # Create contingency table
    contingency = df.pivot_table(
        values='count',
        index='income_bracket',
        columns='genre_name',
        fill_value=0
    )
    
    # Perform chi-square test
    chi2, p_value, dof, expected = chi2_contingency(contingency)
    
    # Calculate Cramér's V (effect size)
    n = contingency.sum().sum()
    min_dim = min(contingency.shape) - 1
    cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 else 0
    
    return contingency, chi2, p_value, dof, expected, cramers_v


def print_analysis_results(contingency, chi2, p_value, dof, cramers_v):
    """
    Print formatted analysis results to console.
    
    Args:
        contingency: Contingency table
        chi2: Chi-square statistic
        p_value: P-value
        dof: Degrees of freedom
        cramers_v: Cramér's V effect size
    """
    print("\n" + "="*70)
    print("STATISTICAL ANALYSIS RESULTS")
    print("="*70)
    
    print("\nCONTINGENCY TABLE (Observed Frequencies):")
    print("-"*70)
    print(contingency)
    
    print("\n" + "="*70)
    print("CHI-SQUARE TEST OF INDEPENDENCE")
    print("="*70)
    print(f"Chi-Square Statistic: {chi2:.4f}")
    print(f"P-value: {p_value:.6f}")
    print(f"Degrees of Freedom: {dof}")
    print(f"Significance Level (α): 0.05")
    
    print("\nINTERPRETATION:")
    if p_value < 0.05:
        print("  ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
        print("  → Income level and genre preference ARE related")
        print("  → We reject the null hypothesis of independence")
    else:
        print("  ✗ NOT STATISTICALLY SIGNIFICANT (p ≥ 0.05)")
        print("  → No strong evidence of relationship")
        print("  → We fail to reject the null hypothesis")
    
    print(f"\nEFFECT SIZE (Cramér's V): {cramers_v:.3f}")
    if cramers_v < 0.1:
        print("  → Small effect (V < 0.1)")
    elif cramers_v < 0.3:
        print("  → Medium effect (0.1 ≤ V < 0.3)")
    else:
        print("  → Large effect (V ≥ 0.3)")
    
    print("="*70)


def export_results_to_file(df, contingency, chi2, p_value, dof, cramers_v):
    """
    Export all results to a text file.
    Satisfies the "write calculated data to a file" requirement.
    
    Args:
        df: Original data
        contingency: Contingency table
        chi2, p_value, dof, cramers_v: Test results
    """
    filename = 'analysis_results.txt'
    
    with open(filename, 'w') as f:
        f.write("="*70 + "\n")
        f.write("MUSIC TRENDS VS INCOME LEVEL - ANALYSIS RESULTS\n")
        f.write("SI 201 Final Project\n")
        f.write("="*70 + "\n\n")
        
        # Data summary
        f.write("DATA SUMMARY\n")
        f.write("-"*70 + "\n")
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM tracks')
        f.write(f"Total Tracks: {cursor.fetchone()[0]}\n")
        
        cursor.execute('SELECT COUNT(*) FROM regions')
        f.write(f"Total Regions: {cursor.fetchone()[0]}\n")
        
        cursor.execute('SELECT COUNT(*) FROM genres')
        f.write(f"Total Genres: {cursor.fetchone()[0]}\n")
        
        cursor.execute('SELECT COUNT(*) FROM regional_popularity')
        f.write(f"Regional Preferences: {cursor.fetchone()[0]}\n")
        
        conn.close()
        
        # Query results
        f.write("\n" + "="*70 + "\n")
        f.write("GENRE PREFERENCES BY INCOME BRACKET\n")
        f.write("-"*70 + "\n")
        f.write(df.to_string(index=False))
        f.write("\n")
        
        # Contingency table
        f.write("\n" + "="*70 + "\n")
        f.write("CONTINGENCY TABLE\n")
        f.write("-"*70 + "\n")
        f.write(contingency.to_string())
        f.write("\n")
        
        # Chi-square results
        f.write("\n" + "="*70 + "\n")
        f.write("CHI-SQUARE TEST RESULTS\n")
        f.write("-"*70 + "\n")
        f.write(f"Chi-Square Statistic: {chi2:.4f}\n")
        f.write(f"P-value: {p_value:.6f}\n")
        f.write(f"Degrees of Freedom: {dof}\n")
        f.write(f"Cramér's V (Effect Size): {cramers_v:.3f}\n")
        f.write(f"\nSignificant at α=0.05: {'Yes' if p_value < 0.05 else 'No'}\n")
        
        f.write("\n" + "="*70 + "\n")
    
    print(f"✓ Results exported to {filename}")


# ==============================================================================
# VISUALIZATION FUNCTIONS (Team Member 3)
# ==============================================================================

def create_heatmap_observed(contingency, chi2, p_value):
    """
    Visualization 1: Heatmap of observed frequencies.
    
    Args:
        contingency: Contingency table
        chi2: Chi-square statistic
        p_value: P-value
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    sns.heatmap(
        contingency,
        annot=True,
        fmt='g',
        cmap='YlOrRd',
        cbar_kws={'label': 'Number of Tracks'},
        linewidths=1,
        linecolor='white',
        ax=ax
    )
    
    significance = "(Significant)" if p_value < 0.05 else "(Not Significant)"
    plt.title(
        f'Music Genre Preferences by Income Level\n'
        f'χ² = {chi2:.2f}, p-value = {p_value:.4f} {significance}',
        fontsize=16,
        fontweight='bold',
        pad=20
    )
    plt.xlabel('Music Genre', fontsize=13, fontweight='bold')
    plt.ylabel('Income Bracket', fontsize=13, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    
    plt.tight_layout()
    plt.savefig('viz1_heatmap_observed.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Saved: viz1_heatmap_observed.png")


def create_heatmap_percentage(contingency):
    """
    Visualization 2: Heatmap showing percentage distribution.
    
    Args:
        contingency: Contingency table
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Calculate percentages within each income bracket
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
        vmax=percentages.max().max()
    )
    
    plt.title(
        'Genre Distribution Within Each Income Bracket (%)',
        fontsize=16,
        fontweight='bold',
        pad=20
    )
    plt.xlabel('Music Genre', fontsize=13, fontweight='bold')
    plt.ylabel('Income Bracket', fontsize=13, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    
    plt.tight_layout()
    plt.savefig('viz2_heatmap_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Saved: viz2_heatmap_percentage.png")


def create_bar_chart(contingency):
    """
    Visualization 3: Grouped bar chart.
    
    Args:
        contingency: Contingency table
    """
    fig, ax = plt.subplots(figsize=(14, 8))
    
    contingency.plot(
        kind='bar',
        ax=ax,
        width=0.8,
        edgecolor='black',
        linewidth=0.7
    )
    
    plt.title(
        'Music Genre Popularity Across Income Brackets',
        fontsize=16,
        fontweight='bold',
        pad=20
    )
    plt.xlabel('Income Bracket', fontsize=13, fontweight='bold')
    plt.ylabel('Number of Popular Tracks', fontsize=13, fontweight='bold')
    plt.xticks(rotation=0)
    plt.legend(
        title='Genre',
        bbox_to_anchor=(1.05, 1),
        loc='upper left',
        frameon=True,
        shadow=True
    )
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%.0f', padding=3, fontsize=8)
    
    plt.tight_layout()
    plt.savefig('viz3_bar_chart.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Saved: viz3_bar_chart.png")


def create_heatmap_residuals(contingency, expected):
    """
    Visualization 4 (BONUS): Standardized residuals heatmap.
    Shows which combinations are over/underrepresented.
    
    Args:
        contingency: Observed frequencies
        expected: Expected frequencies from chi-square test
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Calculate standardized residuals
    residuals = (contingency - expected) / np.sqrt(expected)
    
    sns.heatmap(
        residuals,
        annot=True,
        fmt='.2f',
        cmap='RdBu_r',
        center=0,
        vmin=-3,
        vmax=3,
        cbar_kws={'label': 'Standardized Residual'},
        linewidths=1,
        linecolor='white',
        ax=ax
    )
    
    plt.title(
        'Standardized Residuals: Deviation from Expected\n'
        '(Red = More than expected, Blue = Less than expected)',
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    plt.xlabel('Music Genre', fontsize=13, fontweight='bold')
    plt.ylabel('Income Bracket', fontsize=13, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    
    plt.tight_layout()
    plt.savefig('viz4_heatmap_residuals.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Saved: viz4_heatmap_residuals.png (BONUS)")


# ==============================================================================
# MAIN FUNCTION
# ==============================================================================

def main():
    """
    Main function to perform analysis and create visualizations.
    """
    print("\n" + "="*70)
    print("ANALYSIS AND VISUALIZATION")
    print("="*70)
    
    # Step 1: Select data with JOINs
    print("\n[1] Selecting data from database with JOINs...")
    df = select_data_with_join()
    
    if df.empty:
        print("✗ No data found! Please run all gather scripts first.")
        return
    
    print(f"✓ Retrieved {len(df)} rows of aggregated data")
    print("\nSample data:")
    print(df.head(10))
    
    # Step 2: Perform chi-square test
    print("\n[2] Performing chi-square analysis...")
    contingency, chi2, p_value, dof, expected, cramers_v = perform_chi_square_test(df)
    print_analysis_results(contingency, chi2, p_value, dof, cramers_v)
    
    # Step 3: Create visualizations
    print("\n[3] Creating visualizations...")
    create_heatmap_observed(contingency, chi2, p_value)
    create_heatmap_percentage(contingency)
    create_bar_chart(contingency)
    create_heatmap_residuals(contingency, expected)
    
    # Step 4: Export results
    print("\n[4] Exporting results to file...")
    export_results_to_file(df, contingency, chi2, p_value, dof, cramers_v)
    
    # Final summary
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE!")
    print("="*70)
    print("\nGenerated files:")
    print("  • viz1_heatmap_observed.png")
    print("  • viz2_heatmap_percentage.png")
    print("  • viz3_bar_chart.png")
    print("  • viz4_heatmap_residuals.png (BONUS)")
    print("  • analysis_results.txt")
    print("="*70)


if __name__ == "__main__":
    main()
