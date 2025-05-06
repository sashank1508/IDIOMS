#!/usr/bin/env python3
"""
Visualization utility for IDIOMS Adaptive Query Distribution benchmark results.
Reads benchmark_results.csv and generates visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import sys

def load_benchmark_results(csv_file='benchmark_results.csv'):
    """Load benchmark results from CSV file."""
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found")
        sys.exit(1)
        
    return pd.read_csv(csv_file)

def plot_overall_improvement(results_df):
    """Plot overall performance comparison."""
    # Calculate overall times
    standard_total = results_df['StandardTime'].sum()
    adaptive_total = results_df['AdaptiveTime'].sum()
    
    # Create bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(['Standard IDIOMS', 'Adaptive IDIOMS'], 
                  [standard_total, adaptive_total],
                  color=['#3498db', '#2ecc71'])
    
    # Add improvement percentage
    improvement = (standard_total - adaptive_total) / standard_total * 100
    plt.text(1, adaptive_total + 5, f"{improvement:.2f}% improvement", 
             ha='center', fontsize=12, fontweight='bold')
    
    # Add time values
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                f"{height:.2f} ms",
                ha='center', va='bottom', fontsize=10)
    
    plt.title('Overall Performance Comparison', fontsize=16)
    plt.ylabel('Total Query Time (ms)', fontsize=14)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('overall_improvement.png', dpi=300)
    print("Saved overall_improvement.png")

def plot_performance_by_popularity(results_df):
    """Plot performance improvement by popularity."""
    # Sort by popularity
    results_df = results_df.sort_values('Popularity', ascending=False)
    
    # Select top 10 queries for clarity
    top_queries = results_df.head(10)
    
    # Create figure
    plt.figure(figsize=(12, 8))
    
    # Plot bars
    x = np.arange(len(top_queries))
    width = 0.35
    
    standard_bars = plt.bar(x - width/2, top_queries['StandardTime'], width, 
                          label='Standard IDIOMS', color='#3498db')
    adaptive_bars = plt.bar(x + width/2, top_queries['AdaptiveTime'], width,
                          label='Adaptive IDIOMS', color='#2ecc71')
    
    # Customizations
    plt.xlabel('Query Pattern', fontsize=14)
    plt.ylabel('Query Time (ms)', fontsize=14)
    plt.title('Performance by Query Pattern (Most Popular First)', fontsize=16)
    plt.xticks(x, top_queries['Query'], rotation=45, ha='right')
    plt.legend()
    
    # Add replication factor
    for i, query in enumerate(top_queries['Query']):
        repfactor = top_queries.iloc[i]['ReplicationFactor']
        plt.text(i, top_queries.iloc[i]['StandardTime'] + 1, 
                f"RF: {repfactor}", ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.grid(axis='y', alpha=0.3)
    plt.savefig('performance_by_popularity.png', dpi=300)
    print("Saved performance_by_popularity.png")

def plot_improvement_vs_popularity(results_df):
    """Plot improvement percentage vs popularity."""
    plt.figure(figsize=(12, 8))
    
    # Create scatter plot
    plt.scatter(results_df['Popularity'], results_df['Improvement'], 
               s=results_df['ReplicationFactor']*30, # Size based on replication factor
               alpha=0.7, c=results_df['ReplicationFactor'], cmap='viridis')
    
    # Add trend line
    z = np.polyfit(results_df['Popularity'], results_df['Improvement'], 1)
    p = np.poly1d(z)
    plt.plot(results_df['Popularity'], p(results_df['Popularity']), 
             "r--", alpha=0.8, label=f"Trend: y={z[0]:.2f}x+{z[1]:.2f}")
    
    # Customizations
    plt.xlabel('Popularity Score', fontsize=14)
    plt.ylabel('Performance Improvement (%)', fontsize=14)
    plt.title('Performance Improvement vs. Query Popularity', fontsize=16)
    plt.grid(alpha=0.3)
    plt.colorbar(label='Replication Factor')
    plt.legend()
    
    # Annotate points
    for i, row in results_df.iterrows():
        # Only annotate some points to avoid clutter
        if row['Popularity'] > 10 or row['Improvement'] > 30:
            plt.annotate(row['Query'], 
                        (row['Popularity'], row['Improvement']),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8)
    
    plt.tight_layout()
    plt.savefig('improvement_vs_popularity.png', dpi=300)
    print("Saved improvement_vs_popularity.png")

def plot_replication_impact(results_df):
    """Plot the impact of replication factor on performance."""
    # Group by replication factor
    grouped = results_df.groupby('ReplicationFactor').agg({
        'StandardTime': 'mean',
        'AdaptiveTime': 'mean',
        'Improvement': 'mean',
        'Popularity': 'mean'
    }).reset_index()
    
    # Create bar chart
    plt.figure(figsize=(12, 6))
    x = grouped['ReplicationFactor']
    
    # Plot bars
    plt.bar(x, grouped['Improvement'], color='#f39c12')
    
    # Customizations
    plt.xlabel('Replication Factor', fontsize=14)
    plt.ylabel('Average Improvement (%)', fontsize=14)
    plt.title('Impact of Replication Factor on Query Performance', fontsize=16)
    plt.grid(axis='y', alpha=0.3)
    
    # Add annotations
    for i, row in grouped.iterrows():
        plt.text(row['ReplicationFactor'], row['Improvement']+1, 
                f"{row['Improvement']:.2f}%", ha='center')
    
    plt.tight_layout()
    plt.savefig('replication_impact.png', dpi=300)
    print("Saved replication_impact.png")

def main():
    # Load data
    results = load_benchmark_results()
    
    # Create visualizations
    plot_overall_improvement(results)
    plot_performance_by_popularity(results)
    plot_improvement_vs_popularity(results)
    plot_replication_impact(results)
    
    print("All visualizations generated successfully")

if __name__ == "__main__":
    main()