"""
Data visualization module for TMDB movie analysis reports.

This module provides comprehensive visualization functions for presenting KPI results
and analytical insights from the movie data pipeline. Includes scatter plots with
correlation analysis, temporal trends, comparative charts, and ROI heatmaps.

All visualizations include:
- Professional styling with consistent color schemes
- Statistical annotations (correlations, means, trends)
- High-resolution PNG output (300 dpi) for reports
- Comprehensive logging of visualization operations

Functions:
    trendVisuals: Create scatter plot with correlation and quadrant analysis
    plot_yearly_box_office_trends: Line chart of yearly box office trends
    plot_franchise_vs_standalone_metrics: 2x2 comparison of franchise vs standalone films
    plot_roi_by_genre: Horizontal bar chart of ROI by genre with color gradient
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from src.utils.logger import setup_logger


def trendVisuals(data: pd.DataFrame, column1: str, column2: str, title: str, output_dir: str = "/data/visualizations"):
    """
    Create scatter plot with correlation analysis and performance quadrant classification.

    Produces an interactive scatter plot showing the relationship between two continuous
    variables with mean reference lines dividing the plot into four performance quadrants.
    Automatically calculates Pearson correlation and classifies data regions.

    Args:
        data (pd.DataFrame): Input DataFrame containing analysis columns.
        column1 (str): Name of X-axis column (should be numeric).
        column2 (str): Name of Y-axis column (should be numeric).
        title (str): Title for the plot.
        output_dir (str): Directory where plot PNG will be saved.
                         Defaults to "/data/visualizations".

    Returns:
        None: Displays plot directly and saves to output_dir/trend_{column1}_vs_{column2}.png

    Note:
        - Rows with null values in either column are automatically dropped
        - Highest Y-value is annotated with arrow for easy identification
        - Quadrant labels describe performance characteristics
    """
    # Initialize logger for this visualization operation
    logger = setup_logger(
        name="trend_visuals",
        log_file="/logs/trend_visuals.log"
    )

    logger.info(f"Starting trend visualization: {column1} vs {column2}")

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)

    # Remove rows with missing values in either analysis column
    temp = data[[column1, column2]].dropna()
    x = temp[column1]
    y = temp[column2]

    # Calculate Pearson correlation coefficient
    corr = x.corr(y)

    # Classify correlation strength for interpretation
    if corr >= 0.7:
        corr_label = "Strong Positive Correlation"
    elif corr >= 0.3:
        corr_label = "Moderate Positive Correlation"
    elif corr > -0.3:
        corr_label = "Weak / No Correlation"
    elif corr > -0.7:
        corr_label = "Moderate Negative Correlation"
    else:
        corr_label = "Strong Negative Correlation"

    # Calculate mean values for quadrant dividers
    x_mean = x.mean()
    y_mean = y.mean()

    # Create figure with appropriate size
    plt.figure(figsize=(10, 7))
    plt.scatter(
        x, y,
        alpha=0.75,
        color="#4c72b0",
        edgecolor="white",
        linewidth=0.6
    )

    # Draw mean reference lines to divide plot into quadrants
    plt.axvline(x_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column1}")
    plt.axhline(y_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column2}")

    # Highlight the point with the highest Y value
    max_idx = y.idxmax()
    plt.annotate(
        f"Highest {column2}",
        xy=(x.loc[max_idx], y.loc[max_idx]),
        xytext=(15, -25),
        textcoords="offset points",
        arrowprops=dict(arrowstyle="-|>", color="#c62828", linewidth=2),
        fontsize=10,
        color="black"
    )

    # Calculate dynamic quadrant label positioning based on data range
    x_min, x_max = plt.xlim()
    y_min, y_max = plt.ylim()

    # Position quadrant labels at centers of each quadrant region
    qx_left = (x_min + x_mean) / 2
    qx_right = (x_mean + x_max) / 2
    qy_bottom = (y_min + y_mean) / 2
    qy_top = (y_mean + y_max) / 2

    # Define common style for quadrant labels
    quadrant_style = dict(
        fontsize=9,
        ha="center",
        va="center",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#bbbbbb", alpha=0.85)
    )

    # Add quadrant labels with performance descriptions
    plt.text(qx_right, qy_top, f"High {column1} – High {column2}\n(Strong Performers)", **quadrant_style)
    plt.text(qx_left, qy_top, f"Low {column1} – High {column2}\n(Efficient / Overperformers)", **quadrant_style)
    plt.text(qx_right, qy_bottom, f"High {column1} – Low {column2}\n(Underperformers)", **quadrant_style)
    plt.text(qx_left, qy_bottom, f"Low {column1} – Low {column2}\n(Weak Performers)", **quadrant_style)

    # Display correlation coefficient and strength in corner box
    plt.text(
        0.98,
        0.98,
        f"r = {corr:.2f}\n{corr_label}",
        transform=plt.gca().transAxes,
        ha="right",
        va="top",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.4", fc="white", ec="#888888")
    )

    # Configure axis labels and title
    plt.xlabel(column1.upper())
    plt.ylabel(column2.upper())
    plt.title(title, fontsize=14, weight="bold")

    # Add legend and grid for clarity
    plt.legend(loc="lower right", frameon=False)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.tight_layout()

    # Save high-resolution PNG file
    filename = f"trend_{column1}_vs_{column2}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=300)

    plt.show()
    plt.close()

    logger.info(f"Trend visualization saved to {filepath}")


def plot_yearly_box_office_trends(data: pd.DataFrame, output_dir: str = "/data/visualizations"):
    """
    Visualize yearly box office performance trends over time.

    Creates a line chart showing mean revenue generated by movies for each
    release year. Includes annotations for peak and lowest-performing years.
    Useful for identifying industry trends and optimal production periods.

    Args:
        data (pd.DataFrame): Input DataFrame with columns:
                            - release_date: Date string or datetime object
                            - revenue_musd: Numeric revenue in millions USD
        output_dir (str): Directory where plot PNG will be saved.
                         Defaults to "/data/visualizations".

    Returns:
        None: Displays plot directly and saves to output_dir/yearly_box_office_trends.png

    Note:
        - Revenue is averaged (mean) per year, not summed
        - Input data is copied internally to avoid modifications
        - Peak and lowest years are automatically identified and annotated
    """
    # Initialize logger for this visualization operation
    logger = setup_logger(
        name="yearly_box_office_trends",
        log_file="/logs/yearly_box_office_trends.log"
    )

    logger.info("Starting yearly box office trend visualization")

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)

    # Create copy to avoid modifying original data
    data = data.copy()
    
    # Extract year from release_date for grouping
    data["release_year"] = pd.to_datetime(data["release_date"]).dt.year
    
    # Calculate mean revenue per year
    yearly = data.groupby("release_year")["revenue_musd"].mean()

    # Create figure with appropriate size for time series
    plt.figure(figsize=(11, 5.5))

    # Plot trend line with markers
    plt.plot(
        yearly.index,
        yearly.values,
        marker="o",
        linestyle="-",
        linewidth=2.5,
        color="#1f4fd8",              # Deep blue for professional appearance
        markerfacecolor="white",
        markeredgecolor="#1f4fd8",
        markeredgewidth=1.8,
        label="Yearly Average Revenue"
    )

    # Identify peak and lowest performing years for annotation
    peak_year = yearly.idxmax()
    peak_value = yearly.max()

    low_year = yearly.idxmin()
    low_value = yearly.min()

    # Annotate peak year with green arrow (positive/success color)
    plt.annotate(
        f"Peak Revenue\n{peak_value:.1f}M USD",
        xy=(peak_year, peak_value),
        xytext=(20, -35),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="-|>",
            color="#2e7d32",
            linewidth=2
        ),
        fontsize=10,
        color="black",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#2e7d32"),
        zorder=5  # Ensure annotation appears on top of line
    )

    # Annotate lowest year with red arrow (warning/negative color)
    plt.annotate(
        f"Lowest Revenue\n{low_value:.1f}M USD",
        xy=(low_year, low_value),
        xytext=(-25, 30),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="-|>",
            color="#c62828",
            linewidth=2
        ),
        fontsize=10,
        color="black",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#c62828"),
        zorder=5  # Ensure annotation appears on top of line
    )

    # Configure axis labels and title
    plt.title("Yearly Box Office Performance", fontsize=14, weight="bold")
    plt.xlabel("Year", fontsize=11)
    plt.ylabel("Average Revenue (Million USD)", fontsize=11)

    # Add grid for easier value reading
    plt.grid(True, linestyle="--", alpha=0.35)

    # Add legend
    plt.legend(frameon=False)

    plt.tight_layout()

    # Save high-resolution PNG file
    filepath = os.path.join(output_dir, "yearly_box_office_trends.png")
    plt.savefig(filepath, dpi=300)

    plt.show()
    plt.close()

    logger.info(f"Yearly box office trend saved to {filepath}")



def plot_franchise_vs_standalone_metrics(data: pd.DataFrame, output_dir: str = "/data/visualizations"):
    """
    Compare key performance metrics between franchise and standalone movies.

    Creates a 2x2 subplot grid with four bar charts comparing financial, ratings,
    and business performance metrics. Each subplot shows side-by-side bars for
    franchise vs standalone movie categories with values labeled on bars.

    Args:
        data (pd.DataFrame): Input DataFrame with columns:
                            - movie_type: Either "Franchise" or "Standalone"
                            - revenue_musd: Mean revenue in millions USD
                            - budget_musd: Mean budget in millions USD
                            - vote_average: Mean audience rating
                            - ROI: Mean return on investment ratio
        output_dir (str): Directory where plot PNG will be saved.
                         Defaults to "/data/visualizations".

    Returns:
        None: Displays plot directly and saves to output_dir/franchise_vs_standalone_metrics.png

    Note:
        - Data is sorted by movie_type for consistent bar ordering across subplots
        - Professional color scheme: blue for franchises, charcoal for standalone
        - Value labels are displayed above each bar for precision
    """
    # Initialize logger for this visualization operation
    logger = setup_logger(
        name="franchise_vs_standalone",
        log_file="/logs/franchise_vs_standalone.log"
    )

    logger.info("Starting franchise vs standalone visualization")

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)

    # Define metrics to display in subplot grid
    # Format: plot_title -> (dataframe_column, y_axis_label)
    metrics = {
        "Revenue (M USD)": ("revenue_musd", "Mean Revenue"),
        "Budget (M USD)": ("budget_musd", "Mean Budget"),
        "Average Rating": ("vote_average", "Mean Rating"),
        "ROI": ("ROI", "Mean ROI")
    }

    # Sort data by movie type for consistent ordering across subplots
    data = data.sort_values("movie_type")

    # Extract movie type labels for bar positioning
    movie_types = data["movie_type"].values

    # Define colors for each movie type (blue = franchises, charcoal = standalone)
    colors = {
        "Franchise": "#1f4fd8",    # Professional blue
        "Standalone": "#6b7280"   # Neutral charcoal
    }

    # Create 2x2 subplot grid
    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    axes = axes.flatten()  # Flatten to 1D for easier iteration

    # Create a bar chart for each metric
    for ax, (title, (col, ylabel)) in zip(axes, metrics.items()):
        # Extract values for this metric
        values = data[col].values
        
        # Create x-axis positions for bars (0, 1 for two categories)
        x = np.arange(len(values))

        # Create bars with colors determined by movie type
        bars = ax.bar(
            x,
            values,
            width=0.55,
            color=[colors[m] for m in movie_types]  # Color each bar by movie type
        )

        # Add value labels on top of each bar
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,  # Center horizontally
                height * 1.02,  # Slightly above the bar
                f"{height:.1f}",
                ha="center",
                va="bottom",
                fontsize=10,
                color="#111827"
            )

        # Configure subplot appearance
        ax.set_title(title, fontsize=12, weight="bold", pad=10)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(movie_types, fontsize=10)

        # Add grid and remove spines for clean appearance
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        ax.set_axisbelow(True)  # Grid behind bars
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Add overall title spanning all subplots
    fig.suptitle(
        "Franchise vs Standalone Movies — Metric-wise Comparison",
        fontsize=15,
        weight="bold"
    )

    # Adjust layout with space for super title
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save high-resolution PNG file
    filepath = os.path.join(output_dir, "franchise_vs_standalone_metrics.png")
    plt.savefig(filepath, dpi=300)
    plt.show()
    plt.close()

    logger.info(f"Franchise vs Standalone chart saved to {filepath}")



def plot_roi_by_genre(
    data,
    top_n=15,
    figsize=(12, 7),
    output_dir: str = "/data/visualizations"
):
    """
    Visualize median ROI by genre with color-intensity heatmap.

    Creates a horizontal bar chart showing median return on investment (ROI)
    by genre, displaying the top N best-performing genres. Colors are scaled
    from low ROI (dark blue) to high ROI (bright yellow) using the Viridis
    colormap for intuitive visual interpretation.

    Args:
        data (pd.DataFrame): Input DataFrame with columns:
                            - genres: Genre name (string)
                            - median_roi: Median ROI for that genre (numeric)
        top_n (int): Number of top-performing genres to display.
                    Only genres with highest ROI are shown.
                    Defaults to 15.
        figsize (tuple): Matplotlib figure size as (width, height) in inches.
                        Defaults to (12, 7).
        output_dir (str): Directory where plot PNG will be saved.
                         Defaults to "/data/visualizations".

    Returns:
        None: Displays plot directly and saves to output_dir/median_roi_by_genre.png

    Note:
        - Null values in genres or median_roi columns are dropped before plotting
        - Bars are inverted (best genre on top) for natural top-to-bottom reading
        - Color intensity indicates ROI magnitude (viridis: blue to yellow gradient)
        - Value labels are displayed at the end of each bar for precision
    """
    # Initialize logger for this visualization operation
    logger = setup_logger(
        name="roi_by_genre",
        log_file="/logs/roi_by_genre.log"
    )

    logger.info("Starting ROI by genre visualization")

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)

    # Create defensive copy to avoid modifying original data
    df = data.copy()

    # Remove rows with missing data in critical columns
    df = df.dropna(subset=["genres", "median_roi"])

    # Sort by median_roi descending and keep only top_n genres
    # This focuses the visualization on the most successful genres
    df = df.sort_values("median_roi", ascending=False).head(top_n)

    # Extract genre names and ROI values for plotting
    genres = df["genres"]
    roi = df["median_roi"]

    # Normalize ROI values to [0, 1] range for color mapping
    # This ensures color intensity is proportional to ROI magnitude
    norm = (roi - roi.min()) / (roi.max() - roi.min() + 1e-6)
    
    # Map normalized values to Viridis colormap (dark blue to bright yellow)
    # Provides intuitive color progression: low ROI (dark) to high ROI (bright)
    colors = plt.cm.viridis(norm)

    # Create figure and horizontal bar chart
    fig, ax = plt.subplots(figsize=figsize)

    bars = ax.barh(
        genres,
        roi,
        color=colors,
        edgecolor="black",
        linewidth=0.6
    )

    # Add value labels at the end of each bar for precise reading
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width,
            bar.get_y() + bar.get_height() / 2,  # Vertically centered
            f"{width:.2f}",
            va="center",
            ha="left",
            fontsize=10
        )

    # Set titles and axis labels
    ax.set_title(
        "Median Return on Investment (ROI) by Genre",
        fontsize=16,
        fontweight="bold",
        pad=12
    )
    ax.set_xlabel("Median ROI", fontsize=12)
    ax.set_ylabel("Genre", fontsize=12)

    # Add grid and configure appearance
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)  # Grid behind bars

    # Invert y-axis so best genre appears at the top
    # Improves readability by following natural top-to-bottom reading order
    ax.invert_yaxis()

    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save high-resolution PNG file
    filepath = os.path.join(output_dir, "median_roi_by_genre.png")
    plt.savefig(filepath, dpi=300)
    plt.show()
    plt.close()

    logger.info(f"ROI by genre chart saved to {filepath}")




def main():
    """
    Main entry point for visualization module.

    Can be used to test visualization functions during development.
    """
    pass


if __name__ == "__main__":
    main()