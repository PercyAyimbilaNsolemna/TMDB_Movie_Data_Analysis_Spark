"""
Data visualization module for TMDB movie analysis.

This module provides visualization functions for analyzing movie data including:
- Trend analysis with correlation visualization and quadrant classification
- Temporal analysis of box office performance across years
- Comparative analysis between franchise and standalone movies
- Genre-based ROI analysis with heatmap visualization

All visualizations include statistical annotations (correlations, means, peaks)
and are designed for interactive exploration and presentation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def trendVisuals(data: pd.DataFrame, column1: str, column2: str, title: str):
    """
    Create an interactive scatter plot with correlation analysis and quadrant classification.

    Visualizes the relationship between two continuous variables using a scatter plot
    with mean reference lines and quadrant labels. Automatically calculates Pearson
    correlation coefficient and classifies data into performance quadrants.

    The four quadrants represent:
        - High/High: Strong Performers (high on both metrics)
        - Low/High: Efficient/Overperformers (low input, high output)
        - High/Low: Underperformers (high input, low output)
        - Low/Low: Weak Performers (low on both metrics)

    Args:
        data (pd.DataFrame): Input data containing the two columns to analyze.
        column1 (str): Name of first column (X-axis). Should be numeric.
        column2 (str): Name of second column (Y-axis). Should be numeric.
        title (str): Title displayed at the top of the plot.

    Returns:
        None: Displays the plot directly using matplotlib.show().

    Note:
        - Rows with missing values in either column are automatically dropped
        - Correlation strength is interpreted as: >= 0.7 (strong), 0.3-0.7 (moderate), -0.3-0.3 (weak), < -0.7 (strong negative)
        - The highest Y-value is highlighted with an annotation arrow
    """
    # Remove rows with missing values in the two analysis columns
    temp = data[[column1, column2]].dropna()
    x = temp[column1]
    y = temp[column2]

    # Calculate Pearson correlation coefficient
    # Ranges from -1 (perfect negative) to 1 (perfect positive)
    corr = x.corr(y)

    # Classify correlation strength for interpretation
    # These thresholds are based on standard statistical practice
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

    # Calculate mean values for reference lines
    # These divide the plot into quadrants for performance classification
    x_mean = x.mean()
    y_mean = y.mean()

    # Create scatter plot with styling
    plt.figure(figsize=(10, 7))
    plt.scatter(
        x, y,
        alpha=0.75,  # Slight transparency to show overlapping points
        color="#4c72b0",
        edgecolor="white",
        linewidth=0.6
    )

    # Draw mean reference lines to divide the plot into quadrants
    plt.axvline(x_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column1}")
    plt.axhline(y_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column2}")

    # Highlight the point with the highest Y-value with an annotation
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

    # Get axis limits to calculate quadrant label positions dynamically
    # This ensures labels are positioned correctly regardless of data range
    x_min, x_max = plt.xlim()
    y_min, y_max = plt.ylim()

    # Calculate quadrant centers for label placement
    # Centers are positioned between the mean and axis limits for optimal readability
    qx_left = (x_min + x_mean) / 2
    qx_right = (x_mean + x_max) / 2
    qy_bottom = (y_min + y_mean) / 2
    qy_top = (y_mean + y_max) / 2

    # Define common styling for quadrant labels
    quadrant_style = dict(
        fontsize=9,
        ha="center",
        va="center",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#bbbbbb", alpha=0.85)
    )

    # Add quadrant labels to classify movie regions
    # Right-upper: high on both dimensions (strong performers)
    plt.text(qx_right, qy_top, f"High {column1} – High {column2}\n(Strong Performers)", **quadrant_style)
    # Left-upper: low on x, high on y (efficient/overperformers)
    plt.text(qx_left, qy_top, f"Low {column1} – High {column2}\n(Efficient / Overperformers)", **quadrant_style)
    # Right-lower: high on x, low on y (underperformers)
    plt.text(qx_right, qy_bottom, f"High {column1} – Low {column2}\n(Underperformers)", **quadrant_style)
    # Left-lower: low on both dimensions (weak performers)
    plt.text(qx_left, qy_bottom, f"Low {column1} – Low {column2}\n(Weak Performers)", **quadrant_style)

    # Display correlation coefficient and strength label in upper right corner
    # Using transformed coordinates for position independence from data range
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

    # Set axis labels and title
    plt.xlabel(column1.upper())
    plt.ylabel(column2.upper())
    plt.title(title, fontsize=14, weight="bold")

    # Add legend and grid for clarity
    plt.legend(loc="lower right", frameon=False)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.tight_layout()
    plt.show()


def plot_yearly_box_office_trends(data: pd.DataFrame):
    """
    Visualize average movie revenue trends across release years.

    Creates a line plot showing yearly average revenue (in millions USD)
    with annotations for peak and lowest-performing years. Useful for
    identifying industry trends and determining optimal production periods.

    Args:
        data (pd.DataFrame): Input data with columns:
                            - release_date: datetime or date string for extraction
                            - revenue_musd: Numeric revenue in millions USD

    Returns:
        None: Displays the plot directly using matplotlib.show().

    Note:
        - Data is copied internally to avoid modifying original DataFrame
        - Revenue values per year are averaged (mean), not summed
        - Peak and lowest years are automatically identified and annotated
    """
    # Create a copy to avoid modifying the original DataFrame
    data = data.copy()
    
    # Extract year from release_date and add as new column
    # Enables grouping by year for trend analysis
    data["release_year"] = pd.to_datetime(data["release_date"]).dt.year
    
    # Calculate mean revenue per year
    # This shows the typical revenue for movies released in each year
    yearly = data.groupby("release_year")["revenue_musd"].mean()

    # Create figure with appropriate size for time series
    plt.figure(figsize=(11, 5.5))

    # Plot yearly trend line with marker
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

    # Identify the best and worst years for annotation
    peak_year = yearly.idxmax()
    peak_value = yearly.max()

    low_year = yearly.idxmin()
    low_value = yearly.min()

    # Annotate peak year with green arrow and box
    plt.annotate(
        f"Peak Revenue\n{peak_value:.1f}M USD",
        xy=(peak_year, peak_value),
        xytext=(20, -35),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="-|>",
            color="#2e7d32",  # Green for positive/success
            linewidth=2
        ),
        fontsize=10,
        color="black",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#2e7d32"),
        zorder=5  # Ensure annotation appears on top
    )

    # Annotate lowest year with red arrow and box
    plt.annotate(
        f"Lowest Revenue\n{low_value:.1f}M USD",
        xy=(low_year, low_value),
        xytext=(-25, 30),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="-|>",
            color="#c62828",  # Red for warning/low performance
            linewidth=2
        ),
        fontsize=10,
        color="black",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#c62828"),
        zorder=5  # Ensure annotation appears on top
    )

    # Set titles and labels
    plt.title("Yearly Box Office Performance", fontsize=14, weight="bold")
    plt.xlabel("Year", fontsize=11)
    plt.ylabel("Average Revenue (Million USD)", fontsize=11)

    # Add grid for easier value reading
    plt.grid(True, linestyle="--", alpha=0.35)

    # Add legend
    plt.legend(frameon=False)

    plt.tight_layout()
    plt.show()


def plot_franchise_vs_standalone_metrics(data: pd.DataFrame):
    """
    Compare key performance metrics between franchise and standalone movies.

    Creates a 2x2 subplot showing side-by-side bar charts comparing four metrics:
    revenue, budget, average rating, and ROI. Color-coded bars distinguish between
    franchise and standalone movie categories.

    Args:
        data (pd.DataFrame): Input data with columns:
                            - movie_type: Either "Franchise" or "Standalone"
                            - revenue_musd: Mean revenue in millions USD
                            - budget_musd: Mean budget in millions USD
                            - vote_average: Mean average rating
                            - ROI: Mean return on investment ratio

    Returns:
        None: Displays the plot directly using matplotlib.show().

    Note:
        - Data is sorted by movie_type for consistent bar ordering
        - Value labels are displayed above each bar for precise reading
        - Professional color scheme: blue for franchises, charcoal for standalone
    """
    # Import numpy locally (already imported at module level but for clarity)
    import numpy as np
    import matplotlib.pyplot as plt

    # Define metrics to display and their corresponding DataFrame columns
    metrics = {
        "Revenue (M USD)": ("revenue_musd", "Mean Revenue"),
        "Budget (M USD)": ("budget_musd", "Mean Budget"),
        "Average Rating": ("vote_average", "Mean Rating"),
        "ROI": ("ROI", "Mean ROI")
    }

    # Sort data to ensure consistent ordering of movie types across all subplots
    data = data.sort_values("movie_type")

    # Extract movie type labels for x-axis
    movie_types = data["movie_type"].values

    # Define colors for each movie type
    colors = {
        "Franchise": "#1f4fd8",    # Professional blue
        "Standalone": "#6b7280"   # Neutral charcoal
    }

    # Create 2x2 subplot grid for the four metrics
    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    axes = axes.flatten()  # Flatten to 1D for easier iteration

    # Iterate through metrics and create subplot for each
    for ax, (title, (col, ylabel)) in zip(axes, metrics.items()):
        # Extract values for this metric
        values = data[col].values
        
        # Create x-axis positions for bars
        x = np.arange(len(values))

        # Create bars with colors determined by movie type
        bars = ax.bar(
            x,
            values,
            width=0.55,
            color=[colors[m] for m in movie_types]  # Color each bar by its movie type
        )

        # Add value labels on top of each bar for precise reading
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

        # Add grid and remove top/right spines for cleaner appearance
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
    plt.show()



def plot_roi_by_genre(
    data,
    top_n=15,
    figsize=(12, 7)
):
    """
    Visualize median ROI across genres with color-intensity heatmap.

    Creates a horizontal bar chart showing median return on investment (ROI)
    by genre, displaying top N genres ranked by ROI. Colors are scaled
    from low ROI (dark) to high ROI (bright) using the Viridis colormap.

    Args:
        data (pd.DataFrame): Input data with columns:
                            - genres: Genre name (string)
                            - median_roi: Median ROI for that genre (numeric)
        top_n (int): Number of top-performing genres to display.
                    Defaults to 15. Only genres with highest ROI are shown.
        figsize (tuple): Matplotlib figure size as (width, height) in inches.
                        Defaults to (12, 7).

    Returns:
        None: Displays the plot directly using matplotlib.show().

    Note:
        - Null values in genres or median_roi columns are dropped before plotting
        - Bars are inverted (best genre on top) for easier top-to-bottom reading
        - Color intensity indicates ROI magnitude (blue-green-yellow spectrum)
        - Value labels are displayed at the end of each bar
    """

    # Create a defensive copy to avoid modifying the original DataFrame
    df = data.copy()

    # Remove rows with missing values in critical columns
    # This prevents visualization errors from null data
    df = df.dropna(subset=["genres", "median_roi"])

    # Sort by median_roi in descending order and keep only top_n genres
    # This focuses the visualization on the most successful genres
    df = df.sort_values("median_roi", ascending=False).head(top_n)

    # Extract values for plotting
    genres = df["genres"]
    roi = df["median_roi"]

    # Normalize ROI to [0, 1] range for color mapping
    # This scales color intensity proportionally to ROI values
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

    # Set titles and labels
    ax.set_title(
        "Median Return on Investment (ROI) by Genre",
        fontsize=16,
        fontweight="bold",
        pad=12
    )
    ax.set_xlabel("Median ROI", fontsize=12)
    ax.set_ylabel("Genre", fontsize=12)

    # Add grid and aesthetics
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)  # Grid behind bars

    # Invert y-axis so best genre appears on top (top-down reading order)
    # This improves readability of a ranked list
    ax.invert_yaxis()

    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    plt.show()


def main():
    """
    Main entry point for data visualization module.

    Can be used to test visualization functions during development.
    """
    pass


if __name__ == "__main__":
    main()