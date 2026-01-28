import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def trendVisuals(data: pd.DataFrame, column1: str, column2: str, title: str):
    # Prepare data
    temp = data[[column1, column2]].dropna()
    x = temp[column1]
    y = temp[column2]

    # Correlation
    corr = x.corr(y)

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

    # Means
    x_mean = x.mean()
    y_mean = y.mean()

    # Plot
    plt.figure(figsize=(10, 7))
    plt.scatter(
        x, y,
        alpha=0.75,
        color="#4c72b0",
        edgecolor="white",
        linewidth=0.6
    )

    # Mean reference lines
    plt.axvline(x_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column1}")
    plt.axhline(y_mean, linestyle="--", linewidth=1.5, color="#555555", label=f"Mean {column2}")

    # Highlight highest Y value
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

    # Axis limits (needed for quadrant positioning)
    x_min, x_max = plt.xlim()
    y_min, y_max = plt.ylim()

    # Quadrant centers (data-aware positioning)
    qx_left = (x_min + x_mean) / 2
    qx_right = (x_mean + x_max) / 2
    qy_bottom = (y_min + y_mean) / 2
    qy_top = (y_mean + y_max) / 2

    quadrant_style = dict(
        fontsize=9,
        ha="center",
        va="center",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#bbbbbb", alpha=0.85)
    )

    # Quadrant labels
    plt.text(qx_right, qy_top, f"High {column1} – High {column2}\n(Strong Performers)", **quadrant_style)
    plt.text(qx_left, qy_top, f"Low {column1} – High {column2}\n(Efficient / Overperformers)", **quadrant_style)
    plt.text(qx_right, qy_bottom, f"High {column1} – Low {column2}\n(Underperformers)", **quadrant_style)
    plt.text(qx_left, qy_bottom, f"Low {column1} – Low {column2}\n(Weak Performers)", **quadrant_style)

    # Correlation summary
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

    # Labels & styling
    plt.xlabel(column1.upper())
    plt.ylabel(column2.upper())
    plt.title(title, fontsize=14, weight="bold")

    plt.legend(loc="lower right", frameon=False)
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.tight_layout()
    plt.show()



def plot_yearly_box_office_trends(data: pd.DataFrame):
    data = data.copy()
    data["release_year"] = pd.to_datetime(data["release_date"]).dt.year
    yearly = data.groupby("release_year")["revenue_musd"].mean()

    plt.figure(figsize=(11, 5.5))

    # Blue trend line
    plt.plot(
        yearly.index,
        yearly.values,
        marker="o",
        linestyle="-",
        linewidth=2.5,
        color="#1f4fd8",              # Deep blue
        markerfacecolor="white",
        markeredgecolor="#1f4fd8",
        markeredgewidth=1.8,
        label="Yearly Average Revenue"
    )

    # Identify peak & lowest years
    peak_year = yearly.idxmax()
    peak_value = yearly.max()

    low_year = yearly.idxmin()
    low_value = yearly.min()

    # Peak annotation (green arrow)
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
        zorder=5
    )

    # Lowest annotation (red arrow)
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
        zorder=5
    )

    # Titles & labels
    plt.title("Yearly Box Office Performance", fontsize=14, weight="bold")
    plt.xlabel("Year", fontsize=11)
    plt.ylabel("Average Revenue (Million USD)", fontsize=11)

    # Clean grid
    plt.grid(True, linestyle="--", alpha=0.35)

    # Legend
    plt.legend(frameon=False)

    plt.tight_layout()
    plt.show()



def plot_franchise_vs_standalone_metrics(data: pd.DataFrame):
    import numpy as np
    import matplotlib.pyplot as plt

    metrics = {
        "Revenue (M USD)": ("revenue_musd", "Mean Revenue"),
        "Budget (M USD)": ("budget_musd", "Mean Budget"),
        "Average Rating": ("vote_average", "Mean Rating"),
        "ROI": ("ROI", "Mean ROI")
    }

    # Ensure correct ordering
    data = data.sort_values("movie_type")

    movie_types = data["movie_type"].values

    colors = {
        "Franchise": "#1f4fd8",    # professional blue
        "Standalone": "#6b7280"   # neutral charcoal
    }

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    axes = axes.flatten()

    for ax, (title, (col, ylabel)) in zip(axes, metrics.items()):
        values = data[col].values
        x = np.arange(len(values))

        bars = ax.bar(
            x,
            values,
            width=0.55,
            color=[colors[m] for m in movie_types]
        )

        # Value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height * 1.02,
                f"{height:.1f}",
                ha="center",
                va="bottom",
                fontsize=10,
                color="#111827"
            )

        ax.set_title(title, fontsize=12, weight="bold", pad=10)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(movie_types, fontsize=10)

        ax.grid(axis="y", linestyle="--", alpha=0.3)
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle(
        "Franchise vs Standalone Movies — Metric-wise Comparison",
        fontsize=15,
        weight="bold"
    )

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.show()



def plot_roi_by_genre(
    data,
    top_n=15,
    figsize=(12, 7)
):
    """
    Visualize Median ROI by Genre.

    Parameters
    ----------
    data : pd.DataFrame
        Expected columns: ['genre', 'median_roi']
    top_n : int
        Number of top genres to display (by median ROI)
    figsize : tuple
        Figure size
    """

    # Defensive copy
    df = data.copy()

    # Drop nulls just in case
    df = df.dropna(subset=["genres", "median_roi"])

    # Sort and limit
    df = df.sort_values("median_roi", ascending=False).head(top_n)

    # Prepare values
    genres = df["genres"]
    roi = df["median_roi"]

    # Normalize ROI for color intensity
    norm = (roi - roi.min()) / (roi.max() - roi.min() + 1e-6)
    colors = plt.cm.viridis(norm)

    # Plot
    fig, ax = plt.subplots(figsize=figsize)

    bars = ax.barh(
        genres,
        roi,
        color=colors,
        edgecolor="black",
        linewidth=0.6
    )

    # Labels on bars
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width,
            bar.get_y() + bar.get_height() / 2,
            f"{width:.2f}",
            va="center",
            ha="left",
            fontsize=10
        )

    # Titles & labels
    ax.set_title(
        "Median Return on Investment (ROI) by Genre",
        fontsize=16,
        fontweight="bold",
        pad=12
    )
    ax.set_xlabel("Median ROI", fontsize=12)
    ax.set_ylabel("Genre", fontsize=12)

    # Grid & aesthetics
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    # Invert so best genre appears on top
    ax.invert_yaxis()

    # Tight layout
    plt.tight_layout()
    plt.show()




def main():
    ...



if __name__ == "__main__":
    main()