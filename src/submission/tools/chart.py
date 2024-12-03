import boto3.session
from langchain_core.tools import tool
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Literal, Optional, Dict, Any, Union
import boto3
import os
import io
from datetime import datetime

@tool('generate_chart')
def generate_chart(
    chart_type: Literal['scatter', 'line', 'bar', 'box', 'violin', 'heatmap', 'pie', 'histogram', 'kde'],
    data_json: str,
    filename: str,
    x_axis: str,
    y_axis: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a sophisticated chart based on input data and save it as an image file.

    Parameters:
    -----------
    chart_type : str
        The type of chart to generate. Supported types:
        'scatter', 'line', 'bar', 'box', 'violin', 'heatmap', 'pie', 'histogram', 'kde'
    data_json : str
        JSON string containing the data to be plotted.
    filename : str
        Name of the output file.
    x_axis : str
        Column name for x-axis.
    y_axis : str, optional
        Column name for y-axis (not required for some chart types).
    config : dict, optional
        Additional configuration options:
        - title: str - Chart title
        - figsize: tuple - Figure size (width, height)
        - palette: str - Color palette name
        - hue: str - Column name for color grouping
        - style: str - Column name for style grouping
        - orientation: str - 'v' for vertical, 'h' for horizontal
        - bins: int - Number of bins for histogram
        - aggregate: str - Aggregation function ('mean', 'sum', etc.)
        - error_bars: bool - Show error bars
        - legend: bool - Show legend
        - grid: bool - Show grid
        - rotate_labels: int - Degree to rotate x-axis labels
        - font_size: int - Base font size
        - theme: str - Plot theme

    Returns:
    --------
    str
        S3 URL of the generated chart.
    """
    # Set default configuration
    default_config = {
        'title': None,
        'figsize': (10, 6),
        'palette': 'deep',
        'hue': None,
        'style': None,
        'orientation': 'v',
        'bins': 30,
        'aggregate': 'mean',
        'error_bars': False,
        'legend': True,
        'grid': True,
        'rotate_labels': 0,
        'font_size': 10,
        'theme': 'darkgrid'
    }

    # Update default config with provided config
    if config is None:
        config = {}
    config = {**default_config, **config}

    # Convert JSON input to DataFrame
    data = json.loads(data_json)
    df = pd.DataFrame(data)

    # Set the visual style
    sns.set_theme(style=config['theme'])
    plt.rcParams['font.size'] = config['font_size']

    # Create figure
    fig, ax = plt.subplots(figsize=config['figsize'])

    # Generate the appropriate plot based on chart type
    if chart_type == "scatter":
        sns.scatterplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=config['hue'],
            style=config['style'],
            palette=config['palette'],
            ax=ax
        )

    elif chart_type == "line":
        sns.lineplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=config['hue'],
            style=config['style'],
            palette=config['palette'],
            errorbar='sd' if config['error_bars'] else None,
            ax=ax
        )

    elif chart_type == "bar":
        sns.barplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=config['hue'],
            palette=config['palette'],
            errorbar='sd' if config['error_bars'] else None,
            estimator=config['aggregate'],
            ax=ax
        )

    elif chart_type == "box":
        sns.boxplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=config['hue'],
            palette=config['palette'],
            ax=ax
        )

    elif chart_type == "violin":
        sns.violinplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=config['hue'],
            palette=config['palette'],
            ax=ax
        )

    elif chart_type == "heatmap":
        pivot_table = pd.pivot_table(
            df,
            values=y_axis,
            index=x_axis,
            columns=config['hue'] if config['hue'] else None,
            aggfunc=config['aggregate']
        )
        sns.heatmap(
            pivot_table,
            cmap=config['palette'],
            annot=True,
            fmt='.2f',
            ax=ax
        )

    elif chart_type == "pie":
        plt.pie(
            df[y_axis],
            labels=df[x_axis],
            autopct='%1.1f%%',
            colors=sns.color_palette(config['palette'])
        )

    elif chart_type == "histogram":
        sns.histplot(
            data=df,
            x=x_axis,
            hue=config['hue'],
            bins=config['bins'],
            palette=config['palette'],
            ax=ax
        )

    elif chart_type == "kde":
        sns.kdeplot(
            data=df,
            x=x_axis,
            hue=config['hue'],
            palette=config['palette'],
            ax=ax
        )

    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    # Customize the plot
    if config['title']:
        plt.title(config['title'])
    else:
        plt.title(f"{chart_type.capitalize()} Chart: {y_axis if y_axis else ''} vs {x_axis}")

    plt.xlabel(x_axis)
    if y_axis:
        plt.ylabel(y_axis)

    # Rotate x-axis labels if specified
    if config['rotate_labels']:
        plt.xticks(rotation=config['rotate_labels'])

    # Show/hide grid
    plt.grid(config['grid'])

    # Show/hide legend
    if not config['legend']:
        plt.legend([]).remove()

    # Adjust layout
    plt.tight_layout()

    # Save the plot to a BytesIO object
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png', dpi=300, bbox_inches='tight')
    plt.close()

    # Reset the pointer of the BytesIO object
    img_data.seek(0)

    # Upload to S3
    session = boto3.Session()
    s3 = session.client('s3')
    bucket_name = 'gdsc-bucket-891377155936'


    try:
        s3.upload_fileobj(img_data, bucket_name, filename)
        s3_url = f'https://{bucket_name}.s3.amazonaws.com/{filename}'
        return s3_url
    except Exception as e:
        return f"An error occurred: {str(e)}"