import plotly.express as px
import pandas as pd
import webbrowser
import os
from typing import List, Dict, Any

GANTT_CHART_FILENAME = 'production_schedule.html'

def create_gantt_chart(scheduled_list: List[Dict[str, Any]]):
    if not scheduled_list:
        print("Visualization skipped: No orders.")
        return

    print(f"\nGenerating Gantt chart for {len(scheduled_list)} operations...")

    df = pd.DataFrame(scheduled_list)
    
    # 1. Convert strings to datetimes
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['EndTime'] = pd.to_datetime(df['EndTime'])

    # 2. Calculate Duration to hide text on tiny bars
    df['Duration'] = df['EndTime'] - df['StartTime']

    # 3. Create Labels
    # 'HoverLabel': Full details for the tooltip
    df['HoverLabel'] = df['OrderNo'] + "-Op" + df['OpNo'].astype(str) + ": " + df['OpName']
    
    # 'BarText': Short text for the box (Order No only)
    # Logic: If duration < 2 hours, leave empty. Otherwise, show OrderNo.
    def get_bar_text(row):
        # 2 hours threshold (approx 0.08 days)
        if row['Duration'] < pd.Timedelta(hours=2):
            return "" 
        return row['OrderNo']

    df['BarText'] = df.apply(get_bar_text, axis=1)

    # 4. Sort for cleaner chart (Resource > Time)
    df = df.sort_values(by=['ResourceName', 'StartTime'])

    # 5. Create Chart
    fig = px.timeline(
        df,
        x_start="StartTime",
        x_end="EndTime",
        y="ResourceName",
        color="OrderNo", 
        
        # Use a distinct color palette
        color_discrete_sequence=px.colors.qualitative.Dark24,
        
        # Use the SHORT text for the bar
        text="BarText",
        
        # Custom Hover Data
        hover_name="HoverLabel",
        hover_data={
            "OrderNo": True, 
            "OpName": True, 
            "StartTime": True, 
            "EndTime": True, 
            "IsLate": True,
            "BarText": False,   # Don't show this in hover
            "HoverLabel": False # Don't show this in data list (it's the title)
        },
        title="Production Schedule (Hover for Details)"
    )

    # 6. Visual Fixes
    fig.update_traces(
        textposition='inside',      # Force text to stay inside the box
        insidetextanchor='middle',  # Center the text
        textfont_size=10            # Make font slightly smaller to fit better
    )

    fig.update_yaxes(autorange="reversed") 
    fig.update_layout(
        autosize=True, 
        height=400 + (len(df['ResourceName'].unique()) * 40),
        xaxis_title="Date/Time",
        yaxis_title="Resource",
        bargap=0.1, # Slight gap between rows
        uniformtext_mode='hide', # Hides text if it still doesn't fit
        uniformtext_minsize=8
    )

    try:
        fig.write_html(GANTT_CHART_FILENAME)
        print(f"✅ Chart saved: {GANTT_CHART_FILENAME}")
        filepath = os.path.abspath(GANTT_CHART_FILENAME)
        webbrowser.open_new_tab(f'file://{filepath}')
    except Exception as e:
        print(f"Error opening chart: {e}")