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

    # 3. Create Labels based on whether it's changeover or operation
    def create_hover_label(row):
        if row['OrderNo'] == 'CHANGEOVER':
            return f"CHANGEOVER - {row['ChangeoverMins']} minutes"
        return row['OrderNo'] + "-Op" + str(row['OpNo']) + ": " + row['OpName']
    
    df['HoverLabel'] = df.apply(create_hover_label, axis=1)
    
    # 'BarText': Short text for the box
    def get_bar_text(row):
        if row['OrderNo'] == 'CHANGEOVER':
            # Show changeover text if duration is reasonable
            if row['Duration'] >= pd.Timedelta(hours=0.5):
                return "⚙️"  # Gear icon for changeover
            return ""
        # Regular task - 2 hours threshold
        if row['Duration'] < pd.Timedelta(hours=2):
            return "" 
        return row['OrderNo']

    df['BarText'] = df.apply(get_bar_text, axis=1)

    # 4. Sort for cleaner chart (Resource > Time)
    df = df.sort_values(by=['ResourceName', 'StartTime'])

    # 5. Create Chart using px.timeline
    fig = px.timeline(
        df,
        x_start="StartTime",
        x_end="EndTime",
        y="ResourceName",
        color="OrderNo",
        color_discrete_sequence=px.colors.qualitative.Dark24,
        text="BarText",
        hover_name="HoverLabel",
        hover_data={
            "OrderNo": True, 
            "OpName": True, 
            "StartTime": True, 
            "EndTime": True, 
            "IsLate": True,
            "ChangeoverMins": True,
            "BarText": False,
            "HoverLabel": False,
            "Color": False
        },
        title="Production Schedule (Hover for Details)"
    )

    # 6. Update colors - make changeover blocks gray
    fig.for_each_trace(
        lambda trace: trace.update(marker_color='#808080', name='Changeover') 
        if trace.name == 'CHANGEOVER' 
        else trace
    )

    # 7. Visual Fixes
    fig.update_traces(
        textposition='inside',
        insidetextanchor='middle',
        textfont_size=10
    )
    
    fig.update_yaxes(autorange="reversed") 
    fig.update_layout(
        autosize=True, 
        height=400 + (len(df['ResourceName'].unique()) * 40),
        xaxis_title="Date/Time",
        yaxis_title="Resource",
        bargap=0.1,
        uniformtext_mode='hide',
        uniformtext_minsize=8
    )

    try:
        fig.write_html(GANTT_CHART_FILENAME)
        print(f"✅ Chart saved: {GANTT_CHART_FILENAME}")
        filepath = os.path.abspath(GANTT_CHART_FILENAME)
        webbrowser.open_new_tab(f'file://{filepath}')
    except Exception as e:
        print(f"Error opening chart: {e}")