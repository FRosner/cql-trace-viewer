import pandas as pd
import plotly.express as px
import datetime
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_cytoscape
from io import StringIO
import re

app = Dash(__name__)

trace_gantt = dcc.Graph(id='trace_gantt')

with open('trace.txt', 'r') as trace_file:
    trace_input = dcc.Textarea(
        id='cql-trace',
        value=trace_file.read(),
        style={'width': '100%', 'height': 300},
    )

trace_table = dash_table.DataTable(
    data=[{}]
)

network = dash_cytoscape.Cytoscape(
    id='cytoscape',
    elements=[],
    layout={'name': 'breadthfirst'},
    style={'width': '100%', 'height': '400px'},
    stylesheet=[
        {
            'selector': 'node',
            'style': {
                'label': 'data(id)'
            }
        },
        {
            'selector': 'edge',
            'style': {
                # The default curve style does not work with certain arrows
                'curve-style': 'bezier'
            }
        },
        {
            'selector': '.message_sent',
            'style': {
                'target-arrow-shape': 'triangle',
                'label': 'data(type)',
                'width': 'data(width)'
            }
        }
    ],
    userZoomingEnabled=False,
    userPanningEnabled=False
)

app.layout = html.Div([
    trace_input,
    trace_table,
    trace_gantt,
    network
])

def scale_arrow_width(max_size, size):
    return str(size / max_size * 10 + 1) + "px"

def build_network_data(df, active_cell):
    network_df = df
    network_nodes = list(map(lambda n: {'data': {'id': n, 'label': n}}, network_df['source'].unique()))
    network_edges = []
    max_message_size = 0
    for index, row in network_df.iterrows():
        sending_search = re.search('Sending (.*) message to /(.*), size=(.*) bytes', row['activity'], re.IGNORECASE)

        if sending_search:
            message_source = row['source']
            message_type = sending_search.group(1)
            message_target = sending_search.group(2)
            message_size = sending_search.group(3)
            max_message_size = max(int(message_size), max_message_size)
            network_edges.append({'data': {'source': message_source, 'target': message_target, 'type': message_type, 'size': message_size, 'index': index}, 'classes': 'message_sent'})

    for edge in network_edges:
        edge['data'].update({'width': scale_arrow_width(max_message_size, int(edge['data']['size']))})

    network_edges = list(filter(lambda e: (not active_cell) or e['data']['index'] <= active_cell['row'], network_edges))

    return network_nodes + network_edges

# TODO add source to label
# TODO deal with final activity per source
def build_gantt_fig(df, active_cell):
    source_root_timestamps = {}
    gantt_activities = {}
    for index, row in df.iterrows():
        activity_timestamp = datetime.datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        activity_elapsed_timestamp = activity_timestamp + datetime.timedelta(microseconds=int(row['source_elapsed']))
        if row['source'] not in source_root_timestamps:
            source_root_timestamps[row['source']] = activity_timestamp
        if row['source'] not in gantt_activities:
            gantt_activities[row['source']] = []
        last_activity = gantt_activities[row['source']][-1:]
        if last_activity:
            last_activity[0].update({'end': activity_elapsed_timestamp})
        gantt_activity = {'activity': row['activity'], 'start': activity_elapsed_timestamp, 'source': row['source']}
        gantt_activities[row['source']].append(gantt_activity)

    flattened_activities = [item for sublist in list(gantt_activities.values()) for item in sublist]
    fig_df = pd.DataFrame.from_dict(flattened_activities)
    gantt_fig = px.timeline(data_frame=fig_df, x_start="start", x_end="end", y="activity")
    gantt_fig.update_yaxes(autorange="reversed")
    gantt_fig.update_layout(transition_duration=500)
    return gantt_fig

@app.callback(
    Output(trace_table, 'data'),
    Output(trace_table, 'columns'),
    Output(network, 'elements'),
    Output(trace_gantt, 'figure'),
    Input(trace_input, 'value'),
    Input(trace_table, 'active_cell')
)
def parse_trace(raw_trace, active_cell):
    try:
        df = pd.read_csv(StringIO(raw_trace), sep='\s*\|\s*', header=0, skiprows=[1], engine='python')

        table_data = df.to_dict('records')
        table_header = [{"name": i, "id": i} for i in df.columns]

        network_data = build_network_data(df, active_cell)

        gantt_fig = build_gantt_fig(df, active_cell)

        return table_data, table_header, network_data, gantt_fig
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    app.run_server(debug=True)