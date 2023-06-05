import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output
from io import StringIO
import re

app = Dash(__name__)

trace_gantt = dcc.Graph(
    id='trace_gantt',
)

with open('trace.txt', 'r') as trace_file:
    trace_input = dcc.Textarea(
        id='cql-trace',
        value=trace_file.read(),
        style={'width': '100%', 'height': 300},
    )

trace_table = dash_table.DataTable(
    data=[{}]
)

app.layout = html.Div([
    trace_input,
    trace_gantt,
    trace_table
])


def build_scatter_fig(df):
    source_root_timestamps = {}
    trace_activities = {}
    sent_messages = []
    messages = []
    scatter_colors = {}
    for index, row in df.iterrows():
        # Build activities
        source = row['source']
        activity_timestamp = datetime.datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        if source not in source_root_timestamps:
            source_root_timestamps[source] = activity_timestamp
        if source not in trace_activities:
            trace_activities[source] = []

        activity_elapsed_timestamp = source_root_timestamps[source] + datetime.timedelta(microseconds=int(row['source_elapsed']))

        gantt_activity = {'activity': row['activity'], 'timestamp': activity_timestamp, 'start': activity_elapsed_timestamp, 'source': source}
        trace_activities[source].append(gantt_activity)

        # Collect messages being sent
        sending_search = re.search('Sending (.*) message to /(.*), size=(.*) bytes', row['activity'], re.IGNORECASE)
        if sending_search:
            message_source = row['source']
            message_type = sending_search.group(1)
            message_target = sending_search.group(2)
            message_size = sending_search.group(3)
            sent_messages.append({'source': message_source, 'target': message_target, 'type': message_type, 'size': message_size,
                                  'source_activity': row['activity'], 'source_start': activity_elapsed_timestamp})

        # Match received messages with sent messages
        receiving_search = re.search('(.*) message received from /(.*) ', row['activity'], re.IGNORECASE)
        if receiving_search:
            message_target = row['source']
            message_type = receiving_search.group(1)
            message_source = receiving_search.group(2)
            corresponding_sent_messages = [sent_message for sent_message in sent_messages if
                                           sent_message['source'] == message_source
                                           and sent_message['type'] == message_type
                                           and sent_message['target'] == message_target]
            if corresponding_sent_messages:
                corresponding_sent_message = corresponding_sent_messages[0]
                sent_messages.remove(corresponding_sent_message)
                messages.append({**corresponding_sent_message, 'target_activity': row['activity'], 'target_start': activity_elapsed_timestamp})

    flattened_activities = [item for sublist in list(trace_activities.values()) for item in sublist]
    fig_df = pd.DataFrame.from_records(flattened_activities)
    fig = px.scatter(data_frame=fig_df, x='start', y='activity', color='source')

    for scatter in fig["data"]:
        scatter_colors[scatter["name"]] = scatter["marker"]["color"]
    for message in messages:
        fig.add_annotation(
            x=message["target_start"], y=message["target_activity"],  # arrow head
            ax=message["source_start"], ay=message['source_activity'],  # arrows tial
            xref='x', yref='y', axref='x', ayref='y',
            text='', showarrow=True,  # only show the arrow
            arrowhead=2, arrowsize=1.5, arrowwidth=1,
            arrowcolor=scatter_colors[message["source"]]
        )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(transition_duration=500)
    return fig


@app.callback(
    Output(trace_table, 'data'),
    Output(trace_table, 'columns'),
    Output(trace_gantt, 'figure'),
    Output(trace_gantt, 'style'),
    Input(trace_input, 'value'),
)
def parse_trace(raw_trace):
    try:
        if raw_trace:
            df = pd.read_csv(StringIO(raw_trace), sep='\s*\|\s*', header=0, skiprows=[1], engine='python')

            table_data = df.to_dict('records')
            table_header = [{"name": i, "id": i} for i in df.columns]
            scatter_fig = build_scatter_fig(df)
            scatter_style = {'width': '100%', 'height': str(len(df) * 40) + 'px'}

            return table_data, table_header, scatter_fig, scatter_style
        else:
            return [], [], {}, {}
    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    app.run_server(debug=True)
