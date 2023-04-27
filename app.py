import pandas as pd
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_cytoscape
from io import StringIO

app = Dash(__name__)

with open('trace.txt', 'r') as trace_file:
    trace_input = dcc.Textarea(
        id='cql-trace',
        value=trace_file.read(),
        style={'width': '100%', 'height': 300},
    )

trace_table = dash_table.DataTable(data=[{}])

network = dash_cytoscape.Cytoscape(
    id='cytoscape',
    elements=[
        {'data': {'id': 'ca', 'label': 'Canada'}},
        {'data': {'id': 'on', 'label': 'Ontario'}},
        {'data': {'id': 'qc', 'label': 'Quebec'}},
        {'data': {'source': 'ca', 'target': 'on'}},
        {'data': {'source': 'ca', 'target': 'qc'}}
    ],
    layout={'name': 'breadthfirst'},
    style={'width': '400px', 'height': '500px'}
)

app.layout = html.Div([
    trace_input,
    trace_table,
    network
])

@app.callback(
    Output(trace_table, 'data'),
    Output(trace_table, 'columns'),
    Output(network, 'elements'),
    Input(trace_input, 'value')
)
def parse_trace(raw_trace):
    df = pd.read_csv(StringIO(raw_trace), sep='\s*\|\s*', header=0, skiprows=[1], engine='python')

    table_data = df.to_dict('records')
    table_header = [{"name": i, "id": i} for i in df.columns]

    network_nodes = list(map(lambda n: {'data': {'id': n, 'label': n}}, df['source'].unique()))
    return table_data, table_header, network_nodes

if __name__ == "__main__":
    app.run_server(debug=True)