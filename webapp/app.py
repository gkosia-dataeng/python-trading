from dash import Dash, dcc, html, Input, Output, callback, dash_table
import plotly.graph_objects as go
import time

class WebApp:
    db_conn=None

    @staticmethod
    def set_db_conn(db):
        WebApp.db_conn = db
    
    @staticmethod
    def initiate_app():

        app = Dash()
        app.layout = html.Div([
            html.H1("BTCUSDT volume analysis"),
            html.Div("Process agg trades from Binanace and visualize the volume"),
            html.Div(id='table-container'),
            dcc.Interval(
                id='interval-bytime-byprice',
                interval=1*1000, #milliseconds,
                n_intervals=0
            )
        ])

        return app

    # input: is the trigger
    # output: is the graph id that we will update, the property of the graph  
    @callback(
            Output('table-container', 'children'),
            Input('interval-bytime-byprice', 'n_intervals')
            )
    def update_graph_live(n_intervals):
        data_df = WebApp.db_conn.get_byprice_bytime_as_df()

        return dash_table.DataTable(
            data_df.to_dict('records'), [{"name": i, "id": i} for i in data_df.columns]
            )