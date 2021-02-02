import pandas as pd
import psycopg2

from prefect import Flow, task
from oura import OuraClient
from oura.client_pandas import OuraClientDataFrame


oura = OuraClientDataFrame(personal_access_token='')
df = oura.combined_df_edited()
df_unpivoted = (
    df.reset_index()
        .melt(id_vars=['summary_date'], var_name=['header'])
        .assign(split = lambda x: (x['header'].str.split(':')))
        .assign(category = (lambda x: x['split'].str[0]))
        .assign(metric_name = lambda x: x['split'].str[1])
        .drop(columns = ['header', 'split'])
)


