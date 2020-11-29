import TogglPy as tp
import pandas as pd
import math
import time
import datetime
import psycopg2
import prefect
from prefect import Flow, task
from prefect.client import Secret
from sqlalchemy import create_engine
import pangres as pg

db_port = Secret("db_port")
db_user = Secret("db_user")
db_pass = Secret("db_pass")
db_host = Secret("db_host")
db_db = Secret("db_db")
toggl_api_key = Secret("toggl_api_key")
workspace_id = Secret("toggl_workspace_id")

# Authenticate
@task 
def authenticate_toggl():
    '''Authenicate with the toggl API using secrets stored in Prefect Cloud.'''
    toggl = tp.Toggl()
    toggl.setAPIKey(toggl_api_key.get())
    return toggl

# Extract
@task
def extract_toggl_report_api_to_df(auth):
    '''Using the Reporting API, extract all toggl time entries as a data frame.'''
    toggl_api_report_detailed = 'https://toggl.com/reports/api/v2/details'
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    data = {
        'workspace_id': workspace_id.get(),
        'since': '2020-08-01',
        'until': tomorrow
    }

    pages_index = 1
    pages = auth.request(toggl_api_report_detailed, parameters=data)
    try:
        pages_number = math.ceil(pages.get('total_count', 0) / pages.get('per_page', 0))
    except ZeroDivisionError:
        pages_number = 0
    for pages_index in range(2, pages_number +1):
        time.sleep(1)
        data['page'] = pages_index
        pages['data'].extend(auth.request(toggl_api_report_detailed, parameters=data).get('data', []))
    df = pd.DataFrame.from_dict(pages.get('data'))
    df = df.set_index('id')
    return df

# load
@task
def upsert_df_to_postgres(df, table_name='toggl_api'):
    '''Upsert a data frame to a table in Postgres DB'''
    connect = f"postgresql+psycopg2://%s:%s@%s:{db_port.get()}/%s" % (
        db_user.get(),
        db_pass.get(),
        db_host.get(),
        db_db.get()
    )

    engine = create_engine(connect)

    pg.upsert(
        engine=engine,
        df=df,
        table_name=table_name,
        schema='raw',
        if_row_exists='update'
    )

with Flow("toggl-el") as flow:
    auth = authenticate_toggl()
    extract = extract_toggl_report_api_to_df(auth)
    upsert = upsert_df_to_postgres(df=extract)

flow.register(project_name="Quantified Self")
flow.run_agent()
