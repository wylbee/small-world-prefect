import pandas as pd
import os
import psycopg2
import prefect
from prefect import Flow, task
from prefect.client import Secret
from sqlalchemy import create_engine
from google.cloud import storage

db_port = Secret("db_port")
db_user = Secret("db_user")
db_pass = Secret("db_pass")
db_host = Secret("db_host")
db_db = Secret("db_db")

storage_client = storage.Client()

@task
def extract(extract_name):
    extract_path = f'gs://small-world-analytics-filestore/quantified-self/{extract_name}.csv'
    df = pd.read_csv(extract_path)
    return df

@task
def transform(df):
    df.columns = df.columns.str.strip('() ')
    return df

@task
def load(df, extract_name):
    
    connect = f"postgresql+psycopg2://%s:%s@%s:{db_port.get()}/%s" % (
        db_user.get(),
        db_pass.get(),
        db_host.get(),
        db_db.get()
    )

    engine = create_engine(connect)
    df.to_sql(
        f'{extract_name}', 
        con=engine, 
        index=False,
        schema='raw',
        if_exists='replace'
    )

with Flow("ETL") as flow:
    extract_name = prefect.Parameter('extract_name', default='goodreads')
    e = extract(extract_name)
    t = transform(e)
    l = load(t, extract_name)

flow.register(project_name="Quantified Self")
flow.run_agent()
