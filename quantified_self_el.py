import pandas as pd
import os
import psycopg2
import prefect
from prefect import Flow, task
from prefect.client import Secret
from sqlalchemy import create_engine

@task
def extract(extract_name):
    extract_path = f'{extract_name}.csv'
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

flow.run()
