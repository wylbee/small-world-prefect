import pandas as pd
import psycopg2
import prefect

from prefect import Flow, task
from prefect.client import Secret
from sqlalchemy import create_engine
import pangres as pg
from oura.client_pandas import OuraClientDataFrame

oura_pk = Secret("oura_pk")
db_port = Secret("db_port")
db_user = Secret("db_user")
db_pass = Secret("db_pass")
db_host = Secret("db_host")
db_db = Secret("db_db")


@task
def generate_df_from_oura_api():
    """
    Given a personal access key, pull a user's data from the oura
    api and unpivot it. Returns an unpivoted dataframe
    """
    oura = OuraClientDataFrame(personal_access_token=oura_pk.get())
    df = oura.combined_df_edited()
    df_unpivoted = (
        df.reset_index()
        .melt(id_vars=["summary_date"], var_name=["header"])
        .assign(split=lambda x: (x["header"].str.split(":")))
        .assign(category=(lambda x: x["split"].str[0]))
        .assign(metric_name=lambda x: x["split"].str[1])
        .drop(columns=["header", "split"])
        .set_index(['summary_date', 'category', 'metric_name'])
    )
    return df_unpivoted


@task
def upsert_df_to_postgres(df, table_name="oura_api"):
    """Upsert a data frame to a table in Postgres DB"""
    connect = f"postgresql+psycopg2://%s:%s@%s:{db_port.get()}/%s" % (
        db_user.get(),
        db_pass.get(),
        db_host.get(),
        db_db.get(),
    )

    engine = create_engine(connect)

    pg.upsert(
        engine=engine,
        df=df,
        table_name=table_name,
        schema="raw",
        if_row_exists="update",
    )


with Flow("oura-el") as flow:
    df = generate_df_from_oura_api()
    upsert = upsert_df_to_postgres(df=df)

flow.register(project_name="Quantified Self")