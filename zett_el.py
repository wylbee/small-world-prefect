import markdown
import re
import pandas as pd
from google.cloud import storage
from io import StringIO
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
storage_client = storage.Client()


@task
def extract_zett_data_from_directory(bucket="qs-zettelkasten", directory="atoms/2"):
    """Assuming a particular note set up,
    return a dataframe of ids/titles/statuses
    from Zettelkasten."""

    atom_ids = []
    titles = []
    statuses = []

    bucket = storage_client.get_bucket(bucket)
    file_list = bucket.list_blobs(prefix=directory)

    for file_name in file_list:
        f = file_name.download_as_string()
        f = f.decode("ISO-8859-1")
        f = StringIO(f)

        text = markdown.markdown(f.read())
        status = re.findall("status: (.*)", text)
        title = re.findall("title: (.*)", text)
        atom_id = file_name.name

        atom_ids.append(atom_id[6:-3])
        statuses.append(status[0])
        titles.append(title[0])

    results_dict = {"atom_id": atom_ids, "title": titles, "status": statuses}

    df = pd.DataFrame.from_dict(results_dict)

    return df


@task
def upsert_df_to_postgres(df, table_name="zettelkasten"):
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


with Flow("Zettelkasten") as flow:
    extract = extract_zett_data_from_directory()
    upsert = upsert_df_to_postgres(df=extract)

flow.register(project_name="Quantified Self")
flow.run_agent()
