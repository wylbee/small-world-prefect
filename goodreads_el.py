from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd
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


@task
def scrape_goodreads_soup():
    page = "https://www.goodreads.com/review/list_rss/51384411?key=WtDvvUkHkxtfdlRlZRZ5v3AFe4OOq3SGxiQ0xn4NV5oDUDvJ&shelf=%23ALL%23"
    html = urlopen(page)
    soup = BeautifulSoup(html, "lxml")
    filtered_soup = soup.find_all("description")
    return filtered_soup


@task
def generate_df_from_goodreads_soup(filtered_soup=None):
    title_data = []
    detail_data = []
    # first row blank, 100 item feed
    for item in range(1, 101):
        row = filtered_soup[item]

        title_tag = row.find(re.compile("img"))
        title_text = title_tag.get("alt")
        title_data.append(title_text)

        row_data = row.text.strip()
        row_data = row_data.split("\n")
        row_data = [x.split(":", 1)[-1] for x in row_data]
        row_data = [x.strip() for x in row_data]
        detail_data.append(row_data)

    df_title = pd.DataFrame(columns=["title"], data=title_data)
    df_detail = pd.DataFrame(
        columns=[
            "author",
            "name",
            "average_rating",
            "book_published",
            "rating",
            "read_at",
            "date_added",
            "shelves",
            "review",
            "idk",
        ],
        data=detail_data,
    )
    df = pd.concat([df_title, df_detail], axis=1)
    # drop final column since it is computer junk
    df = df.iloc[:, :-1]
    return df


@task
def upsert_df_to_postgres(df, table_name="goodreads_api"):
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


with Flow("goodreads") as flow:
    scrape = scrape_goodreads_soup()
    generate_df = generate_df_from_goodreads_soup(filtered_soup=scrape)
    upsert = upsert_df_to_postgres(df=generate_df)

flow.register(project_name="Quantified Self")
flow.run_agent()
