import pandas as pd

from oura import OuraClient
from oura.client_pandas import OuraClientDataFrame

oura = OuraClientDataFrame(personal_access_token='')
df = oura.combined_df_edited()
df_unpivoted = (
    df.reset_index()
        .melt(id_vars=['summary_date'], var_name=['header'])
        .assign(category = lambda x: (x['header'].str.split(':')))
        #.assign(category=['header'].split(':')[0])
        #.assign(metric_name=['header'].split(':')[1])
)
#df_unpivoted = pd.melt(df, id_vars=['summary_date'], var_name=['header'])
print(df.head())
print(df_unpivoted.head())