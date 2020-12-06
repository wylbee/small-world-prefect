import markdown
import re
import os
import glob
import pandas as pd

#file_list = glob.glob("/home/brown5628/projects/small-world-prefect/data/*.md")

def extract_zett_data_from_directory(directory=None):
    atom_ids=[]
    titles=[]
    statuses=[]

    file_list = glob.glob(f"{directory}*.md")

    for file_name in file_list:
        f = open(file_name)
        text = markdown.markdown(f.read())
        status = re.findall("status: (.*)", text)
        title = re.findall("title: (.*)", text)
        atom_id = (os.path.basename(f.name))

        atom_ids.append(atom_id[:-3])
        statuses.append(status[0])
        titles.append(title[0])

    results_dict = {
        'atom_id': atom_ids,
        'title': titles,
        'status': statuses
    }

    df = pd.DataFrame.from_dict(results_dict)

    return df

print(extract_zett_data_from_directory("/home/brown5628/projects/small-world-prefect/data/"))
