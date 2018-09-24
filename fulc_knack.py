# import packages
import pandas as pd
import numpy as np
import knackpy as kp
import fulcrum as fc
import requests
import pdb
from datetime import datetime, timedelta
# import credentials
from config.secrets import *

form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM_CRED.get("api_key")

print(key)

# test: Query a form from fulcrum (Preventive Maintenance)

# start a fulcrum instance
fulcrum = fc.Fulcrum(key = key)

# print(fulcrum)
forms = fulcrum.forms.search(url_params={'id': form_id})
# print(forms)

def get_elements(form_id):
    """Summary
    get a dictionary with {"key": "label"} pair for all columns in a data
    base
    
    
    
    Args:
        form_id (TYPE): Description
    
    
    
    """

    form = fulcrum.forms.find(form_id)
    elements = form.get("form").get("elements")

    columns = {}

    for element in elements:
    	# column = {element.get("key"): element.get("label")}
    	# columns.append(column)
        columns[element.get("key")] = element.get("label")

    return columns

def get_records(form_id):
    """Summary
    
    Args:
        form_id (TYPE): Description
    """
    records_dirty = fulcrum.records.search(url_params = {"form_id":form_id})

    records_dirty.get("records")

    records = pd.DataFrame.from_dict(records_dirty.get("records"))
    


    return records

def clean_pm(records):
    df = records.copy()

    df["SIGNAL_ID"] = df["signal"].str.split("|").str[0]

    # rename record_id to fulcrum ID
    df = df.rename(columns={'_record_id': 'FULCRUM_ID'})

    df["_server_updated_at"] = df["_server_updated_at"].str.strip("CDT")
    df["PM_COMPLETED_DATETIME"] = pd.to_datetime(df["_server_updated_at"], format = "%Y-%m-%d %H:%M:%S")
    df["PM_COMPLETED_DATE"] = df["PM_COMPLETED_DATETIME"].dt.date

    df["PM_COMPLETED_BY"] = df["technicians"].str.split(",").str[1]
    df["modified_current"] = datetime.now().replace(microsecond=0).isoformat(" ")

    df["modified_current"] =pd.to_datetime(df["modified_current"], format = "%Y-%m-%d %H:%M:%S")

    df["MODIFIED_DATE"] = df["modified_current"] + timedelta(minutes = 20)

    df = df[["SIGNAL_ID", "FULCRUM_ID", "PM_COMPLETED_DATE", "MODIFIED_DATE", "PM_COMPLETED_BY"]]

    return df


def main():
    pass





if __name__ == "__main__":

    # columns = get_elements(form_id)
    # print(columns)
    records = get_records(form_id)

    # df = clean_pm(records)
    print(list(records))


# form = fulcrum.forms.find(form_id)



# print(form)

# url_params={'form_id': '44359e32-1a7f-41bd-b53e-3ebc039bd21a'} ## PM
# records = fulcrum.records.search(url_params={'form_id': 'a1cb3ac7-146f-491a-a4a2-47737fb12074'})

# signal_pm_raw = fulcrum.records.find("44359e32-1a7f-41bd-b53e-3ebc039bd21a")