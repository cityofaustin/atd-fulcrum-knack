# test: Query a form from fulcrum (Preventive Maintenance) and publish to postgresql
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

# test: Query a form from fulcrum (Preventive Maintenance)

def recur_dict(col_names, elements):
    """Summary
    
    Args:
        col_names (TYPE): Description
        elements (TYPE): Description
    
    Returns:
        TYPE: Description
    """


    # print(type(elements))

    for element in elements:

        if type(element) == dict:
            col_names[element.get("key")] = element.get("data_name")

            for key, value in element.items():
                if type(value) == list:
                    recur_dict(col_names, value)

    return col_names



def get_col_names(form_id):
    """Summary
    get a dictionary with {"key": "label"} pair for all columns in a data
    base
    
    
    
    Args:
        form_id (TYPE): Description
    
    
    
    Returns:
        TYPE: Description
    
    
    
    """

    form = fulcrum.forms.find(form_id)
    elements = form.get("form").get("elements")

    col_names = {}

    col_names = recur_dict(col_names, elements)

    return col_names

def get_records(form_id):
    """Summary
    
    Args:
        form_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    # initiate a dataframe

    records_dirty = fulcrum.records.search(url_params = {"form_id":form_id})

    records = pd.DataFrame()



    for record in records_dirty["records"]:
        form_values = record["form_values"]
        for key, value in form_values.items():
            print(value)
            if type(value) == dict and "choice_values" in value:
                value = value["choice_values"]
                
            if type(value) == list and len(value) == 1:
                form_values[key] = value[0]
        form_values["_server_updated_at"] = record["created_at"]
        form_values["_record_id"] = record["id"]
        new_row = pd.DataFrame([form_values], columns=form_values.keys())
    #     print(new_row)
        records = pd.concat([new_row, records], axis =0, sort=False).reset_index(drop=True)

    return records

def interpret_col_name(records):

    return records.rename(columns=col_names)


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
    # start a fulcrum instance
    fulcrum = fc.Fulcrum(key = key)
    forms = fulcrum.forms.search(url_params={'id': form_id})
    col_names = get_col_names(form_id)
    records = get_records(form_id)

    

    records = interpret_col_name(records)

    pdb.set_trace()

    records = clean_pm(records)


    print(list(records))
    print(records)

    pdb.set_trace()
