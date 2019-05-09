"""Summary

Attributes:
    form_id (str): Description
    key (TYPE): Description
    pgrest (TYPE): Description
"""
# test: Query a form from fulcrum (Preventive Maintenance) and publish to postgresql
# import packages
import pandas as pd
import numpy as np
import knackpy as kp
import fulcrum as fc
import requests
import pdb
import json
from datetime import datetime, timedelta
from pypgrest import Postgrest
from tdutils import argutil


from config.secrets import *

# from tdutils.pgrestutil import Postgrest


form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM_CRED.get("api_key")

# create postgrest instance
pgrest = Postgrest(
    "http://transportation-data.austintexas.io/signal_pms", auth=JOB_DB_API_TOKEN
)


# test: Query a form from fulcrum (Preventive Maintenance)


def recur_dict(col_names, elements):
    """Summary
    
    Args:
        col_names (TYPE): Description
        elements (TYPE): Description
    
    Returns:
        TYPE: Description
    """

    for element in elements:

        if type(element) == dict:
            col_names[element.get("key")] = element.get("data_name")

            for key, value in element.items():
                if type(value) == list:
                    recur_dict(col_names, value)

    return col_names


def get_col_names(fulcrum, form_id):
    """Summary
    get a dictionary with {"key": "label"} pair for all columns in a data
    base
    
    
    
    Args:
        fulcrum (TYPE): Description
        form_id (TYPE): Description
    
    
    
    Returns:
        TYPE: Description
    
    
    
    """

    form = fulcrum.forms.find(form_id)
    elements = form.get("form").get("elements")

    col_names = {}

    col_names = recur_dict(col_names, elements)

    return col_names


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argutil.get_parser(
        "fulc_postgre.py"
        "move signal Preventive Maintenance work orders from fulcrum to postgresql database",
        "--last_run_date",
        "--replace"
    )

    args = parser.parse_args()

    return args


def get_last_run():
    """Summary
    
    Returns:
        TYPE: Description
    """

    last_run_date = args.last_run_date

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        # the arrow package needs a specific date and timeformat
        last_run_date = "1970-01-01"

    return last_run_date


def get_fulcrum_records(fulcrum, form_id):
    """Summary
    
    Args:
        fulcrum (TYPE): Description
        form_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """

    records_dirty = fulcrum.records.search(url_params={"form_id": form_id})

    records = pd.DataFrame()

    for record in records_dirty["records"]:
        form_values = record["form_values"]

        for key, value in form_values.items():

            if type(value) == dict and "choice_values" in value:
                value = value["choice_values"]
                if type(value) == list and len(value) >= 2:
                    if key == "fce3":
                        form_values[key] = value[1]

            if type(value) == list and len(value) == 1:
                form_values[key] = value[0]

        form_values["_server_updated_at"] = record["created_at"]
        form_values["_record_id"] = record["id"]
        new_row = pd.DataFrame([form_values], columns=form_values.keys())
        records = pd.concat([new_row, records], axis=0).reset_index(drop=True)

        # print(records)
    return records


def interpret_col_name(col_names, records):
    """Summary
    
    Args:
        col_names (TYPE): Description
        records (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return records.rename(columns=col_names)


def clean_pm(records):
    """Summary
    
    Args:
        records (TYPE): Description
    
    Returns:
        TYPE: Description
    """

    df = records.copy()

    df["signal_id"] = df["signal"].str.split("|").str[0].str.rstrip()

    df = df.rename(columns={"_record_id": "fulcrum_id"})

    df["_server_updated_at"] = df["_server_updated_at"].str.strip("CDT")
    df["pm_completed_date"] = pd.to_datetime(
        df["_server_updated_at"], format="%Y-%m-%d %H:%M:%S"
    )

    df["pm_completed_date"].apply(lambda x: datetime.strftime(x, "%Y-%m-%dT%H:%M:%S"))

    df["pm_completed_by"] = df["technicians"]  # .str.split(",").str[1]

    # pdb.set_trace()

    df["modified_date"] = datetime.now().isoformat(timespec="seconds")

    df["pm_completed_date"] = df["pm_completed_date"].astype(str)
    df["modified_date"] = df["modified_date"].astype(str)

    cleaned_record = df[
        [
            "signal_id",
            "fulcrum_id",
            "pm_completed_date",
            "modified_date",
            "pm_completed_by",
        ]
    ]

    return cleaned_record


def get_pgrest_records():
    """Summary
    
    Returns:
        TYPE: Description
    """
    # the datetime converstin for modified_date is not right. The time part are missing

    params = {}
    results = pgrest.select(params=params)

    if len(results) != 0:
        results = pd.DataFrame(results)
        results["modified_date"] = pd.to_datetime(
            results["modified_date"], format="%Y-%m-%dT%H:%M:%S"
        ).apply(lambda x: datetime.strftime(x, "%Y-%m-%dT%H:%M:%S"))

        results["pm_completed_date"] = pd.to_datetime(
            results["pm_completed_date"]
        ).apply(lambda x: datetime.strftime(x, "%Y-%m-%dT%H:%M:%S"))

    else:
        return results

    return results


def prepare_payload(fulcrum_records, pgrest_records):
    """Summary
    
    #TODO incremental update with the more recent run date
    
    Args:
        fulcrum_records (TYPE): Description
        pgrest_records (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    # compare the modified date and fulcrum id in fulcrum records and
    # in postgrest record.
    # print(fulcrum_records)

    if not pgrest_records.empty:
        payloads = fulcrum_records[
            ~fulcrum_records["fulcrum_id"].isin(pgrest_records["fulcrum_id"])
        ]
    else:
        payloads = fulcrum_records

    payloads = payloads.to_dict(orient="records")
    # print(payloads)

    return payloads


def upsert_pgrest(payloads):
    """Summary
    
    Args:
        payloads (TYPE): Description
    
    Returns:
        TYPE: Description
    
    Deleted Parameters:
        payload (TYPE): Description
    """

    res = pgrest.upsert(payloads)

    # a safety that delete older records and add new records
    pgrest_records = get_pgrest_records
    


    return res


def prepare_replace_payload(fulcrum_records, pgrest_records):

    # create a replace method that refresh records in the postgreSQL database
    # previous replace method does not correctlly executed 
    # pdb.set_trace()

    replace_payload = []

    fulcrum_records = fulcrum_records.to_dict(orient="records")
    # make fulcrum ID as the key to the pgrest_records
    pgrest_records["index_temp"] = pgrest_records["fulcrum_id"]
    pgrest_records = pgrest_records.set_index("index_temp")
    pgrest_records = pgrest_records.to_dict(orient="index")
    

    for fulcrum_record in fulcrum_records:
        # loop all fulcrum record
        fulcrum_id = fulcrum_record["fulcrum_id"] # take the fulcrum id as the key
        if fulcrum_id in pgrest_records:
        # update the record in pgrest database if the fulcrum id is alread a key in pgrest records
            for key, value in fulcrum_record.items():
                # compare the fulcrum records and pgrestSQL records
                if  pgrest_records[fulcrum_id][key] != fulcrum_record[key]:
                    if key != "pm_completed_date" and key!="modified_date":
                        pgrest_records[fulcrum_id][key] = fulcrum_record[key]
                        # print(pgrest_records[fulcrum_id][key])
                        # print(fulcrum_record[key])
                        fulcrum_record["pm_completed_by"] = min(fulcrum_record["pm_completed_by"], pgrest_records[fulcrum_id]["pm_completed_by"])
                        replace_payload.append(fulcrum_record)
                        break
        else:
            # print(fulcrum_record)
            replace_payload.append(fulcrum_record)




    # payloads = payloads.to_dict(orient="records")

    # for record in fulcrum_records:
    #     fulcrum_compare_dict[record["fulcrum_id"]] = record

    # for record in pgrest_records.items():
    #     for key, info in record.items():
    #         if info != fulcrum_compare_dict[record["fulcrum_id"]][key]:
    #             replace_payload.append(record)

    return replace_payload


def main():
    """Summary
    
    Returns:
        TYPE: Description
    """
    args = cli_args()

    fulcrum = fc.Fulcrum(key=key)
    forms = fulcrum.forms.search(url_params={"id": form_id})
    col_names = get_col_names(fulcrum, form_id)

    records = get_fulcrum_records(fulcrum, form_id)
    records = interpret_col_name(col_names, records)

    pgrest_records = get_pgrest_records()

    fulcrum_records = clean_pm(records)

    payloads = prepare_payload(fulcrum_records, pgrest_records)

    if args.replace:
        payloads = prepare_replace_payload(fulcrum_records, pgrest_records)
        
    else:
        payloads = prepare_payload(fulcrum_records, pgrest_records)

    status = upsert_pgrest(payloads)
    status = len(status)

    return status


if __name__ == "__main__":
    # start a fulcrum instance
    fulcrum = fc.Fulcrum(key=key)
    forms = fulcrum.forms.search(url_params={"id": form_id})
    col_names = get_col_names(fulcrum, form_id)

    records = get_fulcrum_records(fulcrum, form_id)
    records = interpret_col_name(col_names, records)

    pgrest_records = get_pgrest_records()

    fulcrum_records = clean_pm(records)

    # payloads = prepare_payload(fulcrum_records, pgrest_records)
    replace_payload = prepare_replace_payload(fulcrum_records, pgrest_records)
    # status = main()
    # print(replace_payload)
