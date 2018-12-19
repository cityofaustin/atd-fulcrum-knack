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


from config.secrets import *

# from tdutils.pgrestutil import Postgrest


form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM_CRED.get("api_key")

# create postgrest instance
pgrest = Postgrest(
    "http://transportation-data.austintexas.io/signal_pms",
    auth=JOB_DB_API_TOKEN,
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
    parser = argutil.get_parser("--replace" "--last_run_date")

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
                if type(value) == list and len(value) == 2:
                    if key == "fce3":
                        form_values[key] = value[1]

            if type(value) == list and len(value) == 1:
                form_values[key] = value[0]

        form_values["_server_updated_at"] = record["created_at"]
        form_values["_record_id"] = record["id"]
        new_row = pd.DataFrame([form_values], columns=form_values.keys())
        records = pd.concat([new_row, records], axis=0).reset_index(
            drop=True
        )

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
    results = pgrest.select(params = params)

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

    if not pgrest_records.empty:
        payloads = fulcrum_records[
            ~fulcrum_records["fulcrum_id"].isin(pgrest_records["fulcrum_id"])
        ]
    else:
        payloads = fulcrum_records

    payloads = payloads.to_dict(orient="records")

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

    return res


def main():
    """Summary
    
    Returns:
        TYPE: Description
    """
    fulcrum = fc.Fulcrum(key=key)
    forms = fulcrum.forms.search(url_params={"id": form_id})
    col_names = get_col_names(fulcrum, form_id)

    records = get_fulcrum_records(fulcrum, form_id)
    records = interpret_col_name(col_names, records)

    pgrest_records = get_pgrest_records()

    fulcrum_records = clean_pm(records)

    payloads = prepare_payload(fulcrum_records, pgrest_records)

    status = upsert_pgrest(payloads)
    status = len(status)
    return status

if __name__ == "__main__":
    # start a fulcrum instance
    print(main())
