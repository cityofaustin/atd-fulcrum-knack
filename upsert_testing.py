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

form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM_CRED.get("api_key")

# create postgrest instance
pgrest = Postgrest(
    "http://transportation-data.austintexas.io/signal_pms", auth=JOB_DB_API_TOKEN
)


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


record_old = []
index_passed_list = []
pgrest_records = get_pgrest_records()
duplicates = (pgrest_records[pgrest_records["fulcrum_id"].duplicated(keep=False) == True]).copy()
duplicates["index_temp"] = duplicates["fulcrum_id"]
duplicates = duplicates.set_index("index_temp")
duplicates_dict = duplicates.to_dict(orient="records")
duplicates_tuple = tuple(duplicates_dict)
for index, record in enumerate(duplicates_tuple):
    for index_c, record_c in enumerate(duplicates_tuple):
        if (record["fulcrum_id"] == record_c["fulcrum_id"]) and (index not in index_passed_list) and (index != index_c):
            if record["modified_date"] < record_c["modified_date"]:
                record_old.append(record)
            else:
                record_old.append(record_c)
            index_passed_list.append(index)
            index_passed_list.append(index_c)

record_old_df = pd.DataFrame.from_dict(record_old)
# print(record_newest_df)

merged = duplicates.merge(record_old_df, indicator=True, how='outer')
delete_payload_df = merged[merged['_merge'] == 'both']
delete_payload_df = delete_payload_df.drop(columns = '_merge')
delete_payload = delete_payload_df.to_dict(orient = "record")
