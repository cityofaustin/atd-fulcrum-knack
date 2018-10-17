import pandas as pd
import numpy as np
import knackpy as kp
import fulcrum as fc
import requests
import pdb
import json
from datetime import datetime, timedelta
# import requests

# import credentials
from config.secrets import *

from tdutils.pgrestutil import Postgrest

form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM_CRED.get("api_key")

# create postgrest instance
pgrest = Postgrest(
    "http://transportation-data-test.austintexas.io/signal_pms",
    auth=JOB_DB_API_TOKEN_test,
)

def get_postgre_records:
	pass

def get_knack_data:
	pass

def prepare_payloads:
	pass

def update_knack:
	pass

if __name__ == "__main__":
	pass

