# import packages
import pandas as pd
import numpy as np
import knackpy as kp
import fulcrum as fc
import requests
import pdb

# import credentials
from config.secrets import *

key = FULCRUM_CRED.get("api_key")

print(key)

# test: Query a form from fulcrum (Preventive maintenance)
fulcrum = fc.Fulcrum(key = key)

print(fulcrum)
forms = fulcrum.forms.search(url_params={'id': '44359e32-1a7f-41bd-b53e-3ebc039bd21a'})
print(forms)

form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
form = fulcrum.forms.find(form_id)

print(form)

# url_params={'form_id': '44359e32-1a7f-41bd-b53e-3ebc039bd21a'} ## PM
# records = fulcrum.records.search(url_params={'form_id': 'a1cb3ac7-146f-491a-a4a2-47737fb12074'})

# signal_pm_raw = fulcrum.records.find("44359e32-1a7f-41bd-b53e-3ebc039bd21a")