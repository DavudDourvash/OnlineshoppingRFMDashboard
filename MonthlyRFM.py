#%%
# Read Data,Import Packages,Set Parameters & Wrangling
#%%
# [1]
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import numpy as np
from tifffile.tifffile import rational
#%%
# [2]
# for Querying MongoDB
beginning_date = datetime(2024, 9, 21, 20, 30)
finishing_date = datetime.now()
module_name = "Onlineshopping"
