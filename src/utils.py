import json
from datetime import datetime, date

import duckdb
import pandas as pd

def get_max_station_id(con):
    """
    Récupère l'ID maximum existant dans la table CONSOLIDATE_STATION.
    Si la table est vide, retourne 0.
    """
    result = con.execute("SELECT MAX(CAST(ID AS INTEGER)) FROM CONSOLIDATE_STATION;").fetchone()
    return result[0] if result[0] is not None else 0