import json
from datetime import datetime, date

import duckdb
import os
import pandas as pd

def get_max_station_id(con):
    """
    Récupère l'ID maximum existant dans la table CONSOLIDATE_STATION.
    Si la table est vide, retourne 0.
    """
    result = con.execute("SELECT MAX(CAST(ID AS INTEGER)) FROM CONSOLIDATE_STATION;").fetchone()
    return result[0] if result[0] is not None else 0


def serialize_data(raw_json: str, file_name: str):

    """
    Sérialise et enregistre des données JSON dans un fichier local.
    
    Arguments:
    - raw_json (str) : Les données JSON brutes à sérialiser et enregistrer.
    - file_name (str) : Le nom du fichier de destination.


    - Crée un dossier dans `data/raw_data/` si ce dernier n'existe pas déjà.
    - Sauvegarde le contenu JSON (raw_json) dans un fichier dont le nom est spécifié (`file_name`).
    - Le dossier utilisé pour l'enregistrement est daté en fonction de la date actuelle (`today_date`).

    """

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
