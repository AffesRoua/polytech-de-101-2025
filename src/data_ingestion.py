import os
from datetime import datetime

import requests
import utils

def get_paris_realtime_bicycle_data():
    """
    Récupère les données en temps réel de la disponibilité des vélos à Paris.

    - Fait une requête HTTP GET pour récupérer les données JSON depuis l'API OpenData Paris.
    - Les données récupérées concernent la disponibilité en temps réel des vélos Vélib' à Paris.
    - Sérialise les données JSON et les enregistre dans un fichier local sous forme de fichier `.json`.

    """
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    utils.serialize_data(response.text, "paris_realtime_bicycle_data.json")

def get_nantes_realtime_bicycle_data():
    """
    Récupère les données en temps réel de la disponibilité des vélos à Nantes.

    - Fait une requête HTTP GET pour récupérer les données JSON depuis l'API OpenData Nantes.
    - Les données récupérées concernent la disponibilité en temps réel des vélos ' à Nantes.
    - Sérialise les données JSON et les enregistre dans un fichier local sous forme de fichier `.json`.

    """
    
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_disponibilite-temps-reel-velos-libre-service-naolib-nantes-metropole/exports/json"
    
    response = requests.request("GET", url)
    
    utils.serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def get_nantes_realtime_bicycle_station_localisation_data():
    """
    Récupère les données en temps réel de la localisation des stations de vélos à Nantes.

    - Fait une requête HTTP GET pour récupérer les données JSON depuis l'API OpenData Nantes.
    - Les données récupérées concernent la localisation en temps réel des vélos à Nantes.
    - Sérialise les données JSON et les enregistre dans un fichier local sous forme de fichier `.json`.

    """
    
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole/exports/json"
    
    response = requests.request("GET", url)
    
    utils.serialize_data(response.text, "nantes_bicycle_station_localisation_data.json")


def get_communes_data():
    """
    Récupère les données en temps réel de la disponibilité des vélos à Nantes.

    - Fait une requête HTTP GET pour récupérer les données JSON depuis l'API OpenData Nantes.
    - Les données récupérées concernent la disponibilité en temps réel des vélos ' à Nantes.
    - Sérialise les données JSON et les enregistre dans un fichier local sous forme de fichier `.json`.

    """
    
    url = "https://geo.api.gouv.fr/communes"
    
    response = requests.request("GET", url)
    
    utils.serialize_data(response.text, "communes_data.json")



