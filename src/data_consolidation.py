import json
from datetime import datetime, date

import duckdb
import pandas as pd

import utils

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    """
    Crée les tables de consolidation dans la base de données DuckDB.

    -lit le fichier SQL situé dans `data/sql_statements/create_consolidate_tables.sql`
    -découpe le fichier en instructions SQL individuelles
    -exécute les requêtes pour créer (ou recréer) les tables nécessaires à l'analyse.

    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data() :

    """
    Consolide les données au niveau de la ville dans la table CONSOLIDATE_CITY.

    - Extrait les informations uniques des villes (code INSEE, arrondissement, population).
    - Enrichit les données avec la date de création du jour.
    - Supprime les doublons pour garantir l'unicité des villes.
    - Insère ou remplace les données dans la table CONSOLIDATE_CITY.

    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    
    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)
    raw_data_df = pd.json_normalize(data)

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name"
    }, inplace=True)

    city_data_df.drop_duplicates(inplace = True)
    city_data_df["created_date"] = date.today()
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    



def consolidate_station_paris_data():

    """
    Consolide les données de station dans la table CONSOLIDATE_STATION.

    - Extrait les informations de base sur chaque station (nom, ville, capacité, etc.).
    - Génère un ID unique pour chaque station.
    - Supprime les doublons dans les données.
    - Insère ou remplace les données dans la table CONSOLIDATE_STATION.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    station_data_df = pd.DataFrame({      
        "CODE": raw_data_df["stationcode"],
        "NAME": raw_data_df["name"],
        "CITY_NAME": raw_data_df["nom_arrondissement_communes"],
        "CITY_CODE": raw_data_df["code_insee_commune"],
        "ADDRESS": None,                             
        "LONGITUDE": raw_data_df["coordonnees_geo.lon"],
        "LATITUDE": raw_data_df["coordonnees_geo.lat"],
        "STATUS": raw_data_df["is_installed"],
        "CREATED_DATE": date.today(),  
        "CAPACITTY": raw_data_df["capacity"]          
    })

    station_data_df.drop_duplicates(inplace = True)
    station_data_df.insert(0, "ID", station_data_df.index.astype(str))
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

def consolidate_station_nantes_data():

    """
    Consolide les données de station dans la table CONSOLIDATE_STATION.

    - Extrait les informations de base sur chaque station (nom, ville, capacité, etc.).
    - Génère un ID unique pour chaque station.
    - Supprime les doublons dans les données.
    - Insère ou remplace les données dans la table CONSOLIDATE_STATION.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data_station_real_time = json.load(fd)
    data_station_real_time = pd.json_normalize(data_station_real_time)

    with open(f"data/raw_data/{today_date}/nantes_bicycle_station_localisation_data.json") as fd:
        data_station_localisation = json.load(fd)
    data_station_localisation = pd.json_normalize(data_station_localisation)

    data_station=data_station_real_time.merge(
        data_station_localisation,
        how='left',
        left_on='address',
        right_on='localisation'
    )

    station_data_df = pd.DataFrame({      
        "CODE": data_station["number"],
        "NAME": data_station["name"],
        "CITY_NAME": data_station["commune"],
        "CITY_CODE": data_station["insee"],
        "ADDRESS": data_station["address"],                             
        "LONGITUDE": data_station["position.lon"],
        "LATITUDE": data_station["position.lat"],
        "STATUS": None,
        "CREATED_DATE": date.today(),  
        "CAPACITTY": data_station["bike_stands"]          
    })
    station_data_df.drop_duplicates(inplace = True)

    max_id = int(utils.get_max_station_id(con))
    station_data_df.insert(0, "ID", range(max_id + 1, max_id + 1 + len(station_data_df)))
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")


def consolidate_station_statement_paris_data():

    """
    Consolide les données de disponibilité des stations dans la table CONSOLIDATE_STATION_STATEMENT.

    - Récupère les IDs des stations depuis la table CONSOLIDATE_STATION.
    - Effectue une jointure entre les données brutes et les IDs des stations.
    - Insère ou remplace les données dans la table CONSOLIDATE_STATION_STATEMENT.
    
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    station_data_df = con.execute("SELECT CODE, ID FROM CONSOLIDATE_STATION").fetchdf()
    merged_df = raw_data_df.merge(station_data_df[['CODE', 'ID']], how='left', left_on='stationcode', right_on='CODE')
    
    # Vérifier les lignes sans correspondance (ID manquant)
    if merged_df['ID'].isnull().any():
        print("Attention : certaines stations n'ont pas pu être associées à un ID.")
    
    # Création du DataFrame consolidé
    station_statement_df = pd.DataFrame({
        "STATION_ID": merged_df["ID"],  # Utiliser "ID" comme identifiant de station
        "BICYCLE_DOCKS_AVAILABLE": merged_df["numdocksavailable"],
        "BICYCLE_AVAILABLE": merged_df["numbikesavailable"],
        "LAST_STATEMENT_DATE": pd.to_datetime(merged_df["duedate"]),
        "CREATED_DATE": date.today()        
    })

    # Supprimer les doublons et réinitialiser l'index
    station_statement_df.drop_duplicates(inplace=True, ignore_index=True)
    
    # Insérer les données dans la table de faits CONSOLIDATE_STATION_STATEMENT
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_df;")


def consolidate_station_statement_paris_data():

    """
    Consolide les données de disponibilité des stations dans la table CONSOLIDATE_STATION_STATEMENT.

    - Récupère les IDs des stations depuis la table CONSOLIDATE_STATION.
    - Effectue une jointure entre les données brutes et les IDs des stations.
    - Insère ou remplace les données dans la table CONSOLIDATE_STATION_STATEMENT.
    
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    station_data_df = con.execute("SELECT CODE, ID FROM CONSOLIDATE_STATION").fetchdf()
    merged_df = raw_data_df.merge(station_data_df[['CODE', 'ID']], how='left', left_on='stationcode', right_on='CODE')
    
    # Vérifier les lignes sans correspondance (ID manquant)
    if merged_df['ID'].isnull().any():
        print("Attention : certaines stations n'ont pas pu être associées à un ID.")
    
    # Création du DataFrame consolidé
    station_statement_df = pd.DataFrame({
        "STATION_ID": merged_df["ID"],  # Utiliser "ID" comme identifiant de station
        "BICYCLE_DOCKS_AVAILABLE": merged_df["numdocksavailable"],
        "BICYCLE_AVAILABLE": merged_df["numbikesavailable"],
        "LAST_STATEMENT_DATE": pd.to_datetime(merged_df["duedate"]),
        "CREATED_DATE": date.today()        
    })

    # Supprimer les doublons et réinitialiser l'index
    station_statement_df.drop_duplicates(inplace=True, ignore_index=True)
    
    # Insérer les données dans la table de faits CONSOLIDATE_STATION_STATEMENT
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_df;")


def consolidate_station_statement_nantes_data():

    """
    Consolide les données de disponibilité des stations dans la table CONSOLIDATE_STATION_STATEMENT.

    - Récupère les IDs des stations depuis la table CONSOLIDATE_STATION.
    - Effectue une jointure entre les données brutes et les IDs des stations.
    - Insère ou remplace les données dans la table CONSOLIDATE_STATION_STATEMENT.
    
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    station_data_df = con.execute("SELECT CODE, ID,CITY_NAME FROM CONSOLIDATE_STATION").fetchdf()
    merged_df = raw_data_df.merge(station_data_df[['CODE', 'ID','CITY_NAME']], how='inner', left_on='number', right_on='CODE')
    
    # Vérifier les lignes sans correspondance (ID manquant)
    if merged_df['ID'].isnull().any():
        print("Attention : certaines stations n'ont pas pu être associées à un ID.")

    # Création du DataFrame consolidé
    station_statement_df = pd.DataFrame({
        "STATION_ID": merged_df["ID"],  # Utiliser "ID" comme identifiant de station
        "BICYCLE_DOCKS_AVAILABLE": merged_df["available_bike_stands"],
        "BICYCLE_AVAILABLE": merged_df["available_bikes"],
        "LAST_STATEMENT_DATE": pd.to_datetime(merged_df["last_update"]),
        "CREATED_DATE": date.today()        
    })

    # Supprimer les doublons et réinitialiser l'index
    station_statement_df.drop_duplicates(inplace=True, ignore_index=True)
    
    # Insérer les données dans la table de faits CONSOLIDATE_STATION_STATEMENT
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_df;")

