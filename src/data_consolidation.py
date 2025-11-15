import json
from datetime import datetime, date

import duckdb
import pandas as pd

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

def consolidate_city_data():

    """
    Consolide les données au niveau de la ville dans la table CONSOLIDATE_CITY.

    - Extrait les informations uniques des villes (code INSEE, arrondissement, population).
    - Enrichit les données avec la date de création du jour.
    - Supprime les doublons pour garantir l'unicité des villes.
    - Insère ou remplace les données dans la table CONSOLIDATE_CITY.

    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)

    city_data_df.drop_duplicates(inplace = True)
    city_data_df["created_date"] = date.today()

    #print(city_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def get_max_station_id(con):
    """
    Récupère l'ID maximum existant dans la table CONSOLIDATE_STATION.
    Si la table est vide, retourne 0.
    """
    result = con.execute("SELECT MAX(CAST(ID AS INTEGER)) FROM CONSOLIDATE_STATION;").fetchone()
    return result[0] if result[0] is not None else 0


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
    #print(raw_data_df)
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


    #print(station_data_df)
    
    #con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

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
        data = json.load(fd)
    raw_data_df = pd.json_normalize(data["results"])
    #print(raw_data_df)
    station_data_df = pd.DataFrame({      
        "CODE": raw_data_df["number"],
        "NAME": raw_data_df["name"],
        "CITY_NAME": None,
        "CITY_CODE": None,
        "ADDRESS": raw_data_df["address"],                             
        "LONGITUDE": raw_data_df["position.lon"],
        "LATITUDE": raw_data_df["position.lat"],
        "STATUS": None,
        "CREATED_DATE": date.today(),  
        "CAPACITTY": raw_data_df["bike_stands"]          
    })
    station_data_df.drop_duplicates(inplace = True)

    max_id = int(get_max_station_id(con))
    print(max_id)
    print(type(int(max_id)))
    station_data_df.insert(0, "ID", range(max_id + 1, max_id + 1 + len(station_data_df)))

    #station_data_df.insert(0, "ID", station_data_df.index.astype(str))

    print(station_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")


def consolidate_station_statement_data():

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
    #print(station_statement_df)
    
    # Insérer les données dans la table de faits CONSOLIDATE_STATION_STATEMENT
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_df;")

