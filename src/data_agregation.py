import duckdb


def create_agregate_tables():

    """
    Crée les tables d'agrégation dans la base de données DuckDB.

    - Lit le fichier SQL situé dans `data/sql_statements/create_agregate_tables.sql`.
    - Découpe le fichier en instructions SQL individuelles.
    - Exécute chaque instruction pour créer ou recréer les tables d'agrégation nécessaires.
    """
     
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            con.execute(statement)


def agregate_dim_city():
    """
    Agrège les données de la table DIM_CITY à partir de la table CONSOLIDATE_CITY.

    - Insère ou remplace les données dans la table DIM_CITY.
    - Sélectionne les informations (ID, nom, population) de la table CONSOLIDATE_CITY.
    - Prend uniquement les données les plus récentes (selon `CREATED_DATE`).
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)

def agregate_dim_station():
    """
    Agrège les données de la table DIM_STATION à partir de la table CONSOLIDATE_STATION.

    - Insère ou remplace les données dans la table DIM_STATION.
    - Sélectionne les informations (ID, code, nom, adresse, longitude, latitude, statut, capacité) de la table CONSOLIDATE_STATION.
    - Prend uniquement les données les plus récentes (selon `CREATED_DATE`).
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION 
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)

def build_fact_station_statement():
    """
    Agrège les données de station dans la table FACT_STATION_STATEMENT.

    - Insère ou remplace les données dans la table FACT_STATION_STATEMENT.
    - Joint les données des stations (DIM_STATION) avec les déclarations de station (FACT_STATION_STATEMENT).
    - Agrège les informations sur les emplacements disponibles et les vélos disponibles par station et par ville.
    - Prend la date la plus récente pour les déclarations de station.
    """

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    sql_statement = """
        INSERT OR REPLACE INTO FACT_STATION_STATEMENT
        SELECT
            ss.STATION_ID,                             
            c.ID AS CITY_ID,                          
            ss.BICYCLE_DOCKS_AVAILABLE,               
            ss.BICYCLE_AVAILABLE,                     
            ss.LAST_STATEMENT_DATE,                  
            ss.CREATED_DATE                          
        FROM
            CONSOLIDATE_STATION_STATEMENT ss
        JOIN
            CONSOLIDATE_STATION s ON ss.STATION_ID = s.ID      
        JOIN
            DIM_CITY c ON s.CITY_CODE = c.ID         
    """

    con.execute(sql_statement)


def get_bicycle_dock_availability_by_city():
    # Nb d'emplacements disponibles de vélos dans une ville

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
        SELECT 
            dm.NAME, 
            tmp.SUM_BICYCLE_DOCKS_AVAILABLE 
        FROM 
            DIM_CITY dm 
        INNER JOIN (
            SELECT 
                CITY_ID, 
                SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE 
            FROM 
                FACT_STATION_STATEMENT 
            WHERE 
                CREATED_DATE = (
                    SELECT 
                        MAX(CREATED_DATE) 
                    FROM 
                    CONSOLIDATE_STATION
                ) 
            GROUP BY 
                CITY_ID
            ) tmp ON dm.ID = tmp.CITY_ID 
        WHERE 
            lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

    """

    df_result = con.execute(sql_statement).fetchdf()
    print("exécution requête : Nb d'emplacements disponibles de vélos dans une ville")
    print(df_result)

def get_average_bikes_available_per_station():
    #Nb de vélos disponibles en moyenne dans chaque station
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
        SELECT 
            ds.name, 
            ds.code, 
            ds.address, 
            tmp.avg_dock_available 
        FROM 
            DIM_STATION ds 
        JOIN (
            SELECT 
                station_id, 
                AVG(BICYCLE_AVAILABLE) AS avg_dock_available 
            FROM 
                FACT_STATION_STATEMENT 
            GROUP BY 
                station_id
            ) AS tmp ON ds.id = tmp.station_id;
    """

    df_result = con.execute(sql_statement).fetchdf()

    print("exécution requête : Nb de vélos disponibles en moyenne dans chaque station")
    print(df_result)