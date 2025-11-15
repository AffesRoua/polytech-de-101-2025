from data_agregation import (
    create_agregate_tables,
    agregate_dim_station,
    agregate_dim_city,
    build_fact_station_statement,
    get_bicycle_dock_availability_by_city,
    get_average_bikes_available_per_station
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_station_data , 
    consolidate_city_data,
    consolidate_station_statement_data,
)
from data_ingestion import (
    get_paris_realtime_bicycle_data
)

def main():
    print("Process start.")
    # data ingestion

    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    print("Data ingestion ended.")

    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()
    # Other consolidation here
    print("Consolidation data ended.")

    # data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_station()
    agregate_dim_city()
    build_fact_station_statement()
    get_average_bikes_available_per_station()
    get_bicycle_dock_availability_by_city()
    # Other agregations here
    print("Agregate data ended.")

if __name__ == "__main__":
    main()