CREATE TABLE IF NOT EXISTS air_quality (
    unique_id INTEGER NOT NULL,
    indicator_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    measure TEXT NOT NULL,
    measure_info TEXT NOT NULL,
    geo_type_name TEXT NOT NULL,
    geo_join_id INTEGER NOT NULL,
    geo_place_name TEXT NOT NULL,
    time_period TEXT NOT NULL,
    start_date DATE NOT NULL,
    data_value DECIMAL(6,2) NOT NULL,
    message TEXT,

    PRIMARY KEY (unique_id)
);