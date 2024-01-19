import psycopg2
from datetime import date, timedelta
import random


def create_database_connection():
    """Create a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='my_small_dwh',
        port=5432
    )
    return conn


def setup_marine_schema(cur):
    """Create the necessary schema and tables in the marine database."""
    cur.execute('''
        CREATE SCHEMA IF NOT EXISTS marine;
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS marine.dim_cities (
            id SERIAL PRIMARY KEY,
            city_name VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS marine.dim_ship_types (
            id SERIAL PRIMARY KEY,
            ship_type VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS marine.dim_companies (
            id SERIAL PRIMARY KEY,
            company_name VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS marine.fact_ship_travels (
            id SERIAL PRIMARY KEY,
            ship_type_id INTEGER REFERENCES marine.dim_ship_types(id),
            origin_city_id INTEGER REFERENCES marine.dim_cities(id),
            destination_city_id INTEGER REFERENCES marine.dim_cities(id),
            company_id INTEGER REFERENCES marine.dim_companies(id),
            travel_date DATE,
            distance_travelled NUMERIC(10,2)  -- Distance in miles or km
        );
    ''')


def insert_marine_sample_data(cur):
    """Insert sample data into the marine tables."""
    cities = ["Los Angeles", "New York", "London", "Tokyo", "Sydney"]
    for city in cities:
        cur.execute('''
            INSERT INTO marine.dim_cities (city_name) VALUES (%s);
        ''', (city,))

    ship_types = ["Cargo", "Cruise", "Fishing", "Naval", "Yacht"]
    for ship_type in ship_types:
        cur.execute('''
            INSERT INTO marine.dim_ship_types (ship_type) VALUES (%s);
        ''', (ship_type,))

    companies = ["MarineCorp", "SeaTravels", "OceanWaves", "BlueSea"]
    for company in companies:
        cur.execute('''
            INSERT INTO marine.dim_companies (company_name) VALUES (%s);
        ''', (company,))

    for _ in range(42):
        ship_type_id = random.randint(1, len(ship_types))
        origin_city_id = random.randint(1, len(cities))
        destination_city_id = origin_city_id
        while destination_city_id == origin_city_id:
            destination_city_id = random.randint(1, len(cities))
        company_id = random.randint(1, len(companies))
        travel_date = date.today() - timedelta(days=random.randint(0, 365 * 2))  # travels from the last 2 years
        distance_travelled = random.uniform(100, 5000)  # 100 to 5000 miles

        cur.execute('''
            INSERT INTO marine.fact_ship_travels (ship_type_id, origin_city_id, destination_city_id, company_id, travel_date, distance_travelled)
            VALUES (%s, %s, %s, %s, %s, %s);
        ''', (ship_type_id, origin_city_id, destination_city_id, company_id, travel_date, distance_travelled))


def setup_financial_schema(cur):
    """Create the necessary schema and tables in the financial database."""
    cur.execute('''
        CREATE SCHEMA IF NOT EXISTS financial;
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.dim_banks (
            id SERIAL PRIMARY KEY,
            bank_name VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.dim_currencies (
            id SERIAL PRIMARY KEY,
            currency_name VARCHAR(10)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.dim_clients (
            id SERIAL PRIMARY KEY,
            client_name VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.dim_cargo_types (
            id SERIAL PRIMARY KEY,
            cargo_type_name VARCHAR(100)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.fact_bank_transactions (
            id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES financial.dim_clients(id),
            bank_id INTEGER REFERENCES financial.dim_banks(id),
            transaction_date DATE,
            amount NUMERIC(10,2),
            currency_id INTEGER REFERENCES financial.dim_currencies(id)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial.fact_cargo_amounts (
            id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES financial.dim_clients(id),
            cargo_type_id INTEGER REFERENCES financial.dim_cargo_types(id),
            transaction_date DATE,
            weight NUMERIC(10,2)  -- Weight in kg or lbs
        );
    ''')


def insert_financial_sample_data(cur):
    """Insert sample data into the financial tables."""
    banks = ["GlobalBank", "MoneySafe", "TrustBank", "National Savings"]
    for bank in banks:
        cur.execute('''
            INSERT INTO financial.dim_banks (bank_name) VALUES (%s);
        ''', (bank,))

    currencies = ["USD", "EUR", "GBP", "JPY", "AUD"]
    for currency in currencies:
        cur.execute('''
            INSERT INTO financial.dim_currencies (currency_name) VALUES (%s);
        ''', (currency,))

    clients = ["Alpha Corp", "Beta Ltd", "Gamma Industries", "Delta Services"]
    for client in clients:
        cur.execute('''
            INSERT INTO financial.dim_clients (client_name) VALUES (%s);
        ''', (client,))

    cargo_types = ["Electronics", "Apparel", "Groceries", "Machinery", "Pharmaceuticals"]
    for cargo_type in cargo_types:
        cur.execute('''
            INSERT INTO financial.dim_cargo_types (cargo_type_name) VALUES (%s);
        ''', (cargo_type,))

    for _ in range(24):
        client_id = random.randint(1, len(clients))
        bank_id = random.randint(1, len(banks))
        transaction_date = date.today() - timedelta(days=random.randint(0, 365))
        amount = random.uniform(100, 10000)
        currency_id = random.randint(1, len(currencies))

        cur.execute('''
            INSERT INTO financial.fact_bank_transactions (client_id, bank_id, transaction_date, amount, currency_id)
            VALUES (%s, %s, %s, %s, %s);
        ''', (client_id, bank_id, transaction_date, amount, currency_id))

    for _ in range(18):
        client_id = random.randint(1, len(clients))
        cargo_type_id = random.randint(1, len(cargo_types))
        transaction_date = date.today() - timedelta(days=random.randint(0, 365))
        weight = random.uniform(1, 1000)

        cur.execute('''
            INSERT INTO financial.fact_cargo_amounts (client_id, cargo_type_id, transaction_date, weight)
            VALUES (%s, %s, %s, %s);
        ''', (client_id, cargo_type_id, transaction_date, weight))


def setup_data_quality_schema(cur):
    """Create the data quality schema and tables."""
    cur.execute('''
        CREATE SCHEMA IF NOT EXISTS data_quality;
    ''')


def setup_migration_schema(cur):
    """Create the schema and tables for data migration."""
    cur.execute('''
        CREATE SCHEMA IF NOT EXISTS migration;
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS migration.fact_ship_travels (
            id SERIAL PRIMARY KEY,
            ship_type_id INTEGER REFERENCES marine.dim_ship_types(id),
            origin_city_id INTEGER REFERENCES marine.dim_cities(id),
            destination_city_id INTEGER REFERENCES marine.dim_cities(id),
            company_id INTEGER REFERENCES marine.dim_companies(id),
            travel_date DATE,
            distance_travelled NUMERIC(10,2)  -- Distance in miles or km
        );
        ''')

def insert_migration_data(cur):
    """Insert data into the migration table."""
    cur.execute('''
        INSERT INTO migration.fact_ship_travels (ship_type_id, origin_city_id, destination_city_id, company_id, travel_date, distance_travelled)
        SELECT ship_type_id, origin_city_id, destination_city_id, company_id, travel_date, distance_travelled
        FROM   marine.fact_ship_travels;
    ''')



def start_generation():
    """Main function to generate the sample data in the database."""
    conn = create_database_connection()
    cur = conn.cursor()

    setup_marine_schema(cur)
    insert_marine_sample_data(cur)

    setup_financial_schema(cur)
    insert_financial_sample_data(cur)

    setup_data_quality_schema(cur)

    setup_migration_schema(cur)
    insert_migration_data(cur)

    conn.commit()
    cur.close()
    conn.close()
