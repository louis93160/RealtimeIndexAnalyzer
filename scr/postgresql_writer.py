import psycopg2
import yaml

def load_config(config_file):
    # Charger la configuration YAML depuis un fichier
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Charger la configuration
config = load_config('config/config.yaml')

def insert_into_postgresql(partition):
    # Insérer les données d'une partition dans PostgreSQL
    conn = psycopg2.connect(
        dbname=config['postgres']['dbname'],
        user=config['postgres']['user'],
        password=config['postgres']['password'],
        host=config['postgres']['host']
    )
    try:
        cursor = conn.cursor()
        for row in partition:
            cursor.execute("""
                INSERT INTO public.tickers (symbol, low_1m, high_1m, avg_1m, diff_1m, rate_1m, low_5m, high_5m, avg_5m, diff_5m, rate_5m)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    low_1m = EXCLUDED.low_1m,
                    high_1m = EXCLUDED.high_1m,
                    avg_1m = EXCLUDED.avg_1m,
                    diff_1m = EXCLUDED.diff_1m,
                    rate_1m = EXCLUDED.rate_1m,
                    low_5m = EXCLUDED.low_5m,
                    high_5m = EXCLUDED.high_5m,
                    avg_5m = EXCLUDED.avg_5m,
                    diff_5m = EXCLUDED.diff_5m,
                    rate_5m = EXCLUDED.rate_5m
            """, (
                row['symbol'], row['low_1m'], row['high_1m'], row['avg_1m'], row['diff_1m'], row['rate_1m'],
                row['low_5m'], row['high_5m'], row['avg_5m'], row['diff_5m'], row['rate_5m']
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
