from sqlalchemy import create_engine, text
import time

def create_table(db_url):
    """
    Create the necessary tables in the database.
    """
    engine = create_engine(db_url)
    
    with engine.begin() as conn:
        # Create SERVICO table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_servico (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) UNIQUE
            )
        """))
        
        # Create GRUPO_ECONÔMICO table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_grupo_economico (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) UNIQUE
            )
        """))
        
        # Create VARIAVEL table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_variavel (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) UNIQUE 
            )
        """))

        # Create DATA table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_data (
                id SERIAL PRIMARY KEY,
                ano INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                data_referencia DATE UNIQUE
            )
        """))

        # Create main data table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fato_ida (
                id SERIAL PRIMARY KEY,
                servico_id INTEGER REFERENCES dim_servico(id),
                grupo_economico_id INTEGER REFERENCES dim_grupo_economico(id),
                variavel_id INTEGER REFERENCES dim_variavel(id),
                data_id INTEGER REFERENCES dim_data(id),
                valor FLOAT
            )
        """))


if __name__ == "__main__":
    # Example usage
    while True:
        try:
            create_table("postgresql+psycopg2://julio:123456@db/project_ida") # Attempt to create the tables
            break
        except Exception as e:
            print(f"Error creating tables: {e}. Retrying...")
            time.sleep(5)
    print("Tables created successfully.")