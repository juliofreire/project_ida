import pandas as pd
from sqlalchemy import create_engine, text

class Loader:
    def __init__(self, db_url, input_file):
        """ Initializes the Loader with a database URL and an optional input file.
            db_url: The database connection string.
            input_file: Optional path to a CSV file to load data from.
        """
        self.input_file = input_file
        self.engine = create_engine(db_url)


    def get_or_create_id(self, table, value):
        """
        Inserts a new record into the specified table if it does not exist,
        or retrieves the existing record's ID if it does.
        """
        with self.engine.begin() as conn:
            result = conn.execute(text(f"""
                INSERT INTO {table} (nome)
                VALUES (:value)
                ON CONFLICT (nome) DO NOTHING
                RETURNING id
            """), {"value": value})
            inserted = result.scalar()
            if inserted is not None:
                return inserted

            # Já existia, buscar o ID
            result = conn.execute(text(f"""
                SELECT id FROM {table} WHERE nome = :value
            """), {"value": value})
            return result.scalar()


    def get_or_create_data_id(self, date_value):
        """ Inserts a new date into the dim_data table if it does not exist,
            or retrieves the existing record's ID if it does.
        """

        ano = date_value.year
        mes = date_value.month
        data_referencia = date_value.replace(day=1)  # Fix to the first day of the month

        with self.engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO dim_data (ano, mes, data_referencia)
                VALUES (:ano, :mes, :date_referencia)
                ON CONFLICT (data_referencia) DO NOTHING
                RETURNING id
            """), {
                "ano": ano,
                "mes": mes,
                "date_referencia": data_referencia
                })
            inserted = result.scalar()
            if inserted is not None:
                return inserted

            result = conn.execute(text("""
                SELECT id FROM dim_data WHERE data_referencia = :data_referencia
            """), {"data_referencia": data_referencia})
            return result.scalar()


    def insert_fact(self, servico_id, grupo_economico_id, variavel_id, data_id, valor):
        """ Inserts a new record into the fato_ida table.
            Assumes that servico_id, grupo_economico_id, variavel_id, and data_id
            are already created or retrieved using get_or_create_id.
        """

        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fato_ida (servico_id, grupo_economico_id, variavel_id, data_id, valor)
                VALUES (:servico_id, :grupo_economico_id, :variavel_id, :data_id, :valor)
            """), {
                "servico_id": servico_id,
                "grupo_economico_id": grupo_economico_id,
                "variavel_id": variavel_id,
                "data_id": data_id,
                "valor": valor
            })


    def load_dataframe(self, df: pd.DataFrame):
        """
        Espera um DataFrame com colunas:
        SERVIÇO | GRUPO ECONÔMICO | VARIÁVEL | data | valor
        """
        for _, row in df.iterrows():
            servico_id = self.get_or_create_id("dim_servico", row["SERVIÇO"])
            grupo_id = self.get_or_create_id("dim_grupo_economico", row["GRUPO ECONÔMICO"])
            variavel_id = self.get_or_create_id("dim_variavel", row["VARIÁVEL"])
            data_id = self.get_or_create_data_id(row["data"])
            self.insert_fact(servico_id, grupo_id, variavel_id, data_id, row["valor"])
    

    def run(self):
        """" Run the loader to load data from a DataFrame.
        """
        
        df = pd.read_csv(self.input_file)
        df = df.dropna(subset=["SERVIÇO", "GRUPO ECONÔMICO", "VARIÁVEL", "data", "valor"])
        df["data"] = pd.to_datetime(df["data"])
        df["data"] = df["data"].dt.to_period("M").dt.to_timestamp().dt.date

        self.load_dataframe(df)

        print("Data loaded successfully into the database.")
        

