import psycopg2
import os

# Parâmetros de conexão
db_config = {
    "host": os.getenv("DB_HOST", "db"),
    "database": os.getenv("DB_NAME", "project_ida"),
    "user": os.getenv("DB_USER", "julio"),
    "password": os.getenv("DB_PASSWORD", "123456"),
    "port": os.getenv("DB_PORT", "5432")
}

# Lê o conteúdo SQL do arquivo
with open("create_view.sql", "r", encoding="utf-8") as file:
    sql_script = file.read()

try:
    print("Conectando ao banco...")
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    print("Executando o script para criar a view...")
    cur.execute(sql_script)
    
    conn.commit()
    print("View criada com sucesso.")

    cur.close()
    conn.close()
except Exception as e:
    print("Erro ao criar a view:", e)