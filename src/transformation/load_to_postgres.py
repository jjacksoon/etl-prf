import os
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Carregando variáveis de ambiente do arquivo .env
load_dotenv()

def load_gold_to_postgres():
    #Pegando credenciais
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    host = "localhost"
    port = "5432"

    # 2. Configurando conexão com sqlalchemy
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(connection_string)
    
    caminho_gold = "data/gold/acidentes_prf.parquet"

    if not os.path.exists(caminho_gold):
        print(f"ERROR: Arquivo {caminho_gold} não encontrado!")
        return

    try:
        print("Lendo caminhos da camada Gold (parquet)")
        df = pd.read_parquet(caminho_gold)

        print(f"Iniciando carga de {len(df)} registros no PostgreSQL")

        # 4. Enviando ao banco
        # replace - sobrescrevendo a tabela se ela já existir
        # chunksize - envia os dados em blocos para não sobrecarregar memória
        df.to_sql(
            'fato_acidentes',
            con=engine,
            if_exists = 'replace',
            index=False,
            chunksize=10000,
            method='multi' #Acelera a inserção no  Postgres
        )

        print("✅ SUCESSO: Dados carregados na tabela 'fato_acidentes'!")
    except Exception as e:
        print(f"Erro durante carga: {e}")



if __name__ == "__main__":
    load_gold_to_postgres()
