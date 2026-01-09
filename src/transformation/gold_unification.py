import os
import pandas as pd 

#Configuração dos caminhos
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

def create_gold_layer():
    # 1. Garantindo que a pasta gold existe
    if not os.path.exists(GOLD_DIR):
        od.makedirs(GOLD_DIR)
        print(f"Pasta {GOLD_DIR} criada.")
    
    # 2. Listar todos os arquivos parquet que estão na silver
    arquivos_silver = []
    for arquivo in os.listdir(SILVER_DIR):
        if arquivo.endswith(".parquet"):
            arquivos_silver.append(arquivo)
    
    arquivos_silver.sort()

    if not arquivos_silver:
        print("Nenhum arquivo encontrado na camada Silver!")

    # 3. Lista para armazenar os dataframes de cada ano
    lista_df = []
    print(f"Iniciando a unificação de {len(arquivos_silver)} arquivos...")

    for nome_arquivo in arquivos_silver:
        caminho_arquivo = os.path.join(SILVER_DIR, nome_arquivo)

        #lendo arquivo parquet
        df_ano = pd.read_parquet(caminho_arquivo)

        #Adicionando à lista
        lista_df.append(df_ano)
        print(f"Arquivo {nome_arquivo} carregado")

    
    # 4. Concatenando (união) todos os anos em uma unica tabela
    print("Unificando tabelas...")
    df_final = pd.concat(lista_df, ignore_index = True)

    # --- TRATAMENTO DE TIPOS PARA A GOLD ---
    print("Padronizando tipos de colunas geográficas...")
    
    # Lista de colunas que costumam dar erro por serem mistas (texto e número)
    colunas_para_texto = ['br', 'km', 'latitude', 'longitude', 'uop', 'delegacia', 'regional']
    
    for col in colunas_para_texto:
        if col in df_final.columns:
            # Forçando para string e removendo o "nan" de texto que o pandas cria
            df_final[col] = df_final[col].astype(str).replace(['nan', 'None', 'NaN'], '')

    # 5. Salvando o arquivo "Mestre" na Gold
    caminho_gold = os.path.join(GOLD_DIR, "acidentes_prf.parquet")

    #snappy torna a compressão mais rápida e leve
    #pyarrow lida melhor com tipos mistos
    df_final.to_parquet(caminho_gold, index=False, compression='snappy', engine='pyarrow')

    print(f"\n--- SUCESSO! ---")
    print(f"Arquivo Gold fgerado em: {caminho_arquivo}")
    print(f"Total de registros unificados: {len(df_final)}")




if __name__ == "__main__":
    create_gold_layer()
