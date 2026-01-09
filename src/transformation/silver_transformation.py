import pandas as pd 
import os 

# --- Configurações de Pastas ---
BRONZE_DIR = 'data/bronze'
SILVER_DIR = 'data/silver'

#Função de teste para verificar quais são as colunas que estão nas tabelas (verificando padrões e mapeando reajustes)
def check_columns(year):
    path = f"data/bronze/datatran{year}.csv"
    if os.path.exists(path):
        #lendo primeira linha
        try:
            df = pd.read_csv(path, sep=None, engine='python', nrows=0, encoding='latin-1')
            print(f"Colunas de {year}: {list(df.columns)}")    
        except Exception as e:
            print(f"Erro ao ler {year}: {e}")


#Processamento da camada silver
def process_silver():
    # 1.Garantir que a pasta silver exista
    if not os.path.exists(SILVER_DIR):
        os.makedirs(SILVER_DIR)
        print(f"Pasta {SILVER_DIR} criada.")

    # 2. Pegando todos arquivos da pasta bronze e filtrando apenas csv
    todos_arquivos = os.listdir(BRONZE_DIR)
    lista_csv = []

    for nome in todos_arquivos:
        if nome.endswith(".csv"):
            lista_csv.append(nome)
    
    # 3. Ordenando lista para processar ano a ano
    lista_csv.sort()

    # 4. Iniciando o processamento de cada arquivo
    for arquivo in lista_csv:
        caminho_entrada = os.path.join(BRONZE_DIR, arquivo)

        #Extraindo o ano do nome do arquivo para usar depois
        ano_str = ""
        for letra in arquivo:
            if letra.isdigit():
                ano_str += letra
        
        #Definindo nome do arquivo de saida (parquet)
        arquivo_saida = f"prf_acidentes_{ano_str}.parquet"
        caminho_saida = os.path.join(SILVER_DIR, arquivo_saida)

        #Idempotência: se já tiver processado esse ano, não faça novamente
        if os.path.exists(caminho_saida):
            print(f"O ano {ano_str} já está na silver. Pulando...")
            continue
        
        print(f"Limpando e transformando dados do ano: {ano_str}")

        try:
            # 5. Leitura do arquivo
            # sep=None faz o pandas descobrir se é vírgula ou ponto-e-vírgula sozinho
            # encoding='latin-1' resolve o erro de caracteres especiais (acentos)
            df = pd.read_csv(caminho_entrada, sep=None, engine='python', encoding='latin-1')

            # 6. Padronização básica:
            # Deixando todos os nomes de colunas em letras maiusculas e sem espaços extras
            novas_colunas= []
            for col in df.columns:
                novas_colunas.append(col.strip().lower())
            df.columns = novas_colunas

            # 7. Criando a coluna de ano se ela não existir
            if 'ano' not in df.columns:
                df['ano'] = int(ano_str)

            # 8. Limpeza das colunas numéricas (trocanod erros por 0 e garantindo que seja número)
            colunas_corrigir = ['km', 'pessoas','mortos', 'feridos_leves', 'feridos_graves', 'ilesos', 'ignorados', 'feridos', 'veiculos']
            for col in colunas_corrigir:
                if col in df.columns:
                    # pd.to_numeric converte texto em número; errors='coerce' vira NaN se der erro
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # fillna(0) troca o que deu erro por zero
                    df[col] = df[col].fillna(0).astype(int)
            
            # 9. Converter a data para o formato que o computador entenda como data real
            # %d = dia, %m = mês, %Y = ano com 4 dígitos
            df['data_inversa'] = pd.to_datetime(df['data_inversa'], format='%d/%m/%Y', errors='coerce')
        
            # 10. Salvar o resultado final na pasta Silver
            # Index=False evita que o pandas crie uma coluna extra de números
            df.to_parquet(caminho_saida, index=False)
            print(f"Sucesso! Arquivo {arquivo_saida} gerado.")

        except Exception as e:
            print(f"Houve um erro no ano {ano_str} : {e}")

if __name__ == "__main__":
    process_silver()
