"""Programa básico para manipulação e visualização
de arquivos parquet"""

import pandas as pd 

caminho = "data/silver/prf_acidentes_2025.parquet"

df = pd.read_parquet(caminho)

print(df.head())