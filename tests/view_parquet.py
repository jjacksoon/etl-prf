"""Programa básico para manipulação e visualização
de arquivos parquet"""

import pandas as pd 

caminho = "data/gold/acidentes_prf.parquet"

df = pd.read_parquet(caminho)

df_coordenadas = df[df['latitude'] != '']

print(len(df_coordenadas))
print(df_coordenadas[['id','data_inversa','latitude','longitude']].head())