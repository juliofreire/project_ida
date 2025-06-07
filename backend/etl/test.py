import pandas as pd

df = pd.read_csv("../data/processed/dados_tratados.csv")

# Supondo que df já seja seu DataFrame
nan_variaveis = df[df["VARIÁVEL"].isna()]

print("Linhas com 'VARIÁVEL' == NaN:")
print(nan_variaveis)