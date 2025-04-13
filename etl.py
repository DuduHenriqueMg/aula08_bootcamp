import pandas as pd
import os
import glob



def extrair_dados(pasta: str) -> pd.DataFrame:
    
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)

    return df_total


def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

def carregar_dados(df: pd.DataFrame, format_saida: list):
    if 'csv' in format_saida:
        df.to_csv('dados.csv')
    if 'parquet' in format_saida:
        df.to_parquet('dados.parquet')
        
def pipeline_calcular_kpi_de_vendas_consolidao(pasta: str, formato_de_saida:list):
    df_extraido = extrair_dados(pasta)
    df_calculado = calcular_kpi_de_total_de_vendas(df_extraido)
    carregar_dados(df_extraido, formato_de_saida)
    
if __name__ == "__main__":
    pasta = 'data'
    df_extraido = extrair_dados(pasta=pasta)
    df_calculado = calcular_kpi_de_total_de_vendas(df_extraido)
    carregar_dados(df_calculado, ['csv', 'parquet'])
    