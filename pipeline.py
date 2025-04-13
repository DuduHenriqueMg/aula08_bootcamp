from etl import pipeline_calcular_kpi_de_vendas_consolidao

pasta:str = 'data'
formato_de_saida:list = ["csv", "parquet"]

pipeline_calcular_kpi_de_vendas_consolidao(pasta, formato_de_saida)