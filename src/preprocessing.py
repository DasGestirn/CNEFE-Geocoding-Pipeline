import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

Path("data/silver").mkdir(parents=True, exist_ok=True)

def padronizar_texto(texto):

    # Troca espaço vazio por uma string vazia
    if pd.isna(texto) or texto is None:
        return ""
    
    # Deixando todo o texto em caixa alta e remove espaço em branco nas pontas
    texto = str(texto).upper().strip()
    
    # Tirando todo os acentos das palavras ("NÃO" -> "NAO")
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    
    # Substituindo pontuações e caracteres especiais por espaço
    texto = re.sub(r'[^\w\s]', ' ', texto)
    
    substituicoes = {
        r'\bRUA\b': 'R',
        r'\bAVENIDA\b': 'AV',
        r'\bRODOVIA\b': 'ROD',
        r'\bTRAVESSA\b': 'TV',
        r'\bPRACA\b': 'PCA',
        r'\bSEM NUMERO\b': 'SN'
    }
    for padrao, substituto in substituicoes.items():
        texto = re.sub(padrao, substituto, texto)
        
    # Remoção de espaços duplos gerados pelas etapas anteriores
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def processar_cnefe():
    print("Extraindo endereços únicos do CNEFE com DuckDB...")
    
    # CONCAT_WS une as três colunas de logradouro ignorando os nulos.
    query = """
    SELECT DISTINCT
        CAST(COD_MUNICIPIO AS VARCHAR) AS municipio,
        CAST(CEP AS VARCHAR) AS cep,
        DSC_LOCALIDADE AS bairro,
        CONCAT_WS(' ', 
            NULLIF(TRIM(NOM_TIPO_SEGLOGR), ''), 
            NULLIF(TRIM(NOM_TITULO_SEGLOGR), ''), 
            NULLIF(TRIM(NOM_SEGLOGR), '')
        ) AS logradouro,
        CASE 
            WHEN DSC_MODIFICADOR = 'SN' THEN 'SN' 
            ELSE CAST(NUM_ENDERECO AS VARCHAR) 
        END AS numero,
        COD_SETOR AS setor_censitario
    FROM read_csv(
        'data/bronze/29_BA.csv', 
        sep=';', 
        header=True, 
        ignore_errors=True
    )
    """
    
    df_cnefe = duckdb.execute(query).df()
    
    print("Aplicando padronização de texto no CNEFE...")
    df_cnefe['logradouro_limpo'] = df_cnefe['logradouro'].apply(padronizar_texto)
    df_cnefe['bairro_limpo'] = df_cnefe['bairro'].apply(padronizar_texto)
    
    # Limpa números com notação decimal desnecessárias
    df_cnefe['numero'] = df_cnefe['numero'].fillna('SN').astype(str).str.replace(r'\.0$', '', regex=True)
    
    caminho_saida = 'data/silver/cnefe.parquet'
    df_cnefe.to_parquet(caminho_saida, index=False)
    print(f"CNEFE salvo em {caminho_saida} com {len(df_cnefe)} registros únicos.\n")

