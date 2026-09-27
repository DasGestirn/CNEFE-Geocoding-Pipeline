import gc
import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

Path("data/silver").mkdir(parents=True, exist_ok=True)

# Pré-compilação

REGEX_NONE = re.compile(r"\bNONE\b")
REGEX_PONTUACAO = re.compile(r'[^\w\s]')
REGEX_ESPACOS = re.compile(r'\s+')

SUBSTITUICOES = [
    (re.compile(r"\bRUA\b"), "R"),
    (re.compile(r"\bAVENIDA\b"), "AV"),
    (re.compile(r"\bRODOVIA\b"), "ROD"),
    (re.compile(r"\bTRAVESSA\b"), "TV"),
    (re.compile(r"\bPRACA\b"), "PCA"),
    (re.compile(r"\bSEM NUMERO\b"), "SN"),
    (re.compile(r"\bS N\b"), "SN"),
    (re.compile(r"\bS/N\b"), "SN"),
]

def padronizar_texto(texto):

    # Troca espaço vazio por uma string vazia
    if pd.isna(texto) or texto is None:
        return ""
    
    # Deixando todo o texto em caixa alta e remove espaço em branco nas pontas
    texto = str(texto).upper().strip()

    # Remove o 'NONE' de dentro das células
    texto = REGEX_NONE.sub("", texto)
    
    # Tirando todo os acentos das palavras ("NÃO" -> "NAO")
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    
    # Substituindo pontuações e caracteres especiais por espaço
    texto = REGEX_PONTUACAO.sub(' ', texto)
    
    for padrao, substituto in SUBSTITUICOES.items():
        texto = re.sub(padrao, substituto, texto)
        
    # Remoção de espaços duplos gerados pelas etapas anteriores
    texto = REGEX_ESPACOS.sub(' ', texto).strip()
    return texto

def processar_cnefe():

    # TODO: Tem que otimizar essa query de alguma forma. A pipeline está travando na etapa 1.
    
    query = """
    SELECT DISTINCT
        SUBSTR(CAST(COD_MUNICIPIO AS VARCHAR), 1, 6) AS municipio,
        LPAD(CAST(CEP AS VARCHAR), 8, '0') AS cep,
        DSC_LOCALIDADE AS bairro,
        CONCAT_WS(' ', 
            NULLIF(TRIM(NOM_TIPO_SEGLOGR), ''), 
            NULLIF(TRIM(NOM_TITULO_SEGLOGR), ''), 
            NULLIF(TRIM(NOM_SEGLOGR), '')
        ) AS logradouro,
        CONCAT_WS(' ', 
            NULLIF(TRIM(NOM_COMP_ELEM1), ''), NULLIF(TRIM(VAL_COMP_ELEM1), ''),
            NULLIF(TRIM(NOM_COMP_ELEM2), ''), NULLIF(TRIM(VAL_COMP_ELEM2), ''),
            NULLIF(TRIM(NOM_COMP_ELEM3), ''), NULLIF(TRIM(VAL_COMP_ELEM3), '')
        ) AS complemento,
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
    
    df_cnefe['logradouro_limpo'] = df_cnefe['logradouro'].apply(padronizar_texto)
    gc.collect()

    df_cnefe['bairro_limpo'] = df_cnefe['bairro'].apply(padronizar_texto)
    gc.collect()

    df_cnefe["complemento_limpo"] = df_cnefe["complemento"].apply(padronizar_texto)
    gc.collect()
    
    # Limpa números com notação decimal desnecessárias
    df_cnefe['numero'] = df_cnefe['numero'].fillna('SN').astype(str).str.replace(r'\.0$', '', regex=True)
    
    caminho_saida = 'data/silver/cnefe.parquet'
    df_cnefe.to_parquet("data/silver/cnefe.parquet", index=False)
    print(f"CNEFE salvo em {caminho_saida} com sucesso.")

def processar_busca():

  caminho_busca = "data/bronze/2026_teste_tecnico_relinke_cidacs(in).csv"
  df_busca = pd.read_csv(caminho_busca, dtype=str)

  df_busca["logradouro_limpo"] = df_busca["consulta_logradouro"].apply(
      padronizar_texto
  )
  df_busca["bairro_limpo"] = df_busca["consulta_bairro"].apply(
      padronizar_texto
  )
  df_busca["complemento_limpo"] = df_busca["consulta_complemento"].apply(
      padronizar_texto
  )

  # Tratamento do campo número 
  df_busca["numero"] = (
      df_busca["consulta_numero"].fillna("SN").apply(padronizar_texto)
  )
  df_busca["numero"] = df_busca["numero"].replace(["", "NONE"], "SN")

  df_busca["cep"] = (
      df_busca["consulta_cep"]
      .fillna("")
      .str.replace(r"\D", "", regex=True)
      .str.zfill(8)
  )
  df_busca["municipio"] = (
      df_busca["consulta_municipio"].astype(str).str.strip().str[:6]
  )

  df_busca.to_parquet("data/silver/busca.parquet", index=False)
  print("Base de busca salva em data/silver/busca.parquet com sucesso.")


if __name__ == "__main__":
  processar_cnefe()
  processar_busca()

