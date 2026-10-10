import re
import unicodedata
from pathlib import Path

import duckdb
import polars as pl

Path("data/silver").mkdir(parents=True, exist_ok=True)

# Pré-compilação

REGEX_NONE = re.compile(r"\bNONE\b")
REGEX_PONTUACAO = re.compile(r'[^\w\s]')
REGEX_ESPACOS = re.compile(r'\s+')

SUBSTITUICOES = [
    (re.compile(r"^RUA\b"), "R"),
    (re.compile(r"^AVENIDA\b"), "AV"),
    (re.compile(r"^RODOVIA\b"), "ROD"),
    (re.compile(r"^TRAVESSA\b"), "TV"),
    (re.compile(r"^PRACA\b"), "PCA"),
    (re.compile(r"\bSEM NUMERO\b"), "SN"),
    (re.compile(r"\bS N\b"), "SN"),
    (re.compile(r"\bS/N\b"), "SN"),
]

def padronizar_texto(texto):

    # Troca espaço vazio por uma string vazia
    if texto is None:
        return ""
    
    # Deixando todo o texto em caixa alta e remove espaço em branco nas pontas
    texto = str(texto).upper().strip()

    # Remove o 'NONE' de dentro das células
    texto = REGEX_NONE.sub("", texto)
    
    # Tirando todo os acentos das palavras ("NÃO" -> "NAO", "PÓ" -> "PO")
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    
    # Substituindo pontuações e caracteres especiais por espaço
    texto = REGEX_PONTUACAO.sub(' ', texto)
    
    for padrao, substituto in SUBSTITUICOES:
        texto = padrao.sub(substituto, texto)
        
    # Remoção de espaços duplos gerados pelas etapas anteriores
    texto = REGEX_ESPACOS.sub(' ', texto).strip()
    return texto

def processar_cnefe():
    
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
    
    df_cnefe = duckdb.execute(query).pl()

    df_cnefe = df_cnefe.with_columns(
       pl.col("logradouro").map_elements(padronizar_texto, return_dtype=pl.String).alias("logradouro_limpo"),
       pl.col("bairro").map_elements(padronizar_texto, return_dtype=pl.String).alias("bairro_limpo"),
       pl.col("complemento").map_elements(padronizar_texto, return_dtype=pl.String).alias("complemento_limpo")
    )
    
    # Limpa números com notação decimal desnecessárias

    df_cnefe = df_cnefe.with_columns(pl.col("numero").fill_null("SN").str.replace(r"\.0$", ""))

    caminho_saida = 'data/silver/cnefe.parquet'
    df_cnefe.write_parquet('data/silver/cnefe.parquet')
    print(f"CNEFE salvo em {caminho_saida} com sucesso.")

def processar_busca():

  caminho_busca = "data/bronze/2026_teste_tecnico_relinke_cidacs(in).csv"
  df_busca = pl.read_csv(caminho_busca, separator=';', infer_schema_length=0)

  df_busca = df_busca.with_columns(
         pl.col("consulta_logradouro").map_elements(padronizar_texto, return_dtype=pl.String).alias("logradouro_limpo"),
         pl.col("consulta_bairro").map_elements(padronizar_texto, return_dtype=pl.String).alias("bairro_limpo"),
         pl.col("consulta_complemento").map_elements(padronizar_texto, return_dtype=pl.String).alias("complemento_limpo")
      )

  # Tratamento do campo número 

  df_busca = df_busca.with_columns(pl.col("consulta_numero")
                                   .fill_null("SN")
                                   .map_elements(padronizar_texto,return_dtype=pl.String)
                                   .alias("numero")
                                   )

  df_busca = df_busca.with_columns(
        pl.col("numero").replace({"": "SN", "NONE": "SN"})
    )

  # Padronizando o CEP: SUbstitui espaços vazios por "" e tira tudo que não é um número
  # Exemplo: ("01310-100" -> "01310100")
  df_busca = df_busca.with_columns(pl.col("consulta_cep")
                                   .fill_null("")
                                   .str.replace_all(r"\D", "")
                                   .str.zfill(8)
                                   .alias("cep")
                                   )

  df_busca = df_busca.with_columns(pl.col("consulta_municipio")
                                   .cast(pl.String)
                                   .str.strip_chars()
                                   .str.slice(0, 6)
                                   .alias("municipio")
                                   )

  df_busca.write_parquet("data/silver/busca.parquet")
  print("Base de busca salva em data/silver/busca.parquet com sucesso.")


if __name__ == "__main__":
  processar_cnefe()
  processar_busca()

