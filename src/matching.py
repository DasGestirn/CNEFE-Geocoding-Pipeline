from pathlib import Path

import duckdb
import polars as pl
from rapidfuzz import fuzz, process

Path("data/gold").mkdir(parents=True, exist_ok=True)

def executar_matching():
    
    query_exata = """
    SELECT 
        b.*, 
        c.setor_censitario AS setor_censitario_encontrado,
        CASE WHEN c.setor_censitario IS NOT NULL THEN 100.0 ELSE 0.0 END AS score_similaridade,
        CASE WHEN c.setor_censitario IS NOT NULL THEN 'EXATO' ELSE 'PENDENTE' END AS status_match
    FROM read_parquet('data/silver/busca.parquet') b
    LEFT JOIN (
        SELECT 
            municipio, logradouro_limpo, numero, complemento_limpo,
            MIN(setor_censitario) AS setor_censitario 
        FROM read_parquet('data/silver/cnefe.parquet')
        GROUP BY municipio, logradouro_limpo, numero, complemento_limpo
    ) c
        ON b.municipio = c.municipio 
       AND b.logradouro_limpo = c.logradouro_limpo 
       AND b.numero = c.numero
       AND b.complemento_limpo = c.complemento_limpo
    """
    
    df_resultado = duckdb.execute(query_exata).pl()
    
    df_exatos = df_resultado.filter(pl.col("status_match") == "EXATO")
    df_pendentes = df_resultado.filter(pl.col("status_match") == "PENDENTE")
    
    if not df_pendentes.is_empty():
        print(f"[{df_pendentes.height}] endereços pendentes. Iniciando Fuzzy Match (RapidFuzz)...")
        
        query_cnefe_fuzzy = """
        SELECT 
            municipio, logradouro_limpo, 
            MIN(CAST(setor_censitario AS VARCHAR)) AS setor_censitario 
        FROM read_parquet('data/silver/cnefe.parquet')
        WHERE logradouro_limpo IS NOT NULL
        GROUP BY municipio, logradouro_limpo
        """
        df_cnefe_fuzzy = duckdb.execute(query_cnefe_fuzzy).pl()

        dict_candidatos = {}
        for row in df_cnefe_fuzzy.iter_rows(named=True):
            mun = row["municipio"]
            if mun not in dict_candidatos:
                dict_candidatos[mun] = {}
            dict_candidatos[mun][row["logradouro_limpo"]] = row["setor_censitario"]

        def aplicar_fuzzy(row):
            mun = row["municipio"]
            logr_busca = row["logradouro_limpo"]
            
            opcoes = dict_candidatos.get(mun)
            
            if not opcoes:
                return (None, 0.0, "NAO_ENCONTRADO_MUN")
                
            # opcoes.keys() passa apenas os logradouros 
            match = process.extractOne(
                str(logr_busca), 
                opcoes.keys(), 
                scorer=fuzz.token_sort_ratio
            )
            
            if match:
                logr_encontrado = match[0]
                score = float(match[1])
                
                if score >= 85.0:
                    setor = opcoes[logr_encontrado]
                    return (setor, score, "ALTA_CONFIANCA")
                elif score >= 65.0:
                    return (None, score, "AMBIGUO")
                else:
                    return (None, score, "NAO_ENCONTRADO")
            
            return (None, 0.0, "ERRO_FUZZY")

        df_pendentes = (
            df_pendentes.with_columns(
                pl.struct(["municipio", "logradouro_limpo"])
                .map_elements(
                    aplicar_fuzzy, 
                    return_dtype=pl.Struct([
                        pl.Field("setor_censitario_encontrado_novo", pl.String),
                        pl.Field("score_similaridade_novo", pl.Float64),
                        pl.Field("status_match_novo", pl.String)
                    ])
                )
                .alias("fuzzy_result")
            )
            .unnest("fuzzy_result") # Expande a struct em 3 colunas
            .with_columns([
                pl.col("setor_censitario_encontrado_novo").alias("setor_censitario_encontrado"),
                pl.col("score_similaridade_novo").alias("score_similaridade"),
                pl.col("status_match_novo").alias("status_match")
            ])
            .drop(["setor_censitario_encontrado_novo", "score_similaridade_novo", "status_match_novo"])
        )
        
        df_resultado = pl.concat([df_exatos, df_pendentes])

    caminho_saida = "data/gold/resultado_final.csv"
    df_resultado.write_csv(caminho_saida, separator=';')
    
    print(f"\nPipeline de matching concluída! Arquivo gerado em: {caminho_saida}")
    print("\n--- RESUMO DA EXECUÇÃO ---")
    print(df_resultado['status_match'].value_counts())
    print("--------------------------\n")

if __name__ == "__main__":
    executar_matching()