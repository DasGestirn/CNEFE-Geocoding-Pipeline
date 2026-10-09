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

    df_resultado = df_resultado.with_columns([
        pl.col("score_similaridade").cast(pl.Float64),
        pl.col("setor_censitario_encontrado").cast(pl.String)
    ])
    
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

        registros_ambiguos = []

        def aplicar_fuzzy(row):
            mun = row["municipio"]
            logr_busca = row["logradouro_limpo"]
            
            opcoes = dict_candidatos.get(mun)
            
            if not opcoes:
                return {
                    "setor_censitario_encontrado_novo": None, 
                    "score_similaridade_novo": 0.0, 
                    "status_match_novo": "NAO_ENCONTRADO_MUN"
                }
                
            matches = process.extract(
                str(logr_busca), 
                opcoes.keys(),
                scorer=fuzz.WRatio,
                limit = 3, 
            )
            
            if matches:
                melhor_match = matches[0]
                logr_top1, score_top1 = melhor_match[0], round(float(melhor_match[1]), 2)
                
                if score_top1 >= 85.0:
                    setor = str(opcoes[logr_top1]) 
                    return {
                        "setor_censitario_encontrado_novo": setor, 
                        "score_similaridade_novo": score_top1, 
                        "status_match_novo": "ALTA_CONFIANCA"
                    }
                elif score_top1 >= 65.0:
                    # Criando csv com resultados ambíguos (65 <= score <= 84.9) para revisão manual
                    sugestao = {
                        "municipio": mun,
                        "logradouro": logr_busca,
                        "score_top1": score_top1,
                        "candidato_1": logr_top1,
                        "setor_1": opcoes[logr_top1]
                    }
                    if (len(matches) > 1):
                        sugestao["candidato_2"] = matches[1][0]
                        sugestao["score_2"] = round(float(matches[1][1]), 2)
                        sugestao["setor_2"] = opcoes[matches[1][0]]
                    if (len(matches) > 2):
                        sugestao["candidato_3"] = matches[2][0]
                        sugestao["score_3"] = round(float(matches[2][1]), 2)
                        sugestao["setor_3"] = opcoes[matches[2][0]]
                    registros_ambiguos.append(sugestao)

                else:
                    return {
                        "setor_censitario_encontrado_novo": None, 
                        "score_similaridade_novo": score_top1, 
                        "status_match_novo": "NAO_ENCONTRADO"
                    }
            
            return {
                "setor_censitario_encontrado_novo": None, 
                "score_similaridade_novo": 0.0, 
                "status_match_novo": "ERRO_FUZZY"
            }

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

    if registros_ambiguos:
        df_revisao = pl.DataFrame(registros_ambiguos)
        caminho_revisao = "data/gold/revisao_ambiguos.csv"
        df_revisao.write_csv(caminho_revisao, separator=';')
        print(f"[{df_revisao.height}] casos ambíguos salvos em: {caminho_revisao} para revisão manual.")

if __name__ == "__main__":
    executar_matching()