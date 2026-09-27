from pathlib import Path

import duckdb
import pandas as pd
from rapidfuzz import fuzz, process

Path("data/gold").mkdir(parents=True, exist_ok=True)

def executar_matching():
    
    query_exata = """
    SELECT 
        b.*, 
        c.setor_censitario AS setor_censitario_encontrado,
        CASE WHEN c.setor_censitario IS NOT NULL THEN 100 ELSE 0 END AS score_similaridade,
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
    
    df_resultado = duckdb.execute(query_exata).df()
    
    mask_pendentes = df_resultado['status_match'] == 'PENDENTE'
    df_pendentes = df_resultado[mask_pendentes]
    
    if not df_pendentes.empty:
        print(f"[{len(df_pendentes)}] endereços pendentes. Iniciando Fuzzy Match (RapidFuzz)...")
        
        query_cnefe_fuzzy = """
        SELECT 
            municipio, logradouro_limpo, 
            MIN(setor_censitario) AS setor_censitario 
        FROM read_parquet('data/silver/cnefe.parquet')
        GROUP BY municipio, logradouro_limpo
        """
        df_cnefe_fuzzy = duckdb.execute(query_cnefe_fuzzy).df()

        def aplicar_fuzzy(row):

            candidatos = df_cnefe_fuzzy[df_cnefe_fuzzy['municipio'] == row['municipio']]
            
            if candidatos.empty:
                return pd.Series([None, 0, 'NAO_ENCONTRADO_MUN'])
                
            opcoes = candidatos['logradouro_limpo'].dropna().to_dict()
            if not opcoes:
                return pd.Series([None, 0, 'NAO_ENCONTRADO_LOGR'])
                
            busca_texto = str(row['logradouro_limpo'])
            
            # extractOne retorna (texto_encontrado, score, chave_do_dicionario)
            match = process.extractOne(
                busca_texto, 
                opcoes, 
                scorer=fuzz.token_sort_ratio
            )
            
            if match:
                score = match[1]
                idx_original = match[2]
                setor = candidatos.loc[idx_original, 'setor_censitario']
                
                if score >= 85:
                    return pd.Series([setor, score, 'ALTA_CONFIANCA'])
                elif score >= 65:
                    return pd.Series([None, score, 'AMBIGUO'])
                else:
                    return pd.Series([None, score, 'NAO_ENCONTRADO'])
            
            return pd.Series([None, 0, 'ERRO_FUZZY'])

        resultados_fuzzy = df_pendentes.apply(aplicar_fuzzy, axis=1)
        resultados_fuzzy.columns = ['setor_censitario_encontrado', 'score_similaridade', 'status_match']
        
        df_resultado.loc[mask_pendentes, ['setor_censitario_encontrado', 'score_similaridade', 'status_match']] = resultados_fuzzy.values

    caminho_saida = "data/gold/resultado_final.csv"
    df_resultado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
    
    print(f"\nPipeline de matching concluída! Arquivo gerado em: {caminho_saida}")
    print("\n--- RESUMO DA EXECUÇÃO ---")
    print(df_resultado['status_match'].value_counts())
    print("--------------------------\n")

if __name__ == "__main__":
    executar_matching()