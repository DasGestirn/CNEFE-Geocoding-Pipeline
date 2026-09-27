import sys
import time

from matching import executar_matching
from preprocessing import processar_busca, processar_cnefe


def main():
    print("="*55)
    print("INICIANDO A PIPELINE DE GEOCODING CNEFE")
    print("="*55)
    
    # Tempo que iniciou a execucação da pipeline
    start_time = time.time()

    try:
        print("\n[Etapa 1/3] Pré-processamento do CNEFE (Bronze -> Silver)")
        processar_cnefe()

        print("\n[Etapa 2/3] Pré-processamento da Base de Busca (Bronze -> Silver)")
        processar_busca()

        print("\n[Etapa 3/3] Cruzamento de Dados e Matching (Silver -> Gold)")
        executar_matching()

        # Cálculo de tempo de execucação do código
        end_time = time.time()
        tempo_total = end_time - start_time
        
        print("="*55)
        print(f"PIPELINE CONCLUÍDA EM {tempo_total:.2f} segundos.")
        print("="*55)

    except Exception as e:  # noqa: BLE001
        print("\n Erro durante a execução da pipeline!")
        print(f"Detalhes do Erro: {e}")
        # Interrompe o script com código de erro 
        sys.exit(1)

if __name__ == "__main__":
    main()