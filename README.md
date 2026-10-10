# 🌐 CNEFE-Geocoding-Pipeline

O **CNEFE-Geocoding-Pipeline** é uma solução de alta performance desenvolvida para realizar o vinculamento e a
geocodificação automatizada de bases de endereços em relação ao **CNEFE (Cadastro Nacional de Endereços para Fins Estatísticos - IBGE)** do Estado da Bahia.

Projetado sob os princípios da **Arquitetura Medalhão (Bronze, Silver e Gold)**, o sistema utiliza o **DuckDB** para consultas relacionais e leitura eficiente em streaming, o **Polars** para transformação
vetorizada em memória e o **RapidFuzz** para pareamento por similaridade (*fuzzy matching*). A pipeline também inclui um fluxo *Human-in-the-Loop*, facilitando a avaliação de registros ambíguos.

## Dependências Necessárias

* **Python 3.10+**
* **Polars:** Manipulação e transformação ultra-rápida de DataFrames em memória.
* **DuckDB:** Consultas SQL de alta performance e streaming direto sobre arquivos CSV e Parquet.
* **RapidFuzz:** Cálculo vetorizado de similaridade de strings e *fuzzy matching*.
* **PyArrow:** Interoperabilidade e conversão com cópia zero entre DuckDB e Polars.
* **Docker:** (Opcional) Para execução conteinerizada e reprodutível.

## Configuração da Pipeline

1. Baixe a base de referência do [CNEFE Bahia](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html?=&t=downloads) e envie o arquivo .csv até a pasta 'bronze'. Garanta que o nome do arquivo csv seja exatamente '29_BA.csv'. Se o nome for diferente, a pipeline não funcionará.

2. Baixe a [base de busca](https://fiocruzbr-my.sharepoint.com/:x:/g/personal/robespierre_pita_fiocruz_br/IQBk08ehW4aIRIjamLlzG_w1AbZuys15b1-WD5Q2l4ni3hw?e=MZ6mD5) e também envie o arquivo .csv à pasta 'bronze'. Garanta que o nome do arquivo csv seja exatamente '2026_teste_tecnico_relinke_cidacs(in).csv'.

## Configurações Gerais

Os códigos desse programa exigem Python3 e é recomendado o uso de um ambiente virtual (virtualenv).

Para criar o ambiente virtual, vá até a pasta raiz do programa em seu terminal e digite o seguinte comando:

```
python3 -m venv .venv
```

Em seguida, para configurar o ambiente virtual e posteriormente rodar os códigos, execute:

```
source venv/bin/activate           # Ativa o ambiente virtual
pip3 install -r requirements.txt  # Instala as dependencias
```
Caso queira sair do ambiente virtual, basta digitar o comando abaixo no terminal. Caso queira executar códigos novamente, bastará ativar o ambiente virtual seguindo a instrução acima.

```
deactivate
```

## Execução

Para executar a pipeline diretamente com o Python, digite o comando abaixo:

```
python3 src/main.py
```

Para executar a pipeline usando o container Docker, siga as próximas instruções:

```
docker build -t pipeline-cnefe .
```

Se usar Linux/MacOS:

```
docker run --rm -v $(pwd)/data:/app/data pipeline-cnefe
```

Se usar Windows:

```
docker run --rm -v ${PWD}/data:/app/data pipeline-cnefe
```

## Funcionamento da Pipeline

A arquitetura do projeto foi desenhada utilizando o conceito da **Arquitetura Medalhão** (Bronze, Silver, Gold), garantindo a evolução progressiva da qualidade e integridade dos dados. O fluxo ocorre em três etapas principais:

1. **Pré-processamento (Bronze -> Silver):**

   * Os arquivos brutos (CNEFE da Bahia e a base de busca) são lidos. O DuckDB é utilizado para a leitura e formatação SQL inicial, enquanto o Polars realiza as transformações em memória.

   * Aplica-se um pipeline de higienização de strings: conversão para maiúsculas, remoção de acentos, pontuações e padronização de prefixos urbanos (ex: "RUA" para "R", "SEM NUMERO" para "SN").

   * Os arquivos processados são convertidos e exportados no formato `.parquet` para garantir compressão e interoperabilidade rápida.

2. **Match Exato (Silver -> Gold):**

   * Uma query via DuckDB cruza as bases exigindo igualdade total entre Município, Logradouro, Número e Complemento.

   * Os registros encontrados recebem score `100.0`, status `EXATO` e são separados da fila de processamento.

3. **Fuzzy Matching e Curadoria (Silver -> Gold):**

   * Endereços pendentes passam pelo RapidFuzz, aplicado de forma vetorizada via UDF (User Defined Function) no Polars.

   * A busca é restrita ao dicionário do município do registro, otimizando o tempo de processamento.

   * **Critérios de Scoring:**

     * **>= 85.0 (`ALTA_CONFIANCA`):** Incorporados à base final automaticamente.

     * **65.0 a 84.9 (`AMBIGUO`):** O setor fica em branco na base principal. A pipeline extrai os Top 3 candidatos possíveis e exporta para o arquivo `revisao_ambiguo.csv`, destinado à revisão e curadoria manual.

     * **< 65.0 (`NAO_ENCONTRADO`):** Classificados como não localizados.

   * No fim da pipeline, na camada Gold, será enviado o arquivo `revisao_final.csv` contendo todos os registros
    da base de busca consolidados com seus respectivos setores censitários identificados, scores de
   similaridade e status do matching (EXATO, ALTA_CONFIANCA, AMBIGUO ou NAO_ENCONTRADO), além do arquivo
   auxiliar `revisao_ambiguo.csv` com as sugestões Top 3 para curadoria humana dos casos indeterminados.

## Premissas

* **Tipagem Estrita de Setores:** Considerou-se que os códigos numéricos de setores censitários do IBGE (15 dígitos) podem conter sufixos alfabéticos em bases preliminares (como a letra "P"). Portanto, a coluna `COD_SETOR` é tratada estritamente como texto (`String`).

* **Limitação de Memória:** Assumiu-se que o cruzamento de bases massivas (todo o Estado da Bahia) excede a capacidade da RAM comum. Por isso, evitou-se o uso do Pandas tradicional, substituindo-o por Polars e DuckDB.

* **Qualidade acima da Automação:** Partiu-se do princípio de que falsos positivos são inaceitáveis para análises socioeconômicas. A automação prefere gerar um alerta de ambiguidade a forçar uma dedução incorreta de setor censitário.


## Abordagens Utilizadas

* **Processamento Híbrido (DuckDB + Polars):** Utilização do DuckDB para leitura e SQL e do Polars para processamento em memória ultra-rápido, compartilhando dados via PyArrow com cópia zero.

* **Busca Heurística Particionada:** A base do CNEFE é convertida em dicionários aninhados em Python agrupados por município. Isso garante que o RapidFuzz não compare ruas de Salvador com endereços de Barreiras, economizando CPU.

* **Weighted Ratio no RapidFuzz:** WRatio (Weighted Ratio) no RapidFuzz:** O algoritmo `fuzz.WRatio` foi
adotado por ser uma abordagem híbrida que combina diferentes estratégias de comparação (como
correspondência exata, parcial e ordenação de palavras), ajustando pesos conforme a diferença de comprimento
entre as strings. Essa flexibilidade é ideal para lidar com a volatilidade do preenchimento manual de
endereços, que frequentemente apresenta abreviações, omissão de termos secundários ou variações na ordem das
palavras (ex: *"Residencial Parque das Flores"* vs *"Parque das Flores"*).

* **Human-in-the-Loop:** Adoção de um fluxo onde o algoritmo auxilia o humano em vez de substituí-lo em casos de dúvida. A exportação dos Top 3 candidatos transforma um trabalho de pesquisa demorado em uma simples tomada de decisão.


## Limitações 

* **Omissões no CNEFE:** O cruzamento está limitado à malha territorial do IBGE. Se o CNEFE não possuir o setor censitário para aquele logradouro/número específico (loteamentos muito recentes ou áreas irregulares), o pipeline não terá como preencher o dado.

* **Busca Restrita ao Município:** Endereços em bairros limítrofes que foram cadastrados com o município errado na base de busca não serão localizados no CNEFE, pois a partição do dicionário bloqueia a varredura intermunicipal.

* **Escopo do Fuzzy Match:** Atualmente a similaridade resolve a ambiguidade apenas pelo logradouro. Não há, na versão presente, desempate automatizado ponderando similaridade de bairro ou CEP para a faixa de scores intermediários.
