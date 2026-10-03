# CNEFE-Geocoding-Pipeline


## Como Executar


1. Baixe a base de referência do [CNEFE Bahia](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html?=&t=downloads) e envie o arquivo .csv até a pasta 'bronze'.
2. Baixe a [base de busca](https://fiocruzbr-my.sharepoint.com/:x:/g/personal/robespierre_pita_fiocruz_br/IQBk08ehW4aIRIjamLlzG_w1AbZuys15b1-WD5Q2l4ni3hw?e=MZ6mD5) e também envie o arquivo .csv à pasta 'bronze'.
3. Instale as dependências do projeto:
```
pip3 install -r requirements.txt
```
4. Para executar a pipeline diretamente com o Python, digite o comando abaixo:

```
python3 src/main.py
```