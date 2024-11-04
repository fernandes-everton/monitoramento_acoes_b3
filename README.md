# Monitoramento e Análise de Ações na B3 (Bolsa de Valores Brasileira)

Este projeto contém dois scripts principais para **análise de ações** e **monitoramento via Telegram** de ações listadas na B3. 

1. **Acompanhamento de Ações na B3**: Permite a análise detalhada das cotações, médias móveis, e dividendos das ações selecionadas.
2. **Monitoramento de Ações Via Telegram**: Envia notificações para um grupo ou usuário no Telegram sempre que o preço de uma ação atinge um gatilho pré-definido, tanto em variação percentual quanto em valor.

## Índice

- [Requisitos de Instalação](#requisitos-de-instalação)
- [Estrutura do Projeto](#estrutura-do-projeto)
  - [1. Acompanhamento de Ações na B3](#1-acompanhamento-de-ações-na-b3)
    - [Dependências](#dependências)
    - [Importação de Bibliotecas](#importação-de-bibliotecas)
    - [Configuração de Widgets e Variáveis](#configuração-de-widgets-e-variáveis)
    - [Coleta de Preços das Ações e Cálculo de Médias Móveis](#coleta-de-preços-das-ações-e-cálculo-de-médias-móveis)
    - [Análise SQL das Cotações](#análise-sql-das-cotações)
    - [Exemplos de Consultas SQL para Análise](#exemplos-de-consultas-sql-para-análise)
  - [2. Monitoramento de Ações Via Telegram](#2-monitoramento-de-ações-via-telegram)
    - [Configuração do Bot](#configuração-do-bot)
    - [Gatilhos de Monitoramento](#gatilhos-de-monitoramento)
    - [Funções de Monitoramento e Envio de Mensagens](#funções-de-monitoramento-e-envio-de-mensagens)
    - [Exemplo de Código para Monitoramento](#exemplo-de-código-para-monitoramento)
- [Exemplo de Uso](#exemplo-de-uso)
- [Considerações Finais](#considerações-finais)

## Requisitos de Instalação

Antes de executar o código, instale as dependências necessárias:

```bash
!pip install yfinance
!pip install requests
```

## Estrutura do Projeto

### 1. Acompanhamento de Ações na B3

Este script foi desenvolvido em Jupyter Notebook no **Databricks Community** e está dividido em células com a seguinte estrutura:

#### Dependências

```python
!pip install yfinance
```

#### Importação de Bibliotecas

```python
import yfinance as yf
import pandas as pd
from datetime import datetime
import pyspark
from pyspark.sql import DataFrame, SparkSession
```

#### Configuração de Widgets e Variáveis

- Criação de widgets no Databricks para entrada de dados do usuário: 
  - **Código das Ações**: múltiplos códigos separados por vírgula (ex: `BBAS3.SA,ITUB4.SA`).
  - **Data Inicial** e **Data Final**: Período de análise.

#### Coleta de Preços das Ações e Cálculo de Médias Móveis

1. Para cada ação inserida, são coletadas as cotações diárias.
2. Médias móveis são calculadas para períodos de 15, 30, 60, 90 e 120 dias.
3. Os dados são salvos em um DataFrame PySpark para permitir consultas SQL.

#### Análise SQL das Cotações

Várias consultas SQL foram criadas para analisar o desempenho diário, lucro anual, variação diária e resumo de lucros/perdas. Algumas das principais análises incluem:

- **Variação Diária e Resumo Anual**
- **Volume de Negociações**
- **Dividend Yield e Análise de Dividendos**

#### Exemplos de Consultas SQL para Análise

Abaixo estão alguns exemplos de consultas:

```sql
-- Desempenho Diário das Ações
SELECT
  data_negociacao, 
  codigo, 
  abertura, 
  fechamento, 
  maior_preco AS maxima,
  menor_preco AS minima,
  ROUND(diff, 2) AS diff
FROM acoes_analise
ORDER BY data_negociacao DESC;
```

```sql
-- Cotação YTD (Cotação no Ano até o Momento)
SELECT
  data_negociacao, 
  month(data_negociacao) as mes, 
  codigo, 
  abertura, 
  fechamento
FROM acoes_analise
WHERE year(data_negociacao) = year(current_date())
ORDER BY data_negociacao;
```

```sql
-- Resumo de Dividendos do Ano Anterior e do Ano Atual
SELECT
  codigo,
  ROUND(SUM(CASE WHEN YEAR(data_negociacao) = YEAR(CURRENT_DATE()) THEN dividendos ELSE 0 END), 2) AS total_dividendos_ano_atual,
  ROUND(SUM(CASE WHEN YEAR(data_negociacao) = YEAR(CURRENT_DATE()) - 1 THEN dividendos ELSE 0 END), 2) AS total_dividendos_ultimo_ano
FROM df_dividendos
GROUP BY codigo;
```

### 2. Monitoramento de Ações Via Telegram

Este script monitora as variações de preço das ações e envia alertas via Telegram para gatilhos definidos.

#### Configuração do Bot

1. Configure o bot no Telegram e obtenha o token de acesso.
2. Substitua `<SEU_TOKEN>` e `<SEU_CHAT_ID>` pelos valores do seu bot e chat.

#### Gatilhos de Monitoramento

Cada ação possui quatro gatilhos configuráveis:
- **Percentual de alta/baixa**: aciona o alerta se o preço variar uma certa porcentagem.
- **Valor de alta/baixa**: aciona o alerta se o preço variar um valor específico.

#### Funções de Monitoramento e Envio de Mensagens

- `get_ticket`: Obtem os valores de abertura, fechamento, variação e porcentagem de variação da última cotação.
- `verificar_alertas`: Verifica se os valores da ação ativam os gatilhos definidos.
- `send_message_telegram`: Envia uma mensagem ao Telegram quando um gatilho é ativado.

#### Exemplo de Código para Monitoramento

```python
# Exemplo de configuração do alerta
config = [{
    "acao" : "PETR4.SA",
    "percent_caiu" : 3,
    "percent_subiu" : 5,
    "vl_caiu" : 0.5,
    "vl_subiu" : 0.7,
    "ids_chat" : [<SEU_CHAT_ID>]
}]
```

---

## Exemplo de Uso

1. **Acompanhamento de Ações na B3**: Execute o notebook no Databricks e insira o código das ações, data inicial e data final nos widgets para realizar a análise desejada.
2. **Dashboard de Análise**: Visualize os dados em um dashboard interativo criado a partir dos dados obtidos. [Clique aqui para acessar um exemplo de Dashboard](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4212551225649944/921148053696170/3959289706457058/latest.html).
3. **Monitoramento de Ações Via Telegram**: Execute o script com as configurações dos gatilhos e o bot enviará notificações ao Telegram quando as condições forem atendidas.

## Considerações Finais

Esses scripts foram desenvolvidos para facilitar a análise e o monitoramento automatizado de ações da B3. Sinta-se à vontade para ajustar as configurações de gatilhos, cotações e notificações conforme necessário.

---

**Nota**: Este projeto usa o `yfinance` para acessar dados financeiros e `requests` para envio de mensagens via Telegram. Certifique-se de que as bibliotecas estejam atualizadas e o token do Telegram esteja corretamente configurado.
