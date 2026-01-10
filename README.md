# 🛣️ Pipeline de Dados PRF: Arquitetura de Medalhão com Docker & Postgres

Este projeto implementa um pipeline de dados (ETL) robusto para o processamento de dados históricos de acidentes em rodovias federais brasileira (PRF), cobrindo o período de **2005 a 2025**. O foco principal é a transformação de grandes volumes de dados brutos e inconsistentes em um **Data Warehouse** otimizado para consultas SQL e BI.

## 🏗️ Arquitetura do Projeto

O pipeline foi estruturado utilizando a **Medallion Architecture**, garantindo linhagem e qualidade dos dados em cada etapa do processo:

1.  **Bronze (Raw - Web Scraping):** Desenvolvi um extrator customizado em Python que realiza o **scraping** do portal de dados abertos da PRF. O script automatiza a navegação, identifica os links de download por ano e realiza a ingestão dos arquivos CSV brutos, eliminando o trabalho manual e garantindo a escalabilidade do pipeline.
2.  **Silver (Cleaned):** Limpeza profunda, normalização de encodings (Latin-1 para UTF-8), tratamento de tipos de data e padronização de colunas. Armazenamento eficiente em formato **Apache Parquet**.
3.  **Gold (Curated):** Unificação de mais de 20 anos de registros em uma única tabela mestra (Single Source of Truth), otimizada para análise volumétrica.
4.  **Load (Database):** Carga dos dados tratados em um banco de dados **PostgreSQL** orquestrado via **Docker**, permitindo acesso relacional performático.

## 🛠️ Tecnologias e Ferramentas

* **Web Scraping:** `requests` / `BeautifulSoup` (ou a biblioteca que usamos)
* **Linguagem:** Python 3.11
* **Processamento de Dados:** Pandas
* **Linguagem:** Python 3.11
* **Processamento de Dados:** Pandas
* **Armazenamento Colunar:** Apache Parquet (PyArrow)
* **Infraestrutura:** Docker & Docker Compose
* **Banco de Dados:** PostgreSQL 15
* **Conectividade:** SQLAlchemy & Psycopg2
* **Segurança:** Python-dotenv (Variáveis de ambiente)

## 📁 Estrutura do Repositório

```text
.
├── data/
│   ├── bronze/          # Arquivos CSV originais
│   ├── silver/          # Arquivos Parquet limpos por ano
│   └── gold/            # Parquet único e unificado
├── src/
│   ├── extraction/      # Script de download (Bronze)
│   ├── transformation/  # Scripts Silver e Gold
│   └── storage/         # Script de carga para Postgres
├── .env                 # Credenciais (não versionado)
├── docker-compose.yml   # Orquestração do banco de dados
└── requirements.txt     # Dependências do projeto
```

## 🚀 Como executar

### 1. Clonar e instalar

```
git clone [https://github.com/seu-usuario/et-prf.git](https://github.com/seu-usuario/et-prf.git)
cd et-prf
pip install -r requirements.txt
```

### 2. Configurar o Ambiente

Crie um arquivo .env na raiz:

```
POSTGRES_DB=db_acidentes
POSTGRES_USER=user_prf
POSTGRES_PASSWORD=sua_senha_segura
```

### 3. Subir a Infraestrutura
```
docker-compose up -d
```
### 4. Rodar o Pipeline

```
python src/extraction/extract_prf.py
python src/transformation/silver_transformation.py
python src/transformation/gold_unification.py
python src/transformation/load_to_postgres.py

```

##  📊 Resultados e Performance

- **Escalabilidade:** Processamento de mais de 2,1 milhões de registros.
- **Otimização:** Conversão de arquivos CSV pesados para Parquet, reduzindo o tempo de leitura e espaço em disco.
- **Integridade:** Tratamento de inconsistências históricas em esquemas de dados que mudaram ao longo de duas décadas.

## 👨‍💻 Sobre o Autor
Jackson Nascimento - Engenheiro de Dados em formação | BI | Analytics

Projeto desenvolvido com foco em aprendizado real de engenharia de dados, indo além de tutoriais e demonstrando capacidade de estruturar pipelines próximos ao cenário profissional.

🔗 LinkedIn: https://www.linkedin.com/in/jackson10/