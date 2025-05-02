# Social Media Crypto Audit - Airflow Data Pipeline
The goal of this project is to audit cryptocurrency influencers by setting up a data pipeline to determine their overall P/L of promoted cryptocurrencies, hopefully giving more insight on which influencers can be trusted.

## Tech Stack
- [Python](https://www.python.org/) - Used to write Airflow DAGs and transformation logic
- [Apache Airflow](https://airflow.apache.org/) - Orchestrate ETL workflows for scraping and transforming data
- [Docker](https://www.docker.com/) - Containerize all services for consistent and isolated development environments

## Environment Variables
The following environment variables are required for the app to function properly:

| Variable Name | Description |
|---------------|-------------|
| `AIRFLOW_UID` | Can manually set to default 50000 to get rid of AIRFLOW_UID is not set warning |

Create a `.env` file and define the variables.

## Installation

### Prequisites
Before you being, make sure you have the following installed:

- [Python](https://www.python.org/) - Used to write Airflow DAGs and transformation logic
- [Docker](https://www.docker.com/) - Containerize all services for consistent and isolated development environments

### Steps to Install
1. Clone Repository
```bash
git clone https://github.com/JustinDosaj/crypto-audit-airflow.git
```

2. Run Project
```bash
docker compose up
```

## Project Status
Airflow has been setup. Next step is to create the first DAG which will be responsible for the data ingestion workflow. This DAG should extract recent tweets that contain `$` from crypto influencers.

### General To-Do List
1. Build data pipeline to grab influencer data and posts containing promotions
2. Build data pipeline to calculate profit/loss—store data in database