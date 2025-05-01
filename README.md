# Social Media Crypto Audit
The goal of this project is to audit cryptocurrency influencers by setting up a data pipeline to determine their overall P/L of promoted cryptocurrencies, hopefully giving more insight on which influencers can be trusted.

## Tech Stack
- [Python](https://www.python.org/) - Used to write Airflow DAGs and transformation logic
- [Apache Airflow](https://airflow.apache.org/) - Orchestrate ETL workflows for scraping and transforming data
- [Docker](https://www.docker.com/) - Containerize all services for consistent and isolated development environments
- [PostgreSQL](https://www.postgresql.org/) - SQL database for storing cleaned and structured data
- [Storage]()(Unknown) - Could use some data lake service or something like AWS S3

## Project Status
Project has only been initialized—first step will be to create data pipelines to get

### General To-Do List
1. Build data pipeline to grab influencer data and posts containing promotions
2. Build data pipeline to calculate profit/loss—store data in database
3. Setup RESTful API to securely query database
4. Build UI/UX for accessible searching