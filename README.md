# Data Engineering Portfolio

Azure-based data engineering projects demonstrating end-to-end 
pipeline design, data modeling, and cloud data infrastructure.

## Tech Stack
![Azure](https://img.shields.io/badge/Azure-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=mysql&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![ADF](https://img.shields.io/badge/Azure_Data_Factory-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![ADLS](https://img.shields.io/badge/ADLS_Gen2-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![Synapse](https://img.shields.io/badge/Azure_Synapse-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=power-bi&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

## Projects

### NYC Taxi Medallion Lakehouse *(in progress)*
End-to-end batch pipeline ingesting NYC TLC yellow taxi data 
through a medallion architecture on Azure.

**Pipeline:**
ADF → ADLS Gen2 (Bronze) → Databricks + Delta Lake (Silver) → Synapse (Gold) → Power BI

**Built so far:**
- ADF Copy pipeline with daily schedule trigger
- ADLS Gen2 medallion structure (bronze/silver/gold)
- Python ETL — modular ingestion + dimension loading
- Delta Lake — time travel, schema evolution, OPTIMIZE + Z-ordering
- SCD Type 2 — customer history tracking via Delta MERGE
- Star schema — 7 dimension tables + fact table in MySQL

![Architecture](screenshots/architecture_diagram.png)

[View code →](./python-etl/)

### Data Warehouse Design
Star schema designs for food delivery and NYC taxi domains.
Demonstrates dimensional modeling, grain declaration, SCD types,
and role-playing dimensions.

[View →](./data-warehouse-design/)

## About
Software Engineer (4 yrs) transitioning to Data Engineering.
Building production-style Azure data pipelines on the medallion
lakehouse architecture.

📍 Gurugram, India