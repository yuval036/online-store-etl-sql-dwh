# online-store-etl-sql-dwh
Building a SQL Server data warehouse for an online store using ETL and Bronze/Silver/Gold layers.

## 📌Project Overview
This project implements an end-to-end ETL pipeline for an online store, using SQL Server as a Data Warehouse and following the Medallion Architecture (Bronze, Silver, Gold).

The goal of the project is to ingest raw CRM and ERP source data, clean and standardize it, and transform it into an analytics-ready star schema (Fact & Dimension tables) suitable for BI and reporting.

## 🏗 Architecture
The project is built using the Medallion Architecture:

### Bronze Layer (Raw / Staging)
Stores raw data ingested directly from CSV source files without transformations.

### Silver Layer (Clean / Refined)
Applies data cleansing, deduplication, type casting, and business rules.

### Gold Layer (Analytics / Data Warehouse)
Contains Fact and Dimension tables optimized for analytical queries and reporting.

## 🚀 How to Run

1.Install SQL Server Developer Edition
2.Install SQL Server Management Studio (SSMS)
3.Create the database and schemas (bronze, silver, gold)
4.Run the Bronze table creation scripts
5.Execute the Bronze load stored procedures
6.Run Silver transformation scripts
7.Build Gold fact and dimension tables

## 📌 Project Purpose
This project was built as a portfolio project to demonstrate practical skills in:
Data Engineering
ETL development
SQL-based Data Warehousing
Real-world data modeling patterns

