# online-store-etl-sql-dwh
Building a SQL Server data warehouse for an online store using ETL and Bronze/Silver/Gold layers.

# 📌Project Overview

This project implements an end-to-end ETL pipeline for an online store, using SQL Server as a Data Warehouse and following the Medallion Architecture (Bronze, Silver, Gold).

The goal of the project is to ingest raw CRM and ERP source data, clean and standardize it, and transform it into an analytics-ready star schema (Fact & Dimension tables) suitable for BI and reporting.

# 🏗 Architecture

The project is built using the Medallion Architecture:

Bronze Layer (Raw / Staging)
Stores raw data ingested directly from CSV source files without transformations.

Silver Layer (Clean / Refined)
Applies data cleansing, deduplication, type casting, and business rules.

Gold Layer (Analytics / Data Warehouse)
Contains Fact and Dimension tables optimized for analytical queries and reporting.
