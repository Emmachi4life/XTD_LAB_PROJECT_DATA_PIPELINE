# XTD Labs — UK Carbon Intensity Data Pipeline

## Overview
End-to-end data engineering pipeline analyzing UK regional carbon 
intensity data across 2022-2024.

## Tech Stack
- Python (asyncio, aiohttp) — async data extraction
- PySpark — distributed data transformation  
- PostgreSQL 18 — star schema data warehouse
- Power BI — 3-page interactive dashboard
- Git & GitHub — version control

## Pipeline Architecture
NESO API → Bronze (JSON) → Silver (PySpark) → Gold → PostgreSQL → Power BI

## Key Numbers
- 53,594 raw records extracted
- 19,728 daily records after transformation
- 4-table star schema in PostgreSQL 18
- 3-page Power BI dashboard

## Live Dashboard
Dashboard link for live view(your Power BI link here)



## Setup Instructions
1. Clone the repo
2. Create virtual environment and install dependencies:
   pip install -r requirements.txt
3. Create .env file with PostgreSQL credentials
4. Run schema setup: python create_schema.py
5. Run pipeline: python transform_load.py

## Project Structure
- extract.py — Bronze layer extraction
- transform_load.py — Silver/Gold/PostgreSQL pipeline
- schema.sql — Star schema SQL
- create_schema.py — Schema creation script
- powerbi/ — Dashboard files and screenshots