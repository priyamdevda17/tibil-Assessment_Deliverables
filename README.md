# E-Commerce Analytics Pipeline

## Overview
This project implements an AWS serverless analytics pipeline for daily e-commerce transactions:
- S3 (Raw CSV) → Lambda → Step Function → Glue → Parquet → Athena → QuickSight

## Components
1. **S3 Bucket:** `priyam-tibil-test` with `raw` and `curated` folders
2. **Lambda:** Checks CSV headers, triggers Step Function
3. **Glue Job:** Cleans and converts CSV to Parquet
4. **Step Function:** Orchestrates Lambda → Glue
5. **Athena:** Queries Parquet data
6. **QuickSight/QuickSuit:** Visualizes daily sales summary

## Deployment Steps
1. Upload CSV to `ecommerce-raw-data/`
2. Lambda automatically triggers Step Function
3. Glue job processes CSV → Parquet
4. Athena queries (optional) refresh partitions or compute summary
5. QuickSight connects to S3/Athena for visualization