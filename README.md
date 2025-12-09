# Online Shopping RFM Dashboard

> **⚠️ Project Status:**  
> This project is **NOT completed** yet. Development is **ongoing**, and features, structure, and documentation will be updated continuously.

## Description

This repository provides a data-driven dashboard to perform RFM (Recency, Frequency, Monetary) analysis on online shopping data.  
It includes scripts and notebooks for processing raw order data and generating RFM scores, and helps you segment customers and derive insights on customer behavior.

## Contents / Project Structure

| File / Folder | Description |
|--------------|-------------|
| `MonthlyRFM.py` | Python script to compute RFM metrics and segmentation. |
| `MonthlyRFM.ipynb` | Jupyter Notebook version — exploratory data analysis + visualization of RFM results. |
| `df_orders_sample.csv` | Sample orders data (input) for testing / demonstration. |
| `df_min_order_date_sample.csv` | Sample minimal order-date data (auxiliary sample data). |
| `dimdate5.parquet` | A date-dimension file (preprocessed / auxiliary) that might be used in date-based analysis. |
| … (other files) | Additional data or scripts as needed. |

## RFM Methodology (What the dashboard does)

- **Recency (R):** how recently a customer made a purchase.  
- **Frequency (F):** how often the customer purchases (number of orders).  
- **Monetary (M):** how much money a customer has spent in total or on average.  

Using these three indicators, customers are segmented into different RFM groups — e.g. “High value & recent & frequent”, “Churn risk”, etc.  
This helps in identifying loyal customers, potential churners, big spenders, and more.

## Features

- Data ingestion from CSV (or other formats).  
- Data preprocessing / cleaning.  
- Calculation of RFM scores per customer.  
- Segmentation logic based on R, F, M thresholds.  
- Notebook-based exploratory analysis (visualization, summary tables).  
- Easy to adapt to your own data (just replace sample CSV files).  

## Requirements / Prerequisites

- Python 3.x  
- Common data libraries such as `pandas`, `numpy` (add other dependencies if you used more)  
- Jupyter Notebook (for the `.ipynb` version)  

You can install dependencies via `pip`. Example:

```bash
pip install pandas numpy
