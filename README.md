# 🏗️ Data Warehouse Project  

This project demonstrates the implementation of a **Data Warehouse** using the **Medallion Architecture (Bronze, Silver, Gold layers)**. It covers ingestion of raw data, data cleansing and transformation, and modeling into a **Star Schema** to enable business intelligence and analytics.  

---

## 📐 Architecture  

The overall solution is designed using the **Medallion Architecture**.  

Bronze Layer (Raw Zone):
- Ingests raw CSV files from multiple source systems (CRM, ERP) into staging tables. 
- Implements full load strategy (TRUNCATE + INSERT).

Silver Layer (Cleansed Zone):
- Standardizes schema, enforces data quality checks, and resolves integrity issues. 
- Transformation logic ensures consistent, clean, and joinable datasets.

Gold Layer (Business Zone):
- Data modeled into a Sales Data Mart using a Star Schema consisting of Fact and Dimension tables (e.g., fact_sales, dim_customers, dim_products). 
- Provides a business-friendly semantic layer for analytics.

![Data Architecture](./Docs/Data%20Architecture.drawio.png)  
*Figure 1: Data Architecture*  

---

## 📊 Data Flow  

The data ingestion and transformation pipeline moves data across Bronze → Silver → Gold layers.  

![Data Flow Diagram]
<img width="687" height="531" alt="Data FLow Diagram drawio" src="https://github.com/user-attachments/assets/fa906602-5b5d-4ad8-bab8-a5869e7757da" />  
*Figure 2: Data Flow Diagram*  

---

## 🗂️ Data Modeling  

The business layer is modeled using a **Star Schema** for the Sales Data Mart.  

![Data Model (Star Schema)](./Docs/Data%20Model%20(Star%20Schema).drawio.png)  
*Figure 3: Star Schema Data Model*  

---

## 🔗 Table Relationships  

The relationships between different tables in the warehouse are documented below:  

![Table Relations](./Docs/Table%20Relations.png)  
*Figure 4: Table Relationships*  

---

## ⚙️ Implementation Highlights  

- **Data Ingestion:** Loaded CSV files into SQL Server using `BULK INSERT`.  
- **Stored Procedures:** Automated data loading with error handling (`TRY...CATCH`) and load time tracking.  
- **Data Quality:** Removed duplicates, enforced key relationships, and added audit columns for tracking.  
- **Modeling:** Designed a Star Schema with fact and dimension tables, using surrogate keys for consistency.  
- **Version Control:** All SQL scripts, diagrams, and documentation are stored and managed in GitHub.  

---

## 📁 Repository Structure  
├── Docs/ # Architecture, Data Flow & Schema Diagrams
├── Scripts/ # SQL Scripts for Bronze, Silver, Gold layers
├── Tests/ # Data Quality & Validation Scripts
├── Datasets/ # Source CSV files
└── README.md # Project Documentation
