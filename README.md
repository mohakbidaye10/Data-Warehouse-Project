# 🏗️ Data Warehouse Project  

This project demonstrates the implementation of a **Data Warehouse** using the **Medallion Architecture (Bronze, Silver, Gold layers)**. It covers ingestion of raw data, data cleansing and transformation, and modeling into a **Star Schema** to enable business intelligence and analytics.  

---

## 📐 Architecture  

The overall solution is designed using the **Medallion Architecture**.  

![Data Architecture](./Docs/Data%20Architecture.drawio.png)  
*Figure 1: Data Architecture*  

---

## 📊 Data Flow  

The data ingestion and transformation pipeline moves data across Bronze → Silver → Gold layers.  

![Data Flow Diagram](./Docs/Data%20Flow%20Diagram.drawio.png)  
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

- **Data Ingestion:** `BULK INSERT` from CSV files into SQL Server.  
- **Stored Procedures:** Parameterized loading with `TRY...CATCH` error handling and execution time logging.  
- **Data Quality:** Deduplication, referential integrity enforcement, and metadata columns for auditability.  
- **Modeling:** Star Schema design with surrogate keys and relationship validation.  
- **Version Control:** SQL scripts, diagrams, and documentation maintained in GitHub.  

---

## 📁 Repository Structure  

