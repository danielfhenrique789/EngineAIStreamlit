# 📊 Engine AI Code Challenge - Streamlit Dashboard

## 🚀 Overview
This project is a **Streamlit dashboard** that retrieves and visualizes financial data from **Snowflake**. It includes **multiple interactive widgets** to analyze stock positions, sector trends, and company-specific time series data.

## 📌 Features
✅ **Data Cleaning & Processing in SQL** (Snowflake queries)
✅ **Interactive Visualizations** (Top Sectors & Companies)
✅ **Time Series Analysis** (Stock Prices Over Time)
✅ **Pagination for Large Data Sets**
✅ **Optimized Performance with Caching**


## 🎯 How to Use the Dashboard

### **📊 Widget 1: Top 10 Sectors by USD Position**
- Displays **the top 10 industry sectors** based on their financial position.
- Uses a **bar chart** for easy comparison.
- Data is based on **the most recent available date**.

### **🏆 Widget 2: Top 25% Companies by Average Position**
- Shows a **sortable table** with companies that rank in the top **25% by average position in USD over the last year**.
- Includes key attributes:
  - **Ticker**
  - **Sector**
  - **Shares**
  - **Last Close Price in USD**
  - **Average Position over the Last Year**

### **📈 Widget 3: Time Series Chart of Stock Prices**
- Select a **specific company** from a dropdown menu.
- Displays an **interactive line chart** showing the daily **closing price in USD**.

---

## 📜 SQL Queries Used in Snowflake
1️⃣ **Daily Position in USD:**
   - Uses **window functions** to handle missing price values.
   - Computes **total USD value per company per day**.

2️⃣ **Top 25% Companies by Position:**
   - Computes **average position over the last year**.
   - Uses `PERCENT_RANK()` to select **top 25% companies**.

3️⃣ **Sector-Wide Daily Position Calculation:**
   - Aggregates company positions to **compute total sector value per day**.


## 🏆 Final Notes
This dashboard was built to be **as close to production-ready as possible**, optimizing **performance, usability, and maintainability**. 🚀


