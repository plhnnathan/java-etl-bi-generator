# Energy Generation ETL Pipeline 📊

This project is a Java-based **ETL (Extract, Transform, Load)** tool designed to process public energy generation data. It converts raw CSV files into a structured **Star Schema** model, ready for Business Intelligence tools like Power BI, Tableau, or Excel.

## ⚙️ Key Features
* **Extraction:** Efficiently reads large-scale raw CSV data using the Apache Commons CSV library.
* **Transformation:**
    * Handles character encoding (ISO-8859-1) for seamless Excel integration.
    * Implements **Surrogate Keys** logic for Dimension tables.
    * Dynamically generates a **Time Dimension (Calendar)** based on the dataset's date range.
    * Data cleaning and metric formatting (Potency and Fiscalization values).
* **Loading:** Outputs 5 Dimension tables and 1 Fact table in structured CSV format.

## 🛠️ Technologies & Skills
* **Language:** Java 17+
* **Library:** Apache Commons CSV
* **Concepts:** Data Engineering, Star Schema Modeling, ETL Pipelines, Business Intelligence (BI).

## 📁 Project Structure
* `src/`: Contains the core ETL logic (`FullETLGenerator.java`).
* `data/`: Directory for input and output CSV files.
* `lib/`: Dependency management.

## 📧 Contact
Nathan Chaia | [LinkedIn](www.linkedin.com/in/nathan-chaia-ba57773a2)