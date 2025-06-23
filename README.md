# Data Lake Solution Project

This project aims to create a **Data Lake** solution for storing, processing, and managing various types of unstructured data. The objective is to set up a data pipeline that accommodates multiple file formats such as CSV, Excel, PDF, and text, performs basic data processing, and allows for easy querying and access via Python functions.

## Fitur Utama

- **Storage Solution**: Build a storage infrastructure to accommodate multiple types of data.
- **Data Processing**: Process unstructured data (e.g., CSV, PDF, text files) and make it queryable via Python.
- **API Interface**: Provide an API (using Python functions) for external entities to access the processed data.
- **Staging Area**: Manage staging area to ensure data is prepared for further processing.
- **Data Lake Management**:
    - **Copy data/files** from external sources.
    - **Organize files into folders** and manage folder trees.
  
## File Formats Supported
- **CSV/Excel**: For warehouse temperature sensor files, among others.
- **PDF/Ms Word**: For market share data, annual financial statements, and reports from external sources like IDX.
- **Text files (`.txt`)**: For free text data such as social media comments, tweets, and other user-generated content. 

## Use Case: Social Media Analysis

For example, in the case of **social media analysis**, tweets mentioning the keyword **"adventureworks"** are collected using an external API. The collected data is processed to generate a **word cloud** as a basic analysis.

## Persyaratan

Before you start, make sure you have the following installed:
- Python 3.x
- PostgreSQL or other database solution (for Data Warehouse)
- Required Python libraries (see **Installation** section)

## Instalasi

1. Clone the repository:
   ```bash
   git clone https://github.com/cafauzi13/DataLakehouse.git
