# NexusFlow: AI-Powered Data Reconciliation and Analytics Platform

NexusFlow is an intelligent data integration and analytics platform designed to ingest, clean, reconcile, and analyze data from disparate sources. It leverages AI (Google Gemini) for schema mapping suggestions and provides a Streamlit-based user interface for managing the ETL (Extract, Transform, Load) process and visualizing the unified data.

The system is built to handle common data challenges such as messy customer records, inconsistent product information, and unstructured order data, transforming them into a clean, unified dataset ready for business intelligence.

## ✨ Key Features

*   **Flexible Data Ingestion:** Upload data files (CSV, JSON) through a user-friendly interface.
*   **Automated ETL Pipelines:**
    *   Robust data cleaning and standardization for customer, product, and order entities.
    *   Advanced customer name standardization.
    *   Handling of various date formats, numeric types, and categorical values.
    *   Creation of canonical IDs for customers and products.
*   **AI-Powered Schema Mapping:**
    *   Utilizes Google Gemini (1.5 Pro) to suggest mappings from source file columns to a target canonical schema.
    *   Graceful fallback if API key is not provided or if API calls fail.
*   **Unified Database:** Stores cleaned and reconciled data in a SQLite database for persistence and querying.
*   **Interactive Streamlit Dashboard:**
    *   **File Management:** Upload new files, view registered files, and trigger ETL processing.
    *   **Schema Review:** Pre-flight checks on uploaded files against expected schemas for chosen entity types.
    *   **Data Insights:** Dashboards for Customer Insights, Product Analytics, Order Overviews, and Sales KPIs.
    *   **Data Quality Reporting:** Overview of record counts and null value analysis post-ETL.
    *   **Source File Analytics:** View cleaned data contributions from specific source files.
*   **Modular Architecture:** Well-defined separation of concerns for configuration, database utilities, ETL logic, AI services, and UI components.
*   **Data Profiling:** Basic profiling of uploaded files (row/column counts).

## 🛠️ Tech Stack

*   **Backend & ETL:** Python 3.x
*   **Data Handling:** Pandas, NumPy
*   **Database:** SQLite (via SQLAlchemy)
*   **AI Integration:** Google Generative AI (Gemini API)
*   **Web Framework/UI:** Streamlit
*   **Visualization:** Plotly Express
*   **Authentication (Stubbed):** Basic structure for Streamlit Authenticator (user table exists, but full auth flow not fully integrated in dashboard pages).


## ⚙️ Setup and Installation

1.  **Prerequisites:**
    *   Python 3.8 or higher.
    *   `git` for cloning the repository.

2.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd nexusflow_project
    ```

3.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Environment Variables & Configuration:**

    *   **Gemini API Key (Optional but Recommended for AI features):**
        *   Obtain an API key from [Google AI Studio](https://aistudio.google.com/app/apikey).
        *   Set it as an environment variable:
            ```bash
            export GEMINI_API_KEY="YOUR_GEMINI_API_KEY"
            ```
            (or `set GEMINI_API_KEY="YOUR_GEMINI_API_KEY"` on Windows CMD, or add to a `.env` file if you integrate a library like `python-dotenv`).
        *   If not set, the application will prompt for the key in the console when AI features are first accessed, or AI features will be skipped.

    *   **Streamlit Authenticator Cookie Key (Important for Security if deploying):**
        *   The `src/config.py` file uses a default placeholder `COOKIE_KEY`. For any real deployment or shared use, **you MUST change this.**
        *   Generate a strong random key:
            ```python
            import secrets
            print(secrets.token_hex(32))
            ```
        *   Update the `COOKIE_KEY` in `src/config.py` with the generated key or set it as an environment variable `STREAMLIT_COOKIE_KEY`.

## 🚀 Running the Application

1.  **Initial ETL Run (Optional - Populates DB with sample data):**
    The project includes sample messy data files. To process these and populate the database initially:
    ```bash
    python -m src.main_etl
    ```
    This will create/recreate the `unified_ecommerce.db` file in the project root with data from `customers_messy_data.json`, `products_inconsistent_data.json`, etc.

2.  **Start the Streamlit Application:**
    ```bash
    streamlit run streamlit_app/app.py
    ```
    Open your web browser and navigate to the local URL provided by Streamlit (usually `http://localhost:8501`).

## 📋 Usage Workflow

1.  **Home Page (`app.py` & `00_Home.py`):**
    *   Provides an overview of the application and quick stats from the database.

2.  **Upload New Data Files (`07_File_Upload.py`):**
    *   Navigate to the "Upload New Data Files" page.
    *   Upload CSV or JSON files.
    *   The system saves the files to `data/uploads_new/`, performs basic profiling, and registers them in the `SourceFileRegistry` table in the database.

3.  **Process Uploaded Files (`08_Process_Uploaded_Files.py`):**
    *   Navigate to the "Process Registered Data Files" page.
    *   Select a file that is 'raw_uploaded' or 'profiled'.
    *   Confirm or select the entity type (e.g., Customer, Product, Order Items).
    *   The system will show a schema preview and highlight potential discrepancies against expected columns for that entity type.
    *   If AI schema mapping is configured and an API key is available/provided, it can be used (though the current UI for `08_Process_Uploaded_Files.py` primarily focuses on running the defined ETL pipelines; AI schema mapping is a backend capability in `ai_reconciliation.py` that could be further integrated into this UI step).
    *   Click "Process" to run the corresponding ETL pipeline for the selected file.
    *   The cleaned data will be loaded into the respective database tables (Customers, Products, Orders, OrderItems). The file's status in `SourceFileRegistry` will be updated to 'processed' or an error state.

4.  **Explore Dashboards:**
    *   **Customer Insights (`01_Customer_Insights.py`):** View customer demographics, segments, and statuses.
    *   **Product Analytics (`02_Product_Analytics.py`):** Analyze product categories and stock levels.
    *   **Order Overview (`03_Order_Overview.py`):** Explore recent order items and order status distributions.
    *   **Sales KPIs (`04_Sales_KPIs.py`):** Track total revenue, order counts, average order value, and top-selling products.
    *   **Data Quality Report (`05_Data_Quality_Report.py`):** See record counts and a basic null value analysis for the unified tables.
    *   **Source File Analytics (`06_Source_File_Analytics.py`):** Select a processed source file and view the cleaned data that originated from it, as it exists in the main database tables.

## 🧠 AI Feature: Schema Mapping Suggestions

*   The `src/ai_reconciliation.py` module provides the function `get_ai_schema_mapping_suggestions`.
*   This function takes column names from two source dataframes and a target canonical schema dictionary.
*   It then queries the configured Gemini model to suggest mappings.
*   **Configuration:** Requires `GEMINI_API_KEY` to be set (either as an environment variable or entered at runtime when prompted by `getpass`).
*   **Output:** Returns a JSON object with suggested mappings for each source to the target schema (e.g., `{"source_a_col": "TargetTable.target_column"}`).
*   **Note:** While the backend capability exists, direct UI integration for *visualizing and applying* these AI suggestions during the "Process Uploaded Files" step would be a further enhancement. The current schema check on that page is rule-based.

## 📄 Code Highlights

*   **`src/etl_pipelines.py`:** Contains the core transformation logic for each entity. Demonstrates robust data cleaning, ID generation, and handling of various data inconsistencies.
*   **`src/data_processing_utils.py`:** Provides reusable utility functions for common data manipulation tasks like string cleaning, date parsing, numeric conversion, and advanced name standardization.
*   **`src/ai_reconciliation.py`:** Showcases integration with Google Gemini for intelligent schema mapping.
*   **`src/db_utils.py`:** Manages database connections, table creation with indexing, and data loading, ensuring a clear separation of database concerns.
*   **`streamlit_app/pages/`:** Each file demonstrates how to build interactive data-driven pages using Streamlit, Plotly, and Pandas.

## 🚀 Potential Enhancements & Future Work

*   **Full UI for AI Schema Mapping:** Integrate the `get_ai_schema_mapping_suggestions` directly into the "Process Uploaded Files" page, allowing users to review and apply AI-suggested mappings.
*   **Advanced Data Profiling:** Extend `file_utils.py` to provide more detailed data profiling (e.g., data type inference, value distributions, uniqueness checks) directly on uploaded files.
*   **User Authentication & Authorization:** Fully implement user login using Streamlit Authenticator and role-based access control if needed.
*   **Job Scheduling & Monitoring:** For larger datasets or regular ETL runs, integrate a task scheduler (e.g., Celery, APScheduler).
*   **Scalability:** For very large datasets, consider migrating from SQLite to a more scalable database (e.g., PostgreSQL, MySQL) and optimizing Pandas operations or using Dask/Spark.
*   **Error Handling & Logging:** Enhance error reporting in the UI and expand structured logging for easier debugging.
*   **Configuration Management:** Use a more robust configuration system (e.g., Dynaconf, Pydantic Settings) especially if deploying to multiple environments.
*   **Automated Testing:** Implement unit and integration tests for ETL pipelines and utility functions.


