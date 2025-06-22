# src/db_utils.py
import pandas as pd
from sqlalchemy import create_engine, text, inspect # Make sure inspect is imported
from .config import logger, DB_ENGINE_URL

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine(DB_ENGINE_URL)

def create_tables(engine):
    """Creates database tables based on the defined ERD."""
    with engine.connect() as connection:
        try:
            logger.info("Dropping existing tables (if any)...")
            connection.execute(text("DROP TABLE IF EXISTS OrderItems;"))
            connection.execute(text("DROP TABLE IF EXISTS Orders;"))
            connection.execute(text("DROP TABLE IF EXISTS Products;"))
            connection.execute(text("DROP TABLE IF EXISTS Customers;"))
            # connection.commit() # Commit drops before creating - Important for some DBs, good practice
            logger.info("Dropped existing tables (if any).")

            # Customers Table
            connection.execute(text("""
            CREATE TABLE Customers (
                customer_id TEXT PRIMARY KEY,
                customer_name TEXT,
                email TEXT,
                phone TEXT,
                address_street TEXT,
                address_city TEXT,
                address_state TEXT,
                address_postal_code TEXT,
                registration_date DATE,
                status TEXT,
                total_orders INTEGER,
                total_spent REAL,
                loyalty_points INTEGER,
                preferred_payment_method TEXT,
                birth_date DATE,
                age INTEGER,
                gender TEXT,
                segment TEXT,
                source_customer_id_int INTEGER,
                last_updated_pipeline DATETIME
            );
            """))
            logger.info("Customers table created.")

            # Products Table
            connection.execute(text("""
            CREATE TABLE Products (
                product_id TEXT PRIMARY KEY,
                product_name TEXT,
                description TEXT,
                category TEXT,
                brand TEXT,
                manufacturer TEXT,
                price REAL,
                cost REAL,
                weight_kg REAL,
                dim_length_cm REAL,
                dim_width_cm REAL,
                dim_height_cm REAL,
                color TEXT,
                size TEXT,
                stock_quantity INTEGER,
                reorder_level INTEGER,
                supplier_id TEXT,
                is_active BOOLEAN,
                rating REAL,
                product_created_date DATE,
                product_last_updated_source DATETIME,
                source_item_id_int INTEGER,
                last_updated_pipeline DATETIME
            );
            """))
            logger.info("Products table created.")

            # Orders Table
            connection.execute(text("""
            CREATE TABLE Orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT,
                order_date DATETIME,
                order_status TEXT,
                payment_method TEXT,
                payment_status TEXT,
                delivery_status TEXT,
                shipping_address_full TEXT,
                shipping_cost_total REAL,
                tax_total REAL,
                discount_total REAL,
                order_total_value_gross REAL,
                order_total_value_net REAL,
                amount_paid_total REAL,
                tracking_number TEXT,
                notes TEXT,
                source_order_id_int INTEGER,
                last_updated_pipeline DATETIME,
                FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
            );
            """))
            logger.info("Orders table created.")

            # OrderItems Table
            connection.execute(text("""
            CREATE TABLE OrderItems (
                order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                product_id TEXT,
                customer_id TEXT,
                quantity INTEGER,
                unit_price REAL,
                line_item_total_value REAL,
                line_item_discount REAL,
                line_item_tax REAL,
                line_item_shipping_fee REAL,
                source_file TEXT,
                original_line_identifier TEXT,
                last_updated_pipeline DATETIME,
                FOREIGN KEY (order_id) REFERENCES Orders(order_id),
                FOREIGN KEY (product_id) REFERENCES Products(product_id),
                FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
            );
            """))
            logger.info("OrderItems table created.")

            # Create Indexes
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_customers_email ON Customers(email);")) # Non-unique for now
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON Orders(customer_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_order_date ON Orders(order_date);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_order_id ON OrderItems(order_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_product_id ON OrderItems(product_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_customer_id ON OrderItems(customer_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_products_category ON Products(category);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_products_brand ON Products(brand);"))
            logger.info("Indexes created.")

            connection.commit()
            logger.info("Database tables and indexes created successfully.")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            if connection:
                try:
                    connection.rollback()
                except Exception as rb_e:
                    logger.error(f"Error during rollback: {rb_e}")
            raise

def load_df_to_db(df, table_name, engine, if_exists='append'):
    """Loads a DataFrame into the specified database table, aligning columns."""
    if df.empty:
        logger.info(f"DataFrame for table '{table_name}' is empty. Nothing to load.")
        return
    try:
        inspector = inspect(engine)
        db_table_columns = [col['name'] for col in inspector.get_columns(table_name)]

        # Select only columns that exist in the database table
        df_columns_in_db = [col for col in df.columns if col in db_table_columns]
        df_to_load = df[df_columns_in_db].copy()

        # Check for missing columns that are in DB but not in DF
        missing_cols_in_df = set(db_table_columns) - set(df_to_load.columns)
        if missing_cols_in_df:
            logger.warning(f"DataFrame for '{table_name}' is missing columns expected by DB: {missing_cols_in_df}. These will be NULL or have DB defaults.")
            # These columns will effectively be NULL if not present and the table allows NULLs.

        df_to_load.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"{len(df_to_load)} records loaded/appended into {table_name} table.")
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        logger.error(f"DataFrame columns for {table_name}: {df.columns.tolist()}")
        logger.error(f"DB table columns for {table_name}: {db_table_columns}")
        logger.error(f"First 5 rows of DataFrame intended for {table_name}:\n{df.head()}")
        raise

def fetch_existing_ids(engine, table_name, id_column):
    """Fetches a set of distinct IDs from a table, ensuring column is quoted."""
    try:
        # Quoting the column name to handle potential reserved keywords or special characters
        query = f'SELECT DISTINCT "{id_column}" FROM "{table_name}"'
        df = pd.read_sql_query(query, engine)
        return set(df[id_column].dropna()) # Drop NA values from the set
    except Exception as e:
        logger.error(f"Error fetching existing IDs from {table_name}.\"{id_column}\": {e}")
        return set()

logger.info("Database utilities defined in src/db_utils.py.")