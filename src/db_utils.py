# src/db_utils.py

import pandas as pd
from sqlalchemy import create_engine, text, inspect, exc as sqlalchemy_exc
from datetime import datetime

from .config import logger, DB_ENGINE_URL


def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine(DB_ENGINE_URL)


def create_tables(engine):
    """Creates database tables. Business keys are NOT unique; source_file_name tracks origin."""
    with engine.connect() as connection:
        try:
            logger.info("Dropping existing tables (if any)...")
            connection.execute(text("DROP TABLE IF EXISTS SourceFileRegistry;"))
            connection.execute(text("DROP TABLE IF EXISTS Users;"))
            connection.execute(text("DROP TABLE IF EXISTS OrderItems;"))
            connection.execute(text("DROP TABLE IF EXISTS Orders;"))
            connection.execute(text("DROP TABLE IF EXISTS Products;"))
            connection.execute(text("DROP TABLE IF EXISTS Customers;"))
            logger.info("Dropped existing tables.")

            # Users Table
            connection.execute(text("""
            CREATE TABLE Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                name TEXT,
                email TEXT UNIQUE,
                password TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """))
            logger.info("Users table created.")

            # Customers Table
            connection.execute(text("""
            CREATE TABLE Customers (
                customer_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                customer_name TEXT, email TEXT, phone TEXT,
                address_street TEXT, address_city TEXT, address_state TEXT, address_postal_code TEXT,
                registration_date DATE, status TEXT, total_orders INTEGER, total_spent REAL,
                loyalty_points INTEGER, preferred_payment_method TEXT, birth_date DATE, age INTEGER,
                gender TEXT, segment TEXT, source_customer_id_int INTEGER,
                last_updated_pipeline DATETIME
            );
            """))
            logger.info("Customers table created.")

            # Products Table
            connection.execute(text("""
            CREATE TABLE Products (
                product_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                product_name TEXT, description TEXT, category TEXT, brand TEXT, manufacturer TEXT,
                price REAL, cost REAL, weight_kg REAL,
                dim_length_cm REAL, dim_width_cm REAL, dim_height_cm REAL,
                color TEXT, size TEXT, stock_quantity INTEGER, reorder_level INTEGER,
                supplier_id TEXT, is_active BOOLEAN, rating REAL,
                product_created_date DATE, product_last_updated_source DATETIME,
                source_item_id_int INTEGER, last_updated_pipeline DATETIME
            );
            """))
            logger.info("Products table created.")

            # Orders Table
            connection.execute(text("""
            CREATE TABLE Orders (
                order_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                customer_id TEXT,
                source_file_name TEXT NOT NULL,
                order_date DATETIME, order_status TEXT, payment_method TEXT, payment_status TEXT,
                delivery_status TEXT, shipping_address_full TEXT, shipping_cost_total REAL,
                tax_total REAL, discount_total REAL, order_total_value_gross REAL,
                order_total_value_net REAL, amount_paid_total REAL, tracking_number TEXT,
                notes TEXT, source_order_id_int INTEGER, last_updated_pipeline DATETIME
            );
            """))
            logger.info("Orders table created.")

            # OrderItems Table
            connection.execute(text("""
            CREATE TABLE OrderItems (
                order_item_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                customer_id TEXT,
                source_file_name TEXT NOT NULL,
                quantity INTEGER, unit_price REAL, line_item_total_value REAL,
                line_item_discount REAL, line_item_tax REAL, line_item_shipping_fee REAL,
                original_line_identifier TEXT, last_updated_pipeline DATETIME
            );
            """))
            logger.info("OrderItems table created.")

            # SourceFileRegistry Table
            connection.execute(text("""
            CREATE TABLE SourceFileRegistry (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL UNIQUE,
                upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                processing_status TEXT DEFAULT 'raw_uploaded',
                entity_type_guess TEXT,
                file_size_bytes INTEGER,
                row_count INTEGER,
                col_count INTEGER,
                delimiter_guess TEXT,
                encoding_guess TEXT,
                etl_batch_id TEXT,
                last_processed_timestamp DATETIME,
                last_profiled_timestamp DATETIME,
                error_message TEXT
            );
            """))
            logger.info("SourceFileRegistry table created.")

            # Create Indexes
            connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON Users(username);"))
            connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON Users(email);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_customers_business_id ON Customers(customer_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_customers_source_file ON Customers(source_file_name);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_products_business_id ON Products(product_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_products_source_file ON Products(source_file_name);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_business_id ON Orders(order_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_customer_link_id ON Orders(customer_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orders_source_file ON Orders(source_file_name);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_order_link_id ON OrderItems(order_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_product_link_id ON OrderItems(product_id);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_orderitems_source_file ON OrderItems(source_file_name);"))
            connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_sourcefileregistry_file_path ON SourceFileRegistry(file_path);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_sourcefileregistry_file_name ON SourceFileRegistry(file_name);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_sourcefileregistry_status ON SourceFileRegistry(processing_status);"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS idx_sourcefileregistry_entity ON SourceFileRegistry(entity_type_guess);"))
            logger.info("Indexes created/updated.")

            connection.commit()
            logger.info("Database tables and indexes created/updated successfully.")
        except Exception as e:
            logger.error(f"Error creating/updating tables: {e}", exc_info=True)
            if connection.in_transaction():
                try:
                    connection.rollback()
                except Exception as rb_e:
                    logger.error(f"Error during rollback: {rb_e}")
            raise


def load_df_to_db(df, table_name, engine, if_exists='append'):
    if df.empty:
        logger.info(f"DataFrame for table '{table_name}' is empty. Nothing to load.")
        return

    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        logger.error(f"Table '{table_name}' does not exist. Cannot load data. Run create_tables first.")
        raise ValueError(f"Table '{table_name}' does not exist.")

    db_table_columns = [col['name'] for col in inspector.get_columns(table_name)]

    try:
        df_to_load_cols = [col for col in df.columns if col in db_table_columns]
        df_to_load = df[df_to_load_cols].copy()

        missing_cols_in_df_for_db = set(db_table_columns) - set(df_to_load.columns)

        pk_constraint_info = inspector.get_pk_constraint(table_name)
        pk_cols = pk_constraint_info.get('constrained_columns', []) if pk_constraint_info else []

        missing_cols_to_warn = [col for col in missing_cols_in_df_for_db if col not in pk_cols]

        if missing_cols_to_warn:
            logger.warning(f"DataFrame for '{table_name}' is missing columns (excluding auto PKs) expected by DB: {missing_cols_to_warn}. These will be NULL or have DB defaults if table allows.")
            for col_to_add in missing_cols_to_warn:
                df_to_load[col_to_add] = pd.NA

        df_to_load.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"{len(df_to_load)} records action '{if_exists}' into {table_name} table.")
    except sqlalchemy_exc.IntegrityError as ie:
        logger.error(f"IntegrityError loading data to {table_name}: {ie}. This could be due to various constraints.", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error during to_sql for {table_name}: {e}", exc_info=True)
        df_to_load_cols_str = df_to_load.columns.tolist() if 'df_to_load' in locals() else 'df_to_load not defined'
        df_head_str = df_to_load.head().to_string() if 'df_to_load' in locals() else 'df_to_load not defined'
        logger.error(f"DataFrame columns attempted: {df_to_load_cols_str}")
        logger.error(f"First 5 rows attempted:\n{df_head_str}")
        raise


def fetch_distinct_business_entity_ids(engine, table_name, business_id_column):
    try:
        query = f'SELECT DISTINCT "{business_id_column}" FROM "{table_name}" WHERE "{business_id_column}" IS NOT NULL'
        with engine.connect() as connection:
            df = pd.read_sql_query(text(query), connection)
        return set(df[business_id_column].dropna().astype(str))
    except Exception as e:
        logger.error(f"Error fetching distinct business IDs from {table_name}.\"{business_id_column}\": {e}", exc_info=True)
        return set()


def fetch_all_users(engine):
    credentials = {'usernames': {}}
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT username, name, email, password FROM Users"))
            for row_tuple in result:
                row = row_tuple._asdict()
                credentials['usernames'][row['username']] = {
                    'name': row['name'], 'email': row['email'], 'password': row['password']
                }
        return credentials
    except Exception as e:
        logger.error(f"Error fetching users: {e}", exc_info=True)
        return credentials


def add_user(engine, username, name, email, password):
    try:
        passwords_to_hash = [str(password)]
        hashed_password = stauth.Hasher(passwords_to_hash).generate()[0]
        with engine.connect() as connection:
            stmt = text("INSERT INTO Users (username, name, email, password) VALUES (:username, :name, :email, :password)")
            connection.execute(stmt, {"username": username, "name": name, "email": email, "password": hashed_password})
            connection.commit()
        logger.info(f"User '{username}' added successfully.")
        return True, "User registered successfully."
    except sqlalchemy_exc.IntegrityError as ie:
        logger.warning(f"Integrity error adding user '{username}': {ie}")
        if "UNIQUE constraint failed: Users.username" in str(ie).lower():
            return False, "Username already exists."
        if "UNIQUE constraint failed: Users.email" in str(ie).lower():
            return False, "Email already registered."
        return False, "Database constraint violated."
    except Exception as e:
        logger.error(f"Error adding user '{username}': {e}", exc_info=True)
        return False, f"Error registering user: {str(e)}"


def register_uploaded_file_in_db(engine, file_name, file_path, file_size, entity_type_guess="unknown", row_count=None, col_count=None):
    now = datetime.now()
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                result = connection.execute(
                    text("SELECT file_id FROM SourceFileRegistry WHERE file_path = :file_path"),
                    {"file_path": file_path}
                ).fetchone()

                if result:
                    file_id = result[0]
                    update_stmt = text("""
                        UPDATE SourceFileRegistry SET file_name = :file_name, upload_timestamp = :now,
                        processing_status = 'raw_uploaded', file_size_bytes = :fs,
                        entity_type_guess = :etg, row_count = :rc, col_count = :cc,
                        last_profiled_timestamp = CASE WHEN :rc IS NOT NULL OR :cc IS NOT NULL THEN :now ELSE last_profiled_timestamp END,
                        error_message = NULL WHERE file_id = :fid
                    """)
                    connection.execute(update_stmt, {
                        "file_name": file_name, "now": now, "fs": file_size,
                        "etg": entity_type_guess, "rc": row_count, "cc": col_count,
                        "fid": file_id
                    })
                    msg = f"File '{file_name}' (ID: {file_id}) re-registered/updated."
                else:
                    insert_stmt = text("""
                        INSERT INTO SourceFileRegistry (file_name, file_path, upload_timestamp, processing_status,
                        file_size_bytes, entity_type_guess, row_count, col_count, last_profiled_timestamp)
                        VALUES (:fn, :fp, :now, 'raw_uploaded', :fs, :etg, :rc, :cc, :lpt)
                    """)
                    connection.execute(insert_stmt, {
                        "fn": file_name, "fp": file_path, "now": now,
                        "fs": file_size, "etg": entity_type_guess, "rc": row_count,
                        "cc": col_count, "lpt": (now if row_count is not None or col_count is not None else None)
                    })
                    msg = f"File '{file_name}' registered."
                transaction.commit()
                logger.info(msg)
                return True, msg
            except Exception as e_inner:
                if transaction.is_active:
                    transaction.rollback()
                raise e_inner
    except Exception as e:
        logger.error(f"Error registering/updating file '{file_name}': {e}", exc_info=True)
        return False, f"Error registering file '{file_name}': {str(e)}"


logger.info("Database utilities defined in src/db_utils.py.")
