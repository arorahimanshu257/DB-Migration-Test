#!/usr/bin/env python3
"""
ETL Pipeline for migrating data from a MySQL database to a MongoDB database.

Design and implementation details:
 - Adheres to SOLID principles: Each stage of the pipeline is separated into its own class.
 - Uses pymysql for MySQL extraction and pymongo for MongoDB loading.
 - Includes robust error handling with logging for both positive and negative scenarios.
 - Modular design: New transformations, extraction queries, and load strategies can be added easily.
 - Supports ideas for incremental migration, rollback and unit/integration testing via isolated components.

Usage:
    python pipeline_new.py

Requirements:
    pip install pymysql pymongo

Note: This example demonstrates the migration process for a subset of tables (e.g., activation codes and incentive overrides).
"""

import logging
import sys
import traceback
from datetime import datetime

import pymysql
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_pipeline.log')
    ]
)


class MySQLConnector:
    """Handles MySQL connection and query execution."""

    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                cursorclass=pymysql.cursors.DictCursor
            )
            logging.info('Connected to MySQL database.')
        except Exception as e:
            logging.error(f'Error connecting to MySQL: {e}')
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logging.info('MySQL connection closed.')

    def execute_query(self, query, params=None):
        """Execute a query and return all rows."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                logging.info(f'Executed query: {query}')
                return result
        except Exception as e:
            logging.error(f'Error executing query: {query}. Error: {e}')
            raise


class MongoDBConnector:
    """Handles MongoDB connection and data insertion operations."""

    def __init__(self, uri, database):
        self.uri = uri
        self.database_name = database
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]
            logging.info('Connected to MongoDB.')
        except errors.PyMongoError as e:
            logging.error(f'Error connecting to MongoDB: {e}')
            raise

    def disconnect(self):
        if self.client:
            self.client.close()
            logging.info('MongoDB connection closed.')

    def insert_documents(self, collection_name, documents, batch_size=1000):
        """Insert a list of documents into the given collection in batches."""
        if not documents:
            logging.info(f'No documents to insert into collection {collection_name}.' )
            return
        collection = self.db[collection_name]
        try:
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i+batch_size]
                collection.insert_many(batch, ordered=False)
                logging.info(f'Inserted batch {i//batch_size + 1} into collection {collection_name}.')
        except errors.BulkWriteError as bwe:
            logging.error(f'Bulk write error in collection {collection_name}: {bwe.details}')
            # Optionally implement rollback mechanisms or record failures
        except Exception as e:
            logging.error(f'Error inserting documents into collection {collection_name}: {e}')
            raise


class DataTransformer:
    """Transforms raw MySQL rows to MongoDB document format based on mapping rules."""

    def transform_activation_code_incentive_override(self, row):
        """Transforms MySQL row from activation_code_incentive_overrides to MongoDB document."""
        try:
            document = {
                # In MongoDB, _id is normally generated automatically, but if needed you can map it.
                # 'activation_code_id' should be converted to ObjectId if available from lookup.
                # For this example, we assume the transformation of foreign keys are handled externally.
                'activation_code_id': row.get('activation_code_id'),
                'amount': row.get('amount'),
                'created_at': row.get('created_at'),
                'updated_at': row.get('updated_at')
            }
            return document
        except Exception as e:
            logging.error(f'Error transforming row {row}: {e}')
            raise

    def transform_activation_code_replacement(self, row):
        """Transforms MySQL row from activation_code_replacements to MongoDB document."""
        try:
            document = {
                'new_code_id': row.get('new_code_id'),
                'old_code_id': row.get('old_code_id'),
                'origin': row.get('origin'),
                'clearing_house': row.get('clearing_house'),
                'created_at': row.get('created_at'),
                'updated_at': row.get('updated_at')
            }
            return document
        except Exception as e:
            logging.error(f'Error transforming row {row}: {e}')
            raise

    # Additional transformation methods for other tables/collections can be defined here.


class ETLPipeline:
    """Orchestrates the ETL process: extraction from MySQL, transformation, and loading to MongoDB."""

    def __init__(self, mysql_connector, mongodb_connector, transformer):
        self.mysql = mysql_connector
        self.mongodb = mongodb_connector
        self.transformer = transformer

    def migrate_activation_code_incentive_overrides(self):
        """Migrate data for activation_code_incentive_overrides table to activationCodeIncentiveOverrides collection."""
        query = "SELECT id, activation_code_id, amount, created_at, updated_at FROM activation_code_incentive_overrides"
        try:
            rows = self.mysql.execute_query(query)
            logging.info(f'Extracted {len(rows)} rows from activation_code_incentive_overrides')
            documents = []
            for row in rows:
                try:
                    doc = self.transformer.transform_activation_code_incentive_override(row)
                    documents.append(doc)
                except Exception as e:
                    # Log and continue with next row
                    logging.error(f'Error transforming row with id {row.get("id")}: {e}')
            self.mongodb.insert_documents('activationCodeIncentiveOverrides', documents)
        except Exception as e:
            logging.error(f'Error in migrating activation_code_incentive_overrides: {e}')
            traceback.print_exc()

    def migrate_activation_code_replacements(self):
        """Migrate data for activation_code_replacements table to activationCodeReplacements collection."""
        query = "SELECT id, new_code_id, old_code_id, origin, clearing_house, created_at, updated_at FROM activation_code_replacements"
        try:
            rows = self.mysql.execute_query(query)
            logging.info(f'Extracted {len(rows)} rows from activation_code_replacements')
            documents = []
            for row in rows:
                try:
                    doc = self.transformer.transform_activation_code_replacement(row)
                    documents.append(doc)
                except Exception as e:
                    logging.error(f'Error transforming row with id {row.get("id")}: {e}')
            self.mongodb.insert_documents('activationCodeReplacements', documents)
        except Exception as e:
            logging.error(f'Error in migrating activation_code_replacements: {e}')
            traceback.print_exc()

    def run_all(self):
        """Runs all migration steps."""
        logging.info('Starting ETL pipeline...')
        try:
            self.migrate_activation_code_incentive_overrides()
            self.migrate_activation_code_replacements()
            # Add additional migration function calls here
            logging.info('ETL pipeline finished successfully.')
        except Exception as e:
            logging.error(f'Error running ETL pipeline: {e}')
            traceback.print_exc()
            # Optionally implement rollback mechanisms here


def main():
    # MySQL connection configuration
    mysql_config = {
        'host': 'localhost',
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'database': 'your_mysql_database',
        'port': 3306
    }

    # MongoDB connection configuration
    mongodb_config = {
        'uri': 'mongodb://localhost:27017',
        'database': 'your_mongo_database'
    }

    # Instantiate connectors
    mysql_conn = MySQLConnector(**mysql_config)
    mongodb_conn = MongoDBConnector(mongodb_config['uri'], mongodb_config['database'])

    try:
        mysql_conn.connect()
        mongodb_conn.connect()
    except Exception as e:
        logging.error('Failed to connect to one of the databases. Exiting.')
        sys.exit(1)

    transformer = DataTransformer()
    etl = ETLPipeline(mysql_conn, mongodb_conn, transformer)

    # Run the ETL process
    etl.run_all()

    # Clean up connections
    mysql_conn.disconnect()
    mongodb_conn.disconnect()


if __name__ == '__main__':
    main()
