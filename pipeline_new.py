import logging
import sys
from datetime import datetime

import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient, errors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


class MySQLConnector:
    """
    Responsible for creating and managing MySQL connections and executing queries.
    """
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            if self.connection.is_connected():
                logging.info('Connected to MySQL database')
        except Error as e:
            logging.error(f'MySQL connection error: {str(e)}')
            raise

    def fetch(self, query, params=None):
        """
        Executes the given query and returns fetched results.
        """
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except Error as e:
            logging.error(f'Error executing query: {query} \nError: {str(e)}')
            raise
        finally:
            cursor.close()

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info('MySQL connection closed')


class MongoDBConnector:
    """
    Responsible for connecting and interacting with MongoDB.
    """
    def __init__(self, uri, database):
        self.uri = uri
        self.database_name = database
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.database_name]
            # Trigger a call to check connection
            self.client.server_info()
            logging.info('Connected to MongoDB')
        except errors.ServerSelectionTimeoutError as e:
            logging.error(f'MongoDB connection error: {str(e)}')
            raise

    def insert_batch(self, collection_name, documents):
        """
        Inserts a batch of documents into a collection.
        """
        try:
            if documents:
                collection = self.db[collection_name]
                result = collection.insert_many(documents, ordered=False)
                logging.info(f'Inserted {len(result.inserted_ids)} documents into {collection_name}')
        except errors.BulkWriteError as bwe:
            logging.error(f'Bulk write error in collection {collection_name}: {bwe.details}')
            # Handle individual write errors if needed
            raise
        except Exception as e:
            logging.error(f'Error inserting documents into {collection_name}: {str(e)}')
            raise

    def close(self):
        if self.client:
            self.client.close()
            logging.info('MongoDB connection closed')


class Transformer:
    """
    Responsible for transforming MySQL records to MongoDB document structure based on mapping.
    Implements transformation for activation_codes, incentive_overrides, replacements, etc.
    """
    @staticmethod
    def transform_activation_code(record, incentive_overrides=None, replacements=None):
        """
        Transforms a MySQL activation_codes record along with its incentive overrides
        and replacement records into a MongoDB document.
        """
        try:
            doc = {
                'code': record.get('code'),
                'sequence_number': int(record.get('sequence_number')) if record.get('sequence_number') is not None else None,
                'state': record.get('state'),
                'offer_id': int(record.get('offer_id')) if record.get('offer_id') is not None else None,
                'signup_code_block_id': int(record.get('signup_code_block_id')) if record.get('signup_code_block_id') is not None else None,
                'created_at': Transformer.convert_datetime(record.get('created_at')),
                'updated_at': Transformer.convert_datetime(record.get('updated_at')),
                'expiration_date': Transformer.convert_datetime(record.get('expiration_date')),
                'incentive_overrides': [],
                'replacements': []
            }
            
            if incentive_overrides is not None:
                for inv in incentive_overrides:
                    override = {
                        'amount': int(inv.get('amount')) if inv.get('amount') is not None else None,
                        'created_at': Transformer.convert_datetime(inv.get('created_at')),
                        'updated_at': Transformer.convert_datetime(inv.get('updated_at'))
                    }
                    doc['incentive_overrides'].append(override)

            if replacements is not None:
                for rep in replacements:
                    replacement = {
                        'new_code_id': int(rep.get('new_code_id')) if rep.get('new_code_id') is not None else None,
                        'old_code_id': int(rep.get('old_code_id')) if rep.get('old_code_id') is not None else None,
                        'origin': rep.get('origin'),
                        'clearing_house': rep.get('clearing_house'),
                        'created_at': Transformer.convert_datetime(rep.get('created_at')),
                        'updated_at': Transformer.convert_datetime(rep.get('updated_at'))
                    }
                    doc['replacements'].append(replacement)

            return doc
        except Exception as e:
            logging.error(f'Error transforming activation code record: {str(e)}')
            raise

    @staticmethod
    def transform_active_storage_blob(record):
        try:
            doc = {
                'key': record.get('key'),
                'filename': record.get('filename'),
                'content_type': record.get('content_type'),
                'metadata': record.get('metadata'),
                'byte_size': int(record.get('byte_size')) if record.get('byte_size') is not None else None,
                'checksum': record.get('checksum'),
                'created_at': Transformer.convert_datetime(record.get('created_at')),
                'service_name': record.get('service_name')
            }
            return doc
        except Exception as e:
            logging.error(f'Error transforming active storage blob record: {str(e)}')
            raise

    @staticmethod
    def transform_additional_product(record):
        try:
            doc = {
                'guid': record.get('guid'),
                'sku': record.get('sku'),
                'name': record.get('name'),
                'description': record.get('description'),
                'region_id': int(record.get('region_id')) if record.get('region_id') is not None else None,
                'price_cents': int(record.get('price_cents')) if record.get('price_cents') is not None else None,
                'tax_included': bool(record.get('tax_included')),
                'obsolete': bool(record.get('obsolete')),
                'production_data': bool(record.get('production_data')),
                'url': record.get('url'),
                'original_price_cents': int(record.get('original_price_cents')) if record.get('original_price_cents') is not None else None,
                'classification': int(record.get('classification')) if record.get('classification') is not None else None,
                'created_at': Transformer.convert_datetime(record.get('created_at')),
                'updated_at': Transformer.convert_datetime(record.get('updated_at'))
            }
            return doc
        except Exception as e:
            logging.error(f'Error transforming additional product record: {str(e)}')
            raise

    @staticmethod
    def transform_region(record):
        try:
            doc = {
                'name': record.get('name'),
                'created_at': Transformer.convert_datetime(record.get('created_at')),
                'updated_at': Transformer.convert_datetime(record.get('updated_at')),
                'iso_currency_code': int(record.get('iso_currency_code')) if record.get('iso_currency_code') is not None else None,
                'tax_included': bool(record.get('tax_included')),
                'default_language': record.get('default_language')
            }
            return doc
        except Exception as e:
            logging.error(f'Error transforming region record: {str(e)}')
            raise

    @staticmethod
    def convert_datetime(dt_val):
        """
        Converts a MySQL datetime string to a Python datetime object; if None, returns None.
        """
        if dt_val is None:
            return None
        if isinstance(dt_val, datetime):
            return dt_val
        try:
            # Assuming dt_val is a string in the format YYYY-MM-DD HH:MM:SS
            return datetime.strptime(dt_val, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f'Error converting datetime value {dt_val}: {str(e)}')
            return None


class ETLPipeline:
    """
    Orchestrates the ETL process: Extract from MySQL, Transform records, and Load into MongoDB.
    """
    def __init__(self, mysql_config, mongodb_config, batch_size=100):
        self.mysql = MySQLConnector(**mysql_config)
        self.mongo = MongoDBConnector(**mongodb_config)
        self.batch_size = batch_size

    def run(self):
        try:
            self.mysql.connect()
            self.mongo.connect()

            self.migrate_activation_codes()
            self.migrate_active_storage_blobs()
            self.migrate_additional_products()
            self.migrate_regions()

        except Exception as e:
            logging.error(f'ETL Pipeline encountered an error: {str(e)}')
        finally:
            self.mysql.close()
            self.mongo.close()

    def migrate_activation_codes(self):
        """
        Extracts activation_codes from MySQL, along with incentive_overrides and replacements,
        transforms the data, and loads it into the ActivationCodes collection in MongoDB.
        """
        try:
            # 1. Extract activation_codes
            activation_codes_query = """
                SELECT * FROM activation_codes
            """
            activation_codes = self.mysql.fetch(activation_codes_query)

            documents = []
            for ac in activation_codes:
                ac_id = ac.get('id')
                # Extract related incentive overrides
                overrides_query = """
                    SELECT amount, created_at, updated_at FROM activation_code_incentive_overrides
                    WHERE activation_code_id = %s
                """
                incentive_overrides = self.mysql.fetch(overrides_query, (ac_id,))

                # Extract related replacements
                replacements_query = """
                    SELECT new_code_id, old_code_id, origin, clearing_house, created_at, updated_at
                    FROM activation_code_replacements
                    WHERE old_code_id = %s OR new_code_id = %s
                """
                replacements = self.mysql.fetch(replacements_query, (ac_id, ac_id))

                doc = Transformer.transform_activation_code(ac, incentive_overrides, replacements)
                documents.append(doc)

                if len(documents) >= self.batch_size:
                    self.mongo.insert_batch('ActivationCodes', documents)
                    documents = []

            # Insert remaining documents
            if documents:
                self.mongo.insert_batch('ActivationCodes', documents)
            logging.info('Migration of activation codes completed.')

        except Exception as e:
            logging.error(f'Error in migrating activation codes: {str(e)}')
            # Additional rollback or notification logic could be added here.
            raise

    def migrate_active_storage_blobs(self):
        """
        Extracts blobs from MySQL, transforms them, and loads them into the ActiveStorageBlobs collection in MongoDB.
        """
        try:
            query = """
                SELECT * FROM active_storage_blobs
            """
            blobs = self.mysql.fetch(query)
            documents = []
            for blob in blobs:
                doc = Transformer.transform_active_storage_blob(blob)
                documents.append(doc)
                if len(documents) >= self.batch_size:
                    self.mongo.insert_batch('ActiveStorageBlobs', documents)
                    documents = []
            if documents:
                self.mongo.insert_batch('ActiveStorageBlobs', documents)
            logging.info('Migration of active storage blobs completed.')
        except Exception as e:
            logging.error(f'Error in migrating active storage blobs: {str(e)}')
            raise

    def migrate_additional_products(self):
        """
        Extracts additional_products from MySQL, transforms them, and loads them into the AdditionalProducts collection in MongoDB.
        """
        try:
            query = """
                SELECT * FROM additional_products
            """
            products = self.mysql.fetch(query)
            documents = []
            for prod in products:
                doc = Transformer.transform_additional_product(prod)
                documents.append(doc)
                if len(documents) >= self.batch_size:
                    self.mongo.insert_batch('AdditionalProducts', documents)
                    documents = []
            if documents:
                self.mongo.insert_batch('AdditionalProducts', documents)
            logging.info('Migration of additional products completed.')
        except Exception as e:
            logging.error(f'Error in migrating additional products: {str(e)}')
            raise

    def migrate_regions(self):
        """
        Extracts regions from MySQL, transforms them, and loads them into the Regions collection in MongoDB.
        """
        try:
            query = """
                SELECT * FROM regions
            """
            regions = self.mysql.fetch(query)
            documents = []
            for reg in regions:
                doc = Transformer.transform_region(reg)
                documents.append(doc)
                if len(documents) >= self.batch_size:
                    self.mongo.insert_batch('Regions', documents)
                    documents = []
            if documents:
                self.mongo.insert_batch('Regions', documents)
            logging.info('Migration of regions completed.')
        except Exception as e:
            logging.error(f'Error in migrating regions: {str(e)}')
            raise


def main():
    # Configuration for MySQL and MongoDB connections:
    mysql_config = {
        'host': 'localhost',    # update as needed
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'database': 'your_database',
        'port': 3306
    }

    mongodb_config = {
        'uri': 'mongodb://localhost:27017',  # update as needed
        'database': 'your_mongodb_database'
    }

    etl = ETLPipeline(mysql_config, mongodb_config, batch_size=100)
    etl.run()


if __name__ == '__main__':
    main()
