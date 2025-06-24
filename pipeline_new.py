import pymysql
import pymongo
from bson.objectid import ObjectId
import logging
from datetime import datetime


# ---------------------------------------------------------
# MySQL Extractor Class
# ---------------------------------------------------------
class MySQLExtractor:
    """
    Responsible for connecting to MySQL and extracting data.
    Follows Single Responsibility (extract data from MySQL) and is reusable.
    """
    def __init__(self, host, user, password, db, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db,
                port=self.port,
                cursorclass=pymysql.cursors.DictCursor
            )
            logging.info('Connected to MySQL database.')
        except Exception as e:
            logging.error('MySQL connection error: %s', e)
            raise

    def fetch_all(self, query):
        if self.connection is None:
            self.connect()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info('Extracted %d records for query: %s', len(results), query)
                return results
        except Exception as e:
            logging.error('Error fetching data: %s', e)
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info('MySQL connection closed.')


# ---------------------------------------------------------
# Transformer Class
# ---------------------------------------------------------
class Transformer:
    """
    Responsible for transforming MySQL data rows to MongoDB documents.
    Handles type conversion (e.g., INT to ObjectId, DATETIME to Date), field mappings,
    and relationship handling (e.g., embedding order items) based on mapping documents.
    """
    def __init__(self):
        # Mapping dictionaries for maintaining foreign key references
        self.user_id_map = {}
        self.product_id_map = {}
        self.order_id_map = {}

    def transform_user(self, user_row):
        # Generate ObjectId for user. In a real scenario, you might maintain consistency with original ID
        new_id = ObjectId()
        self.user_id_map[user_row['id']] = new_id
        transformed = {
            '_id': new_id,
            'username': user_row.get('username'),
            'email': user_row.get('email'),
            'password': user_row.get('password'),
            'created_at': user_row.get('created_at') or datetime.utcnow()
        }
        return transformed

    def transform_product(self, product_row):
        new_id = ObjectId()
        self.product_id_map[product_row['id']] = new_id
        transformed = {
            '_id': new_id,
            'name': product_row.get('name'),
            'description': product_row.get('description'),
            'price': float(product_row.get('price')) if product_row.get('price') else 0.0,
            'created_at': product_row.get('created_at') or datetime.utcnow()
        }
        return transformed

    def transform_order(self, order_row, order_items):
        new_id = ObjectId()
        self.order_id_map[order_row['id']] = new_id
        # Convert user_id from MySQL integer to corresponding ObjectId
        user_obj_id = self.user_id_map.get(order_row.get('user_id'))
        if not user_obj_id:
            logging.warning('User ID %s not found in mapping for order %s', order_row.get('user_id'), order_row.get('id'))
        # Process embedded order items
        items = []
        for item in order_items:
            product_obj_id = self.product_id_map.get(item.get('product_id'))
            if not product_obj_id:
                logging.warning('Product ID %s not found in mapping for order item in order %s', item.get('product_id'), order_row.get('id'))
            items.append({
                'product_id': product_obj_id,
                'quantity': item.get('quantity', 1)
            })
        transformed = {
            '_id': new_id,
            'user_id': user_obj_id,
            'order_date': order_row.get('order_date') or datetime.utcnow(),
            'order_items': items
        }
        return transformed


# ---------------------------------------------------------
# Mongo Loader Class
# ---------------------------------------------------------
class MongoLoader:
    """
    Responsible for loading transformed documents into MongoDB.
    Encapsulates insertion logic and batch operations.
    """
    def __init__(self, uri, db_name):
        try:
            self.client = pymongo.MongoClient(uri)
            self.db = self.client[db_name]
            logging.info('Connected to MongoDB.')
        except Exception as e:
            logging.error('MongoDB connection error: %s', e)
            raise

    def load_collection(self, collection_name, documents):
        if not documents:
            logging.info('No documents to load into %s.', collection_name)
            return
        try:
            result = self.db[collection_name].insert_many(documents)
            logging.info('Inserted %d documents into the %s collection.', len(result.inserted_ids), collection_name)
        except Exception as e:
            logging.error('Error inserting documents into %s: %s', collection_name, e)
            raise


# ---------------------------------------------------------
# Main Pipeline Class
# ---------------------------------------------------------
class ETLPipeline:
    """
    Orchestrates the ETL process: extraction, transformation, and loading.
    This class integrates the responsibilities of extraction, transformation, and loading,
    and adheres to SOLID principles by delegating responsibilities to dedicated classes.
    """
    def __init__(self, mysql_config, mongo_uri, mongo_db_name):
        self.mysql_extractor = MySQLExtractor(**mysql_config)
        self.mongo_loader = MongoLoader(mongo_uri, mongo_db_name)
        self.transformer = Transformer()

    def run(self):
        try:
            # Extract data from MySQL
            logging.info('Beginning extraction phase.')
            users = self.mysql_extractor.fetch_all('SELECT * FROM users')
            products = self.mysql_extractor.fetch_all('SELECT * FROM products')
            orders = self.mysql_extractor.fetch_all('SELECT * FROM orders')
            order_items_all = self.mysql_extractor.fetch_all('SELECT * FROM order_items')
            logging.info('Extraction complete.')

            # Pre-processing: Group order_items by order_id
            order_items_group = {}
            for item in order_items_all:
                order_id = item.get('order_id')
                order_items_group.setdefault(order_id, []).append(item)

            # Transform data
            logging.info('Beginning transformation phase.')
            transformed_users = []
            for user in users:
                transformed_users.append(self.transformer.transform_user(user))

            transformed_products = []
            for product in products:
                transformed_products.append(self.transformer.transform_product(product))

            transformed_orders = []
            for order in orders:
                items = order_items_group.get(order.get('id'), [])
                transformed_orders.append(self.transformer.transform_order(order, items))

            logging.info('Transformation complete.')

            # Load data into MongoDB
            logging.info('Beginning loading phase.')
            self.mongo_loader.load_collection('users', transformed_users)
            self.mongo_loader.load_collection('products', transformed_products)
            self.mongo_loader.load_collection('orders', transformed_orders)
            logging.info('Loading complete.')

        except Exception as e:
            logging.error('ETL Pipeline encountered an error: %s', e)
            # Depending on the requirement, implement rollback or alerting mechanisms here
            raise
        finally:
            self.mysql_extractor.close()


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # MySQL configuration parameters (Update with actual credentials and database)
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'your_mysql_password',
        'db': 'your_mysql_db',
        'port': 3306
    }

    # MongoDB configuration (Update with actual connection details)
    mongo_uri = 'mongodb://localhost:27017/'
    mongo_db_name = 'your_mongo_db'

    # Initialize and run the ETL pipeline
    pipeline = ETLPipeline(mysql_config, mongo_uri, mongo_db_name)
    pipeline.run()


if __name__ == '__main__':
    main()
