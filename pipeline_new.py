#!/usr/bin/env python3

import mysql.connector
from pymongo import MongoClient, errors
import logging
import sys
from datetime import datetime
from bson.objectid import ObjectId

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


class MySQLExtractor:
    """
    Responsible for connecting to MySQL and extracting data from the specified tables.
    """
    def __init__(self, host, user, password, database, port=3306):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
        self.connection = None
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            logging.info("Connected to MySQL database.")
        except mysql.connector.Error as err:
            logging.error(f"MySQL Connection error: {err}")
            raise

    def extract_users(self):
        query = "SELECT id, username, email, created_at FROM users;"
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            users = cursor.fetchall()
            logging.info(f"Extracted {len(users)} users from MySQL.")
            return users
        except mysql.connector.Error as err:
            logging.error(f"Error extracting users: {err}")
            raise
        finally:
            cursor.close()

    def extract_orders(self):
        query = "SELECT order_id, user_id, amount, order_date FROM orders;"
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            orders = cursor.fetchall()
            logging.info(f"Extracted {len(orders)} orders from MySQL.")
            return orders
        except mysql.connector.Error as err:
            logging.error(f"Error extracting orders: {err}")
            raise
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("Closed MySQL connection.")


class DataTransformer:
    """
    Responsible for transforming data from MySQL schema to MongoDB schema based on the mapping document.
    Follows SOLID principles by focusing only on transformation logic.
    """
    def __init__(self):
        # Dictionary to maintain mapping between legacy MySQL user ids and new MongoDB ObjectIds
        self.user_id_mapping = {}

    def transform_users(self, users):
        transformed_users = []
        for user in users:
            # Generate a new ObjectId for MongoDB _id
            new_id = ObjectId()
            # Save mapping from legacy MySQL id to new ObjectId for later use in orders transformation
            self.user_id_mapping[user['id']] = new_id
            transformed = {
                "_id": new_id,
                "username": user['username'][:50],  # enforce maximum length
                "email": user['email'][:100],         # enforce maximum length
                "created_at": user['created_at'] if user['created_at'] else datetime.utcnow(),
                # Optionally preserve legacy id
                "legacy_id": user['id']
            }
            transformed_users.append(transformed)
        logging.info("Transformed user data to MongoDB format.")
        return transformed_users

    def transform_orders(self, orders):
        transformed_orders = []
        for order in orders:
            # Retrieve the new ObjectId for the referenced user
            user_object_id = self.user_id_mapping.get(order['user_id'])
            if not user_object_id:
                logging.warning(f"Missing user reference for order_id {order['order_id']}. Skipping order.")
                continue  # Alternatively, log or handle the inconsistency as needed
            transformed = {
                "_id": ObjectId(),  # new generated ObjectId for the order
                "user_id": user_object_id,
                "amount": float(round(order['amount'], 2)),  # ensuring two-decimal precision
                "order_date": order['order_date'] if order['order_date'] else datetime.utcnow(),
                # Optionally preserve legacy order id
                "legacy_order_id": order['order_id']
            }
            transformed_orders.append(transformed)
        logging.info("Transformed order data to MongoDB format.")
        return transformed_orders


class MongoLoader:
    """
    Responsible for loading transformed data into MongoDB.
    Adheres to best practices including batch operations and error handling.
    """
    def __init__(self, uri, database):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[database]
            logging.info("Connected to MongoDB.")
        except errors.PyMongoError as err:
            logging.error(f"MongoDB connection error: {err}")
            raise

    def load_users(self, users):
        try:
            if users:
                collection = self.db['users']
                result = collection.insert_many(users)
                logging.info(f"Inserted {len(result.inserted_ids)} users into MongoDB.")
        except errors.BulkWriteError as bwe:
            logging.error(f"Bulk write error for users: {bwe.details}")
            raise
        except errors.PyMongoError as err:
            logging.error(f"Error inserting users: {err}")
            raise

    def load_orders(self, orders):
        try:
            if orders:
                collection = self.db['orders']
                result = collection.insert_many(orders)
                logging.info(f"Inserted {len(result.inserted_ids)} orders into MongoDB.")
        except errors.BulkWriteError as bwe:
            logging.error(f"Bulk write error for orders: {bwe.details}")
            raise
        except errors.PyMongoError as err:
            logging.error(f"Error inserting orders: {err}")
            raise

    def close(self):
        self.client.close()
        logging.info("Closed MongoDB connection.")


def main():
    # Configuration settings - Replace with actual database credentials and URIs
    mysql_config = {
        "host": "localhost",
        "user": "your_mysql_user",
        "password": "your_mysql_password",
        "database": "my_database",
        "port": 3306
    }
    mongodb_config = {
        "uri": "mongodb://localhost:27017/",
        "database": "my_mongo_db"
    }

    extractor = MySQLExtractor(**mysql_config)
    transformer = DataTransformer()
    loader = MongoLoader(mongodb_config["uri"], mongodb_config["database"])

    try:
        extractor.connect()
        
        # Extraction phase
        users = extractor.extract_users()
        orders = extractor.extract_orders()

        # Transformation phase
        transformed_users = transformer.transform_users(users)
        transformed_orders = transformer.transform_orders(orders)

        # Loading phase
        loader.load_users(transformed_users)
        loader.load_orders(transformed_orders)

        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
    finally:
        extractor.close()
        loader.close()


if __name__ == "__main__":
    main()


# Testing and Monitoring Recommendations:
# 1. Unit Testing: Develop unit tests for each component (MySQLExtractor, DataTransformer, MongoLoader) using mock objects and test databases.
# 2. Integration Testing: Use an isolated MySQL and MongoDB instance to run full ETL tests and validate data consistency.
# 3. Monitoring & Alerting: Integrate with monitoring tools (e.g., Prometheus, ELK stack) to monitor logs and performance metrics.
# 4. Incremental Migration & Rollback: Consider adding transaction management for incremental loads and develop rollback mechanisms in case of failures.
# 5. Logging: Enhance logging by directing logs to files or external systems to assist in troubleshooting and audit trails.
