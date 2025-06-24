import logging
import pymysql
from pymongo import MongoClient, errors as mongo_errors
from bson.objectid import ObjectId
import datetime

# Configure logging to capture the ETL operations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MySQLExtractor:
    """
    Responsible for connecting to MySQL and extracting data from a given table.
    """
    def __init__(self, host, port, user, password, db):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "db": db,
            "cursorclass": pymysql.cursors.DictCursor
        }
        self.connection = None

    def connect(self):
        """
        Establish a connection to the MySQL database.
        """
        try:
            self.connection = pymysql.connect(**self.connection_params)
            logging.info("Connected to MySQL database.")
        except pymysql.MySQLError as e:
            logging.error("Error connecting to MySQL: %s", e)
            raise

    def extract_table(self, table_name):
        """
        Extracts all records from the specified table.
        """
        try:
            with self.connection.cursor() as cursor:
                sql = f"SELECT * FROM {table_name}"
                cursor.execute(sql)
                data = cursor.fetchall()
                logging.info("Extracted %d records from %s", len(data), table_name)
                return data
        except Exception as e:
            logging.error("Error extracting data from %s: %s", table_name, e)
            raise

    def close(self):
        """
        Close the MySQL connection.
        """
        if self.connection:
            self.connection.close()
            logging.info("MySQL connection closed.")


class DataTransformer:
    """
    Handles transformation of MySQL rows to MongoDB document format, according to the mapping:
    - Converts MySQL datetime to Python datetime
    - Generates new ObjectIds and maps legacy IDs for proper foreign key relationships
    """
    def __init__(self):
        # Mapping legacy MySQL IDs to new MongoDB ObjectIds
        self.user_id_map = {}
        self.post_id_map = {}

    def transform_datetime(self, dt):
        """
        Convert MySQL DATETIME value to a Python datetime object.
        Assumes dt is already a datetime object or a string in the '%Y-%m-%d %H:%M:%S' format.
        """
        if isinstance(dt, datetime.datetime):
            return dt
        try:
            return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logging.error("Error converting datetime: %s", e)
            raise

    def transform_user(self, record):
        """
        Convert a MySQL user record into a MongoDB document.
        Maps 'id' to _id using a new ObjectId and stores mapping for foreign key conversion.
        """
        legacy_id = record["id"]
        new_id = ObjectId()
        self.user_id_map[legacy_id] = new_id
        transformed = {
            "_id": new_id,
            "name": record["name"],
            "email": record["email"],
            "created_at": self.transform_datetime(record["created_at"])
        }
        return transformed

    def transform_post(self, record):
        """
        Convert a MySQL post record into a MongoDB document.
        Maps the legacy post id to a new ObjectId and converts the user_id using the user mapping.
        """
        legacy_id = record["id"]
        new_id = ObjectId()
        self.post_id_map[legacy_id] = new_id
        user_legacy_id = record["user_id"]
        if user_legacy_id not in self.user_id_map:
            raise ValueError(f"User id {user_legacy_id} not found in mapping for post.")
        transformed = {
            "_id": new_id,
            "user_id": self.user_id_map[user_legacy_id],
            "title": record["title"],
            "content": record["content"],
            "created_at": self.transform_datetime(record["created_at"])
        }
        return transformed

    def transform_comment(self, record):
        """
        Convert a MySQL comment record into a MongoDB document.
        Converts foreign keys for post_id and user_id based on mappings.
        """
        user_legacy_id = record["user_id"]
        post_legacy_id = record["post_id"]
        if user_legacy_id not in self.user_id_map:
            raise ValueError(f"User id {user_legacy_id} not found in mapping for comment.")
        if post_legacy_id not in self.post_id_map:
            raise ValueError(f"Post id {post_legacy_id} not found in mapping for comment.")
        transformed = {
            "_id": ObjectId(),
            "post_id": self.post_id_map[post_legacy_id],
            "user_id": self.user_id_map[user_legacy_id],
            "comment": record["comment"],
            "created_at": self.transform_datetime(record["created_at"])
        }
        return transformed


class MongoDBLoader:
    """
    Responsible for loading documents into MongoDB collections with bulk insertion.
    """
    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            logging.info("Connected to MongoDB.")
        except mongo_errors.ConnectionError as e:
            logging.error("Error connecting to MongoDB: %s", e)
            raise

    def load_collection(self, collection_name, documents):
        """
        Loads a list of documents into a specified MongoDB collection using bulk insert.
        """
        if not documents:
            logging.info("No documents to load for collection %s", collection_name)
            return
        try:
            collection = self.db[collection_name]
            result = collection.insert_many(documents, ordered=False)
            logging.info("Inserted %d documents into %s", len(result.inserted_ids), collection_name)
        except mongo_errors.BulkWriteError as bwe:
            logging.error("Bulk write error in collection %s: %s", collection_name, bwe.details)
            # Detailed error logging and potential retry mechanisms can be implemented here.
        except Exception as e:
            logging.error("Error inserting documents into %s: %s", collection_name, e)
            raise

    def close(self):
        """
        Close the MongoDB connection.
        """
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed.")


def run_etl(mysql_config, mongo_config):
    """
    Coordinates the complete ETL process:
    1. Connect to MySQL and MongoDB.
    2. Extract data from MySQL tables (users, posts, comments).
    3. Transform the extracted data into target MongoDB document format.
    4. Load the transformed data into MongoDB collections.

    Includes robust error handling and logging at every stage.
    """
    extractor = MySQLExtractor(**mysql_config)
    transformer = DataTransformer()
    loader = MongoDBLoader(**mongo_config)

    try:
        # Connect to both data sources
        extractor.connect()
        loader.connect()

        # Step 1: Extraction
        users = extractor.extract_table("users")
        posts = extractor.extract_table("posts")
        comments = extractor.extract_table("comments")

        # Step 2: Transformation
        transformed_users = []
        for user in users:
            try:
                transformed_users.append(transformer.transform_user(user))
            except Exception as e:
                logging.error("Error transforming user record %s: %s", user, e)
                # Continue processing other records
                continue

        transformed_posts = []
        for post in posts:
            try:
                transformed_posts.append(transformer.transform_post(post))
            except Exception as e:
                logging.error("Error transforming post record %s: %s", post, e)
                continue

        transformed_comments = []
        for comment in comments:
            try:
                transformed_comments.append(transformer.transform_comment(comment))
            except Exception as e:
                logging.error("Error transforming comment record %s: %s", comment, e)
                continue

        # Step 3: Loading
        loader.load_collection("users", transformed_users)
        loader.load_collection("posts", transformed_posts)
        loader.load_collection("comments", transformed_comments)

        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error("ETL process failed: %s", e)
    finally:
        extractor.close()
        loader.close()


if __name__ == "__main__":
    # Configuration should be externalized in production (environment variables, config files, etc.)
    mysql_config = {
        "host": "localhost",
        "port": 3306,
        "user": "your_mysql_user",
        "password": "your_mysql_password",
        "db": "your_mysql_db"
    }
    
    mongo_config = {
        "uri": "mongodb://localhost:27017",
        "db_name": "your_mongo_db"
    }
    
    run_etl(mysql_config, mongo_config)

    # Testing and Monitoring:
    # 1. Unit tests should target individual transformation functions using both valid and invalid inputs.
    # 2. Integration tests can be run on a staging environment to simulate the full migration process.
    # 3. Add retry logic and alerts (e.g., via email or monitoring services) for failure scenarios.
    # 4. For incremental migrations, store checkpoints (e.g., last migrated record IDs) to resume operations.
    # 5. Integrate logging with centralized monitoring solutions (e.g., ELK stack, CloudWatch) to track pipeline health.
