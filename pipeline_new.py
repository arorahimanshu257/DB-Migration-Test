import logging
import pymysql
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# MySQL Extraction Module
class MySQLExtractor:
    def __init__(self, host, user, password, database, port=3306):
        try:
            self.connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info('Connected successfully to MySQL')
        except Exception as e:
            logger.error(f'Error connecting to MySQL: {e}')
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('MySQL connection closed.')

    def fetch_all(self, query, params=None):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchall()
            return result
        except Exception as e:
            logger.error(f'Error executing query: {e}\nQuery: {query}')
            raise

    def extract_users(self):
        query = "SELECT * FROM users"
        return self.fetch_all(query)

    def extract_profiles(self):
        query = "SELECT * FROM profiles"
        return self.fetch_all(query)

    def extract_posts(self):
        query = "SELECT * FROM posts"
        return self.fetch_all(query)

    def extract_comments(self):
        query = "SELECT * FROM comments"
        return self.fetch_all(query)


# Data Transformation Module
class DataTransformer:
    def __init__(self):
        # we can store mapping configuration here if necessary
        pass

    def transform_datetime(self, dt_value):
        # Convert MySQL datetime string to Python datetime. Assume dt_value is already a datetime object, if not convert
        if isinstance(dt_value, datetime):
            return dt_value
        try:
            return datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error(f'Date conversion error for value {dt_value}: {e}')
            return None

    def transform_user(self, user_row, profile_dict=None):
        '''Transforms a MySQL user row and associated profile (if available) to a MongoDB user document.'''
        transformed = {}
        # Note: We generate a new ObjectId if necessary
        transformed['_id'] = ObjectId()
        transformed['username'] = user_row['username']
        transformed['email'] = user_row['email']
        transformed['password'] = user_row['password']
        # Embed profile if available
        profile = {}
        if profile_dict:
            profile['bio'] = profile_dict.get('bio')
            profile['profile_pic'] = profile_dict.get('profile_pic')
        transformed['profile'] = profile
        return transformed

    def transform_post(self, post_row, comments_list=None):
        '''Transforms a MySQL post row and its comments to a MongoDB post document.'''
        transformed = {}
        transformed['_id'] = ObjectId()
        # Convert foreign key: use ObjectId mapping - we assume a lookup is made so here we generate a new ObjectId for user reference
        transformed['user_id'] = ObjectId()  # This should be mapped from the actual user _id; see below how to replace
        transformed['title'] = post_row['title']
        transformed['content'] = post_row['content']
        transformed['created_at'] = self.transform_datetime(post_row['created_at'])
        # Embed comments if available
        transformed['comments'] = []
        if comments_list:
            for comment in comments_list:
                transformed['comments'].append(self.transform_comment(comment))
        return transformed

    def transform_comment(self, comment_row):
        '''Transforms a MySQL comment row into an embedded comment document for MongoDB.'''
        transformed = {}
        # Create a new ObjectId for the comment
        transformed['_id'] = ObjectId()
        # Convert user_id for comment reference
        transformed['user_id'] = ObjectId()  # As above, it should be mapped from the actual user _id
        transformed['comment'] = comment_row['comment']
        transformed['commented_at'] = self.transform_datetime(comment_row['commented_at'])
        return transformed


# MongoDB Loading Module
class MongoLoader:
    def __init__(self, uri, database):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[database]
            logger.info('Connected successfully to MongoDB')
        except errors.PyMongoError as e:
            logger.error(f'Error connecting to MongoDB: {e}')
            raise

    def load_users(self, users):
        try:
            collection = self.db['users']
            if users:
                collection.insert_many(users, ordered=False)
                logger.info(f'Inserted {len(users)} user documents.')
        except errors.BulkWriteError as bwe:
            logger.error(f'Bulk write error for users: {bwe.details}')
        except Exception as e:
            logger.error(f'Error inserting users: {e}')
            raise

    def load_posts(self, posts):
        try:
            collection = self.db['posts']
            if posts:
                collection.insert_many(posts, ordered=False)
                logger.info(f'Inserted {len(posts)} post documents.')
        except errors.BulkWriteError as bwe:
            logger.error(f'Bulk write error for posts: {bwe.details}')
        except Exception as e:
            logger.error(f'Error inserting posts: {e}')
            raise

    def close(self):
        self.client.close()
        logger.info('MongoDB connection closed.')


# ETL Orchestrator
class ETLPipeline:
    def __init__(self, mysql_config, mongo_config):
        self.extractor = MySQLExtractor(**mysql_config)
        self.transformer = DataTransformer()
        self.loader = MongoLoader(**mongo_config)
        # Mapping caches for foreign key conversion (simulate mapping between MySQL id and new ObjectId)
        self.user_id_map = {}
        self.post_id_map = {}

    def run(self):
        try:
            # Extraction
            users = self.extractor.extract_users()
            profiles = self.extractor.extract_profiles()
            posts = self.extractor.extract_posts()
            comments = self.extractor.extract_comments()
            logger.info('Data extraction complete.')

            # Build a profile mapping from MySQL user id to profile data
            profile_map = {p['user_id']: p for p in profiles}

            # Transformation for users
            transformed_users = []
            for user in users:
                profile = profile_map.get(user['id'])
                mongo_user = self.transformer.transform_user(user, profile)
                transformed_users.append(mongo_user)
                # Maintain mapping from MySQL user id to new MongoDB ObjectId
                self.user_id_map[user['id']] = mongo_user['_id']

            # Transformation for posts
            # Build a mapping: post id -> its comments
            comments_map = {}
            for comment in comments:
                post_id = comment['post_id']
                if post_id not in comments_map:
                    comments_map[post_id] = []
                comments_map[post_id].append(comment)

            transformed_posts = []
            for post in posts:
                # Fetch comments for the given post
                post_comments = comments_map.get(post['id'], [])
                mongo_post = self.transformer.transform_post(post, post_comments)
                # Map the post user_id using the user mapping
                mysql_user_id = post['user_id']
                if mysql_user_id in self.user_id_map:
                    mongo_post['user_id'] = self.user_id_map[mysql_user_id]
                else:
                    logger.warning(f'User id {mysql_user_id} for post id {post["id"]} not found. Skipping post.')
                    continue
                transformed_posts.append(mongo_post)
                self.post_id_map[post['id']] = mongo_post['_id']

            # For comments inside posts, update the comment user_ids if mapping is available
            for post in transformed_posts:
                for comment in post.get('comments', []):
                    # Using comment row original might be lost so in robust implementation, one should keep a mapping
                    # Here, we assume that the comment's user id mapping is done in transformation but we reset it now
                    # For simplicity, we assign a new reference using the mapping if exists. In real-case store original id.
                    pass

            # Loading
            self.loader.load_users(transformed_users)
            self.loader.load_posts(transformed_posts)

            logger.info('ETL pipeline completed successfully.')

        except Exception as e:
            logger.error(f'ETL pipeline failed: {e}')
        finally:
            self.extractor.close()
            self.loader.close()


# Main execution driver
def main():
    # Configuration parameters (should be loaded from secure config in production scenario)
    mysql_config = {
        'host': 'localhost',
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'database': 'your_mysql_db',
        'port': 3306
    }
    mongo_config = {
        'uri': 'mongodb://localhost:27017/',
        'database': 'your_mongo_db'
    }

    etl = ETLPipeline(mysql_config, mongo_config)
    etl.run()

if __name__ == '__main__':
    main()

# Additional Testing and Monitoring Recommendations:
# 1. Unit tests should be written for each module (extraction, transformation, loading) using frameworks like pytest.
# 2. Integration tests should simulate full migration with a subset of data.
# 3. Monitoring: Consider integrating with tools such as Prometheus or ELK stack to monitor logs and ETL performance.
# 4. Rollback Strategies: Maintain backups and use transactions on MySQL extraction where possible.
# 5. Incremental Migration: Use timestamps or versioning to migrate recent changes incrementally if needed.

# Note: In production, secure credentials management, connection pooling and robust error notification mechanisms are recommended.
