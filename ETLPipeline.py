# ETLPipeline.py
"""
ETL Pipeline for migrating data from a MySQL database to a MongoDB database.

Design:
  - Extraction: Uses a MySQLExtractor class to pull data from MySQL using mysql-connector-python.
  - Transformation: Uses a DataTransformer class to apply field mappings, join related data, and apply necessary type conversions per mappings.json.
  - Loading: Uses a MongoDBLoader class to load transformed documents into MongoDB via pymongo, applying batch inserts.
  - Orchestration: An ETLPipeline class coordinates the ETL steps with error handling and logging.
  - Best practices follow SOLID principles (each class has a single responsibility, and is modular, extensible, and testable).

Usage:
  1. Configure MySQL and MongoDB connection settings.
  2. Run the script to perform data migration.

Logging and error handling are implemented to manage both expected and unexpected errors.
"""

import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient, errors
import logging
import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration details: Adjust these configurations as needed
MYSQL_CONFIG = {
    'host': 'localhost',
    'database': 'employees',
    'user': 'your_mysql_user',
    'password': 'your_mysql_password'
}

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'employees_db'
}

# ---------------------- Extractor ----------------------
class MySQLExtractor:
    """Extracts data from MySQL database."""
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                logging.info(f'Connected to MySQL Server version {db_info}')
        except Error as e:
            logging.error(f'Error while connecting to MySQL: {e}')
            raise

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info('MySQL connection closed.')

    def fetch_all(self, query, params=None):
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            logging.error(f'Error executing query: {query} - {e}')
            raise

    def extract_employees(self):
        query = """
            SELECT emp_no, birth_date, first_name, last_name, gender, hire_date 
            FROM employees
        """
        return self.fetch_all(query)

    def extract_salaries(self):
        query = """
            SELECT emp_no, salary, from_date, to_date 
            FROM salaries
        """
        return self.fetch_all(query)

    def extract_titles(self):
        query = """
            SELECT emp_no, title, from_date, to_date 
            FROM titles
        """
        return self.fetch_all(query)

    def extract_dept_emp(self):
        query = """
            SELECT emp_no, dept_no, from_date, to_date 
            FROM dept_emp
        """
        return self.fetch_all(query)

    def extract_departments(self):
        query = """
            SELECT dept_no, dept_name
            FROM departments
        """
        return self.fetch_all(query)

    def extract_dept_manager(self):
        query = """
            SELECT emp_no, dept_no, from_date, to_date 
            FROM dept_manager
        """
        return self.fetch_all(query)

# ---------------------- Transformer ----------------------
class DataTransformer:
    """Transforms the raw data extracted from MySQL into the target MongoDB document structure."""
    def __init__(self, employees, salaries, titles, dept_emp, departments, dept_manager):
        self.employees = employees
        self.salaries = salaries
        self.titles = titles
        self.dept_emp = dept_emp
        self.departments = departments
        self.dept_manager = dept_manager
        
    def transform_date(self, date_obj):
        # Ensure date is in proper datetime.date format
        if isinstance(date_obj, datetime.date):
            return date_obj
        try:
            return datetime.datetime.strptime(str(date_obj), '%Y-%m-%d').date()
        except Exception as e:
            logging.error(f'Date conversion error: {date_obj} - {e}')
            return None

    def transform_employees(self):
        """Transforms employee records with embedded arrays (salaries, titles, dept_emp)."""
        # Build lookup dictionaries for one-to-many relationships
        salary_lookup = {}
        for row in self.salaries:
            emp_id = row['emp_no']
            salary_record = {
                'salary': int(row['salary']),
                'from_date': self.transform_date(row['from_date']),
                'to_date': self.transform_date(row['to_date'])
            }
            salary_lookup.setdefault(emp_id, []).append(salary_record)

        title_lookup = {}
        for row in self.titles:
            emp_id = row['emp_no']
            title_record = {
                'title': row['title'],
                'from_date': self.transform_date(row['from_date']),
                'to_date': self.transform_date(row['to_date']) if row['to_date'] else None
            }
            title_lookup.setdefault(emp_id, []).append(title_record)

        dept_lookup = {}
        for row in self.dept_emp:
            emp_id = row['emp_no']
            dept_record = {
                'dept_no': row['dept_no'],
                'from_date': self.transform_date(row['from_date']),
                'to_date': self.transform_date(row['to_date'])
            }
            dept_lookup.setdefault(emp_id, []).append(dept_record)

        transformed_employees = []
        for emp in self.employees:
            try:
                emp_no = emp['emp_no']
                transformed_emp = {
                    'emp_no': emp_no,
                    'birth_date': self.transform_date(emp['birth_date']),
                    'first_name': emp['first_name'],
                    'last_name': emp['last_name'],
                    'gender': emp['gender'],
                    'hire_date': self.transform_date(emp['hire_date']),
                    'salaries': salary_lookup.get(emp_no, []),
                    'titles': title_lookup.get(emp_no, []),
                    'department_assignments': dept_lookup.get(emp_no, [])
                }
                transformed_employees.append(transformed_emp)
            except Exception as e:
                logging.error(f'Error transforming employee record {emp}: {e}')
        
        return transformed_employees

    def transform_departments(self):
        """Transforms department records with embedded managerial history from dept_manager."""
        # Build lookup for managerial history
        managers_lookup = {}
        for row in self.dept_manager:
            dept = row['dept_no']
            manager_record = {
                'emp_no': row['emp_no'],
                'from_date': self.transform_date(row['from_date']),
                'to_date': self.transform_date(row['to_date'])
            }
            managers_lookup.setdefault(dept, []).append(manager_record)

        transformed_departments = []
        for dept in self.departments:
            try:
                dept_no = dept['dept_no']
                transformed_dept = {
                    'dept_no': dept_no,
                    'dept_name': dept['dept_name'],
                    'managers': managers_lookup.get(dept_no, [])
                }
                transformed_departments.append(transformed_dept)
            except Exception as e:
                logging.error(f'Error transforming department record {dept}: {e}')
        
        return transformed_departments

# ---------------------- Loader ----------------------
class MongoDBLoader:
    """Loads the transformed data into MongoDB collections."""
    def __init__(self, config):
        try:
            self.client = MongoClient(host=config['host'], port=config['port'])
            self.db = self.client[config['database']]
            logging.info('Connected to MongoDB.')
        except errors.ConnectionFailure as e:
            logging.error(f'MongoDB connection error: {e}')
            raise

    def load_employees(self, employees):
        """Inserts employee documents into the 'employees' collection. Uses upsert to handle duplicates."""
        collection = self.db.employees
        for emp in employees:
            try:
                # Upsert operation based on unique emp_no
                query = { 'emp_no': emp['emp_no'] }
                update = { '$set': emp }
                collection.update_one(query, update, upsert=True)
                logging.info(f'Upserted employee {emp["emp_no"]}')
            except Exception as e:
                logging.error(f'Error loading employee {emp["emp_no"]}: {e}')

    def load_departments(self, departments):
        """Inserts department documents into the 'departments' collection. Uses upsert to handle duplicates."""
        collection = self.db.departments
        for dept in departments:
            try:
                query = { 'dept_no': dept['dept_no'] }
                update = { '$set': dept }
                collection.update_one(query, update, upsert=True)
                logging.info(f'Upserted department {dept["dept_no"]}')
            except Exception as e:
                logging.error(f'Error loading department {dept["dept_no"]}: {e}')

# ---------------------- ETL Pipeline ----------------------
class ETLPipeline:
    """Coordinates the ETL process."""
    def __init__(self, mysql_config, mongo_config):
        self.extractor = MySQLExtractor(mysql_config)
        self.mongo_loader = MongoDBLoader(mongo_config)

    def run(self):
        try:
            # Connect to MySQL
            self.extractor.connect()

            # Extraction
            logging.info('Starting data extraction...')
            employees = self.extractor.extract_employees()
            salaries = self.extractor.extract_salaries()
            titles = self.extractor.extract_titles()
            dept_emp = self.extractor.extract_dept_emp()
            departments = self.extractor.extract_departments()
            dept_manager = self.extractor.extract_dept_manager()
            logging.info('Data extraction completed.')

            # Transformation
            logging.info('Starting data transformation...')
            transformer = DataTransformer(employees, salaries, titles, dept_emp, departments, dept_manager)
            transformed_employees = transformer.transform_employees()
            transformed_departments = transformer.transform_departments()
            logging.info('Data transformation completed.')

            # Loading
            logging.info('Starting data loading into MongoDB...')
            self.mongo_loader.load_employees(transformed_employees)
            self.mongo_loader.load_departments(transformed_departments)
            logging.info('Data loading completed.')

        except Exception as e:
            logging.error(f'ETL Pipeline encountered an error: {e}')
        finally:
            self.extractor.disconnect()

if __name__ == '__main__':
    # Create and run the ETL pipeline
    etl = ETLPipeline(MYSQL_CONFIG, MONGO_CONFIG)
    etl.run()

# ---------------------- Testing and Monitoring ----------------------
# Unit tests should be implemented for each component (e.g., using pytest or unittest) to validate:
#  - MySQL extraction queries
#  - Data transformation logic
#  - MongoDB upsert/load functionality
# Monitoring:
#  - Integrate with logging and alerting tools (e.g., CloudWatch, ELK stack) to monitor the pipeline performance and failures.
#  - Consider implementing a retry mechanism for transient errors, and a rollback mechanism if needed.

# Migration Strategies:
#  - For incremental migrations, add a timestamp filter or checkpoint mechanism in extraction queries.
#  - Implement data verification after loading to ensure data integrity.

# End of ETLPipeline.py
