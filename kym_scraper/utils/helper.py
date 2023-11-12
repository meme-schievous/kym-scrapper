import psycopg2
from psycopg2 import sql


class ChildrenHelper:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

        # Connect to the database and create the 'children' table
        self.connect()
        self.create_children_table()

    def connect(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.cur = self.conn.cursor()

    def create_children_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS children (
                parent VARCHAR(255),
                child VARCHAR(255)
            );
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

    def insert_tuple(self, data):
        insert_query = """
            INSERT INTO children (parent, child) VALUES (%s, %s);
        """
        self.cur.execute(insert_query, data)
        self.conn.commit()

    def insert_batch(self, data_batch):
        insert_query = """
            INSERT INTO children (parent, child) VALUES (%s, %s);
        """
        self.cur.executemany(insert_query, data_batch)
        self.conn.commit()

    def get_all_tuples(self):
        select_query = """
            SELECT parent, child FROM children;
        """
        self.cur.execute(select_query)
        return self.cur.fetchall()

    def close_connection(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    # Replace these values with your PostgreSQL database credentials
    dbname = "airflow"
    user = "airflow"
    password = "airflow"
    host = "localhost"
    port = "5432"

    # Create an instance of the PostgreSQLManager class
    postgres_manager = ChildrenHelper(dbname, user, password, host, port)

    # Example: Insert a single tuple
    single_data = ("parent1", "child1")
    postgres_manager.insert_tuple(single_data)

    # Example: Insert a batch of tuples
    batch_data = [("parent2", "child2"), ("parent3", "child3"), ("parent4", "child4")]
    postgres_manager.insert_batch(batch_data)

    # Close the database connection
    postgres_manager.close_connection()

    print("Data inserted successfully.")