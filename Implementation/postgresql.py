import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os


# Set up the connection parameters
host = os.getenv("HOST")
database = os.getenv("DATABASE")
user = os.getenv("USERNAME")
port = os.getenv("PORT")
password = os.getenv("PASSWORD")

# Connect to the database
with psycopg2.connect(f"dbname='{database}' user='{user}' host='{host}' port='{port}' password='{password}'") as conn:
    # Create a cursor
    with conn.cursor() as cursor:
        # Define the SQL statement for creating the table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS admin(
                OpMonth DATE,
                CompanyNameLocal VARCHAR(30),
                FuelType VARCHAR(30),
                GenerationType VARCHAR(30),
                NumberOfPlant INT,
                MaxOutputMW FLOAT,
                GenMW FLOAT,
                PublishDate DATE,
                PRIMARY KEY (OpMonth, CompanyNameLocal, FuelType, GenerationType)
            )
        """

        create_table_sql1 = """
                    CREATE TABLE IF NOT EXISTS admin1(
                        OpMonth DATE,
                        CompanyName VARCHAR(30),
                        DemandType1 VARCHAR(30),
                        DemandType2 VARCHAR(30),
                        DemandMW float,
                        PublishDate DATE,
                        PRIMARY KEY (OpMonth, CompanyName, DemandType1, DemandType2)
                    )
                """

        # Create the table if it doesn't already exist
        # cursor.execute(create_table_sql)
        cursor.execute(create_table_sql1)
        conn.commit()

        # Define the SQL statement for inserting data into the table
        insert_sql1 = """
            INSERT INTO admin1 (OpMonth, CompanyName, DemandType1, DemandType2, DemandMW, PublishDate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (OpMonth, CompanyName, DemandType1, DemandType2)
            DO NOTHING
        """

        # Define the data to insert
        data = ('2022/10/1', 'ACME Inc.', 'Type A', 'Type B', 8000, '2023/2/2')

        # Insert the data into the table
        cursor.execute(insert_sql1, data)
        conn.commit()

        # Execute a SELECT statement to retrieve the data from the table
        cursor.execute("SELECT * FROM admin")

        # Print the retrieved data
        for row in cursor:
            print(row)