# pip install mysql-connector-python

import pandas as pd
import mysql.connector

try:
    # Establish connection
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="your_mysql_password", # Replace with your MySQL root password
      database="test_db"             # Replace with your database name
    )

    # SQL query to fetch data
    query = "SELECT * FROM users" # Replace 'users' with your table name

    # Read data into Pandas DataFrame
    df = pd.read_sql(query, con=mydb)

    # Print the DataFrame
    print(df)

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the connection if it was established
    if 'mydb' in locals() and mydb.is_connected():
        mydb.close()
        print("MySQL connection closed.")
