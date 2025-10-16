To read data from a MySQL database into a Pandas DataFrame using mysql.connector in Python, follow these steps:
Install mysql-connector-python: If not already installed, install the MySQL Connector for Python:
Code

    pip install mysql-connector-python
Import Libraries: Import pandas and mysql.connector in your Python script.
Python

    import pandas as pd
    import mysql.connector
Establish Connection: Connect to your MySQL database using mysql.connector.connect(). Provide your host, user, password, and database name.
Python

    mydb = mysql.connector.connect(
      host="localhost",
      user="your_username",
      password="your_password",
      database="your_database_name"
    )
Replace "localhost", "your_username", "your_password", and "your_database_name" with your actual database credentials.
Execute Query and Load into DataFrame: Use pd.read_sql() to directly execute an SQL query and load the results into a Pandas DataFrame. This function handles the cursor and fetching of data automatically.
Python

    query = "SELECT * FROM your_table_name"
    df = pd.read_sql(query, con=mydb)
Replace "your_table_name" with the name of the table you want to query. 
Close Connection: After fetching the data, close the database connection to release resources.
Python

    mydb.close()
Example:
Python

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
