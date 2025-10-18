from sqlalchemy import create_engine

# Database connection details
DB_USER = "your_username"
DB_PASSWORD = "your_password"
DB_HOST = "localhost" # Or your database host/IP
DB_PORT = 3306 # Default MySQL port
DB_NAME = "your_database_name"

# Construct the database URL
# The format varies slightly depending on the database and driver
# For MySQL with PyMySQL driver: mysql+pymysql://user:password@host:port/database_name
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    # Create the engine
    # echo=True will log all SQL statements to standard output, useful for debugging
    engine = create_engine(DATABASE_URL, echo=True)

    # Establish a connection (optional, engine handles connections lazily)
    # This line explicitly opens a connection to verify the connection details
    with engine.connect() as connection:
        print(f"Successfully connected to the database: {DB_NAME}")
        # You can now perform database operations using 'connection'
        # For example, execute a simple query:
        result = connection.execute("SELECT 1")
        print(f"Query result: {result.scalar()}")

except Exception as e:
    print(f"Error connecting to the database: {e}")
