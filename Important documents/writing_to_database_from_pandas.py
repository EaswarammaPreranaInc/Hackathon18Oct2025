import pandas as pd
from sqlalchemy import create_engine

# Sample DataFrame
df = pd.DataFrame({
    'employee_id': [101, 102, 103],
    'name': ['John', 'Jane', 'Doe'],
    'salary': [50000, 60000, 55000]
})

# Example connection string:
# PostgreSQL:  'postgresql://user:password@localhost:5432/mydatabase'
# MySQL:        'mysql+pymysql://user:password@localhost:3306/mydatabase'

engine = create_engine('mysql+pymysql://user:password@localhost:3306/mydatabase')

# Write DataFrame to table 'employees'
df.to_sql('employees', engine, if_exists='append', index=False)

print("✅ DataFrame written to SQL table 'employees' successfully!")
