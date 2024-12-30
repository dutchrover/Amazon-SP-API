import pyodbc

def create_database():
    conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=sa;PWD=Kv1506kV;TrustServerCertificate=yes'
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        print('Connection successful!')
        
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT name FROM sys.databases WHERE name = 'Vericonic'")
        if not cursor.fetchone():
            print("Creating Vericonic database...")
            cursor.execute("CREATE DATABASE Vericonic")
            print("Database created successfully!")
        else:
            print("Vericonic database already exists")
            
        # List all databases
        cursor.execute("SELECT name FROM sys.databases")
        print("\nAvailable databases:")
        for row in cursor.fetchall():
            print(f"- {row[0]}")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Operation failed: {str(e)}')

if __name__ == '__main__':
    create_database() 