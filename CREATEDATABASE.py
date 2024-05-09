import psycopg2

# Database connection parameters
db_params = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "Aq@28082001"
}

# SQL statements for creating tables and inserting data
sql_commands = [
    """
    CREATE TABLE IF NOT EXISTS product
    (
     prod_id CHAR(10),
     pname VARCHAR(30),
     price DECIMAL(10,2)
    );
    """,
    """
    ALTER TABLE IF EXISTS product ADD CONSTRAINT pk_product PRIMARY KEY (prod_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS depot
    (
     dep_id CHAR(10),
     addr VARCHAR(30),
     volume INT
    );
    """,
    """
    ALTER TABLE IF EXISTS depot ADD CONSTRAINT pk_depot PRIMARY KEY (dep_id);
    """,
    """
    CREATE TABLE IF NOT EXISTS stock
    (
     prod_id CHAR(10),
     dep_id CHAR(10),
     quantity INT
    );
    """,
    """
    ALTER TABLE IF EXISTS stock ADD CONSTRAINT pk_stock PRIMARY KEY (prod_id, dep_id);
    """,
    """
    ALTER TABLE IF EXISTS stock 
        ADD CONSTRAINT fk_stock_product 
        FOREIGN KEY (prod_id) 
        REFERENCES Product(prod_id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE IF EXISTS stock 
        ADD CONSTRAINT fk_stock_depot 
        FOREIGN KEY (dep_id) 
        REFERENCES Depot(dep_id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE;
    """,
    """
    INSERT INTO product (prod_id, pname, price)
    VALUES
     ('p1', 'tape', 2.5),
     ('p2', 'tv', 250),
     ('p3', 'vcr', 80);
    """,
    """
    INSERT INTO depot (dep_id, addr, volume)
    VALUES
     ('d1', 'New York', 9000),
     ('d2', 'Syracuse', 6000),
     ('d4', 'New York', 2000);
    """,
    """
    INSERT INTO stock (prod_id, dep_id, quantity)
    VALUES
     ('p1', 'd1', 1000),
     ('p1', 'd2', -100),
     ('p1', 'd4', 1200),
     ('p3', 'd1', 3000),
     ('p3', 'd4', 2000),
     ('p2', 'd4', 1500),
     ('p2', 'd1', -400),
     ('p2', 'd2', 2000);
    """
]

# Establishing a connection to the PostgreSQL server
try:
    con = psycopg2.connect(**db_params)
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)  # Set isolation level here
    con.autocommit = False  # Set autocommit to False for table creation and data insertion

    # Create a cursor object
    cur = con.cursor()

    # Execute each SQL command
    for command in sql_commands:
        cur.execute(command)

    # Commit the transaction
    con.commit()
    print("Tables created and data inserted successfully!")

except psycopg2.DatabaseError as e:
    print(f"Error: {e}")
    # Rollback the transaction if an error occurs
    con.rollback()

finally:
    # Close cursor and connection
    if cur:
        cur.close()
    if con:
        con.commit() #ensures durability
        con.close()
        print("Postgres SQL connection is successfully closed")
