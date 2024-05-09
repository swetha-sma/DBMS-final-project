import psycopg2
from tabulate import tabulate

# Database connection parameters
db_params = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "Aq@28082001"
}

# Transactions
transactions = [
    """
     -- Transaction 5: We add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock, (BY SWETHA)
    BEGIN;
    INSERT INTO product (prod_id, pname, price) VALUES ('p100', 'cd', 5);
    INSERT INTO stock (prod_id, dep_id, quantity) VALUES ('p100', 'd2', 50);
    COMMIT;
    """,
    """
    -- Transaction 6: We add a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock (BY SWETHA)
    BEGIN;
    INSERT INTO depot (dep_id, addr, volume) VALUES ('d100', 'Chicago', 100);
    INSERT INTO stock (prod_id, dep_id, quantity) VALUES ('p1', 'd100', 100);
    COMMIT;
    """,
     """
    -- Transaction 3: The product p1 changes its name to pp1 in Product and Stock.(BY AKIF)
    BEGIN;
    UPDATE product SET prod_id = 'pp1' WHERE TRIM(prod_id) = 'p1';
    COMMIT;
    """,
    """
    -- Transaction 4: The depot d1 changes its name to dd1 in Depot and Stock.(BY AKIF)
    BEGIN;
    UPDATE depot SET dep_id = 'dd1' WHERE TRIM(dep_id) = 'd1';
    COMMIT;
    """,
   
   
]

# Establishing a connection to the PostgreSQL server
try:
    con = psycopg2.connect(**db_params)
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)  # Set isolation level here
    con.autocommit = False  # Set autocommit to False for transactions

    # Create a cursor object
    cur = con.cursor()

    # Execute each transaction
    for transaction in transactions:
        cur.execute(transaction)
        print("Transaction completed successfully!")

except psycopg2.DatabaseError as e:
    print(f"Error: {e}")
    # Rollback the transaction if an error occurs
    con.rollback()
    

finally:
    # Fetch and print data from each table
    if con:
        try:
            for table in ['product', 'depot', 'stock']:
                print(f"\n{table} table:")
                cur.execute(f"SELECT * FROM {table};")
                rows = cur.fetchall()
                print(tabulate(rows, headers=[desc[0] for desc in cur.description]))
        except psycopg2.DatabaseError as e:
            print(f"Error fetching table data: {e}")

        # Close cursor and connection
        if cur:
            cur.close()
        if con:
            con.commit() #ensures durability
            con.close()
            print("Postgres SQL connection is successfully closed")

