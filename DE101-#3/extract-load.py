import sqlite3
import csv
import duckdb
import pandas as pd

sqlite_conn = sqlite3.connect("tpch.db") 

cursor  = sqlite_conn.cursor()

cursor.execute("""
               CREATE TABLE Customer(
               customer_id INT PRIMARY KEY,
               zipcode BIGINT,
               city VARCHAR(100),
               state_code VARCHAR(100),
               datetime_created DATE,
               datetime_updated DATE)""")

with open("Data/customers.csv", 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)

    for row in csvreader:
        cursor.execute("INSERT INTO Customer(customer_id,zipcode,city,state_code,datetime_created,datetime_updated) VALUES(?,?,?,?,?,?)",row)

sqlite_conn.commit()

cursor.execute("SELECT * FROM Customer")
customers = cursor.fetchall()
#print(customers)

duckdb_conn = duckdb.connect("duckdb.db")

insert_query = f"""
INSERT INTO Customer(customer_id,zipcode,city,state_code,datetime_created,datetime_updated) VALUES(?,?,?,?,?,?)
"""

duckdb_conn.executemany(insert_query,customers)

duckdb_conn.commit()

duckdb_conn.close()

sqlite_conn.close()












#create_query_file = open("./sql/sqlite_create_table.sql")

#create_query = create_query_file.read()

#create_query
#cursor.execute(create_query)

#cursor.execute("")
#customers = sqlite_conn.execute("SELECT * FROM Customers").fetchall()