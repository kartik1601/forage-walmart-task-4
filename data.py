import os
import csv
import sqlite3

def createTable(conn):
    # Cursor in sqlite3 for writing the SQL commands
    cur = conn.cursor()
    # Write a new table - shipment_details
    # origin_warehouse as String
    # destination_warehouse as String
    # product as String
    # on_time as Number
    # product_quantity as Number
    # driver_identifier as String
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Shipment_Details (
                origin_warehouse TEXT,
                destination_store TEXT,
                product TEXT,
                on_time INTEGER,
                product_quantity INTEGER,
                driver_identifier TEXT
        );    
    ''')
    # Commit the changes into the database
    conn.commit()

def populate_data1(conn,csv_file_path):
    curr = conn.cursor()
    # Open the spreadsheet in read mode and write it into the database
    with open(csv_file_path, 'r') as file:
        file_reader = csv.reader(file)
        # skip the header row as it contains the headings
        next(file_reader)

        for row in file_reader:
            curr.execute('''
                INSERT INTO Shipment_Details (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
                VALUES (?,?,?,?,?,?);
            ''', (row[0],row[1],row[2],row[3],row[4],row[5]))

    conn.commit()

def caching_data(cache,csv1,csv2):
    # Cache the proper row items from csv1
    with open(csv1,'r') as file:
        file_reader = csv.reader(file)
        next(file_reader)

        for row in file_reader:
            # Key: Shipment_id; Values: Product, On time, Quantity
            # For same shipment Ids just increment the quantity count
            existing_data = cache.get(row[0])
            if existing_data:
                existing_data['product_quantity'] += 1
            
            else:
                # Add the new data
                cache[row[0]] = {
                    'product': row[1],
                    'on_time': row[2],
                    'product_quantity': 1
                }

    # Cahe the proper row items from csv2 acc to shipment_id
    with open(csv2,'r') as file:
        file_reader = csv.reader(file)
        next(file_reader)

        for row in file_reader:
            # Key: Shipment_id; Values: Origin, Destination, Driver
            cache.setdefault(row[0],{}).update({
                'origin_warehouse': row[1],
                'destination_store': row[2],
                'driver_identifier': row[3]
            })

def populate_data2(conn,cache):
    curr = conn.cursor()

    # Get the Key and value pairs from cache and add it into the database
    for Shipment_id, data in cache.items():
        curr.execute('''
            INSERT INTO Shipment_Details (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
            VALUES (?,?,?,?,?,?);
        ''', (
            data.get('origin_warehouse', ''),
            data.get('destination_store', ''),
            data.get('product', ''),
            data.get('on_time', ''),
            data.get('product_quantity', 0),
            data.get('origin_warehouse', '')
        ))

    conn.commit()


def main():
    database_path = 'shipment_database.db'
    csv_folder = 'data'
    csv_file_path_0 = os.path.join(csv_folder,'shipping_data_0.csv')
    csv_file_path_1 = os.path.join(csv_folder,'shipping_data_1.csv')
    csv_file_path_2 = os.path.join(csv_folder,'shipping_data_2.csv')

    # Connect to the database
    conn = sqlite3.connect(database_path)

    # Creating the main table for data
    createTable(conn)

    # Populating the data from the spreadsheets
    populate_data1(conn,csv_file_path_0)
    # Handle the two sheets case separately
    # we will using caching to store the data based upon shipment identifier and then populate the table
    cache = {}
    caching_data(cache, csv_file_path_1, csv_file_path_2)
    populate_data2(conn, cache)

    # Close the database
    conn.close()

if __name__ == "__main__":
    main()

