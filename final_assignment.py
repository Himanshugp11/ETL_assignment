import sqlite3

import pandas as pd


def connect_to_database(db_file):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        print("Connected to SQLite database successfully!")
        return conn

    except sqlite3.Error as e:
        # Handle any exceptions that occur during database connection
        print("An error occurred while connecting to the database:", e)
        return None

def perform_database_operations(connection):
    try:
        if connection:
            # Perform database operations here
            cursor = connection.cursor()
            sql_query = """
                SELECT c.customer_id, i.item_id, i.item_name, SUM(o.quantity) AS total_quantity
                FROM customers c
                JOIN sales s ON c.customer_id = s.customer_id
                JOIN orders o ON s.sales_id = o.sales_id
                JOIN items i ON o.item_id = i.item_id
                WHERE c.age BETWEEN 18 AND 35
                GROUP BY c.customer_id, i.item_id, i.item_name
                HAVING total_quantity > 0
                ORDER BY c.customer_id, i.item_id;
                """
            cursor.execute(sql_query)
            result_rows = cursor.fetchall()
            if result_rows is not None:
                result_df = pd.DataFrame(result_rows, columns=['Customer', 'Age', 'Item', 'Quantity'])
                # Save the DataFrame to a CSV file with semicolon delimiter



                #please change the path before runing other wise i will show error in your laptop
                result_df.to_csv('C:/Users/hgupt178/Documents/assignment/output_file.csv', sep=';', index=False)
                print("Data saved to CSV file successfully!")
            else:
                print(" Error while - Data saved to CSV file")
                


    except sqlite3.Error as e:
        # Handle any exceptions that occur during database operations
        print("An error occurred during database operations:", e)

    finally:
        if connection:
            # Close the database connection
            connection.close()
            print("Connection to SQLite database closed.")




# Usage example
if __name__ == "__main__":
    db_file_path = 'C:/Users/hgupt178/Documents/assignment/S30_Assignment.db'
    connection = connect_to_database(db_file_path)
    if connection:
        perform_database_operations(connection)
    else:
        print("Failed to connect to the SQLite database.")


