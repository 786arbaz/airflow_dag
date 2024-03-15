import mysql.connector
import logging

def execute_mysql_query(mysql_conn_id):
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="mysqlsrv09.mysql.database.azure.com",
            user="ashish",
            password="Admin@789",
            database="airflow"
        )
        cursor = conn.cursor()

        # Insert values into the table
        query = "INSERT INTO testing (job_id, job_name) VALUES (%s, %s)"
        values = (130, "shubhham")
        cursor.execute(query, values)

        print('Values uploaded')

        # Commit the transaction and close the connection
        conn.commit()
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print('MySQL connection closed')

if __name__ == "__main__":
    execute_mysql_query("mysql_conn")
