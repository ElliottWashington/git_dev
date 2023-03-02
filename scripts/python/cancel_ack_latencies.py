import argparse
import psycopg2
import pandas as pd

def run_queries(date):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="10.7.8.59",
        database="fixtransactions",
        user="scalp",
        password="QAtr@de442",
        port="5433"
    )

    # Define the SQL queries
    query1 = f"""
    CREATE TEMP TABLE temp_table1 AS
    SELECT tag11, tag100, tag41
    FROM fixmsg 
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND tag56 = 'INCAPNS'
    AND tag35 = 'AB';
    """

    query2 = f"""
    CREATE TEMP TABLE temp_table2 AS
    SELECT a.tag11, a.tag52 AS cancel_52, b.tag52 AS cancelack_52, a.tag41
    FROM fixmsg a
    JOIN fixmsg b ON a.tag11 = b.tag11
    WHERE a.tag52 >= TIMESTAMP '{date}'
    AND a.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag52 >= TIMESTAMP '{date}'
    AND b.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag35 = '8'
    AND b.tag39 = '4'
    AND a.tag35 = 'F'
    AND a.tag56 = 'INCAPNS';
    """

    query3 = """
    CREATE TEMP TABLE temp_table3 AS
    SELECT AVG(b.cancelack_52 - b.cancel_52) AS avg_difference, COUNT(b.cancelack_52) AS counts, a.tag100
    FROM temp_table1 a
    JOIN temp_table2 b ON a.tag11 = b.tag41
    GROUP BY a.tag100;
    """

    # Execute the queries
    with conn.cursor() as cursor:
        cursor.execute(query1)
        cursor.execute(query2)
        cursor.execute(query3)

    # Load the query results into dataframes
    table1_df = pd.read_sql_query("SELECT * FROM temp_table1", conn)
    table2_df = pd.read_sql_query("SELECT * FROM temp_table2", conn)
    table3_df = pd.read_sql_query("SELECT * FROM temp_table3", conn)

    # Write dataframes to CSV files with date in the file names
    table2_df.to_csv(f"cancel_ack_latency_{date}.csv", index=False)
    table3_df.to_csv(f"average_cancel_ack_latency_{date}.csv", index=False)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run queries for a given date")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Call run_queries with date argument
    run_queries(args.date)