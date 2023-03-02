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
    SELECT 
    b.tag52 AS order_sent_time,
    a.tag52 AS ack_received_time,
    a.tag52 - b.tag52 AS time_difference,
    b.tag11 AS order_id,
    b.tag100 AS exchange
    FROM 
    fixmsg b 
    JOIN fixmsg a ON b.tag11 = a.tag11 
        AND b.tag35 = 'AB' 
        AND a.tag35 = '8'
    WHERE 
    b.tag52 >= TIMESTAMP '{date}'
    AND b.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND a.tag52 >= TIMESTAMP '{date}'
    AND a.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag56 = 'INCAPNS';
    
    SELECT 
        exchange, AVG(time_difference) AS avg_time_difference, COUNT(time_difference) AS counts
    FROM 
        temp_table1 
    GROUP BY 
        exchange;
    """

    # Execute the queries
    with conn.cursor() as cursor:
        cursor.execute(query1)


    # Load the query results into dataframes
    table1_df = pd.read_sql_query("SELECT * FROM temp_table1", conn)

    # Write dataframes to CSV files with date in the file names
    table1_df.to_csv(f"equity_nack_latencies_{date}.csv", index=False)
    

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run queries for a given date")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Call run_queries with date argument
    run_queries(args.date)