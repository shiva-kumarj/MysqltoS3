import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values
import pickle
import os
from datetime import datetime
import csv
import time
import boto3

# Retrieve aws creds from environment variables
aws_access_key_id = os.environ.get('de_access_key_id')
aws_secret_access_key = os.environ.get('de_secret_access_key')
aws_region = os.environ.get('aws_default_region', 'us-east-1')

session = boto3.session.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

s3_client = session.client('s3')

def connect_to_mysql(config):
    try:
        return mysql.connector.connect(
            host=config["DB_HOST"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            database=config["DB_NAME"],
        )
       
    except (mysql.connector.Error, IOError) as err:
        print(err)

    return None


def read_batch_tracker(file_path):
    data = {
        "last_processed_pk": 0,
        "timestamp": None,
        "batch_number": 0,
        "table_name": "",
        "database": "",
    }
    if not os.path.exists(file_path):
        print("File does not exist.")
        return data
    else:
        try:
            with open(file_path, "rb") as f:
                data = pickle.load(f)
        except EOFError:
            print("EOFError: The pickle file is empty or corrupted")
        except pickle.UnpicklingError:
            print(
                "UnpicklingError: Failed to unpickle the data. The file might be corrupted."
            )
    return data


def update_batch_tracker(batch_info, file_path):
    try:
        with open(file_path, "wb") as f:
            pickle.dump(batch_info, f)
    except FileNotFoundError:
        print(f"Pickle file not found")


def process_data(rows):
    print("data processed")
    pass


def write_to_s3(config, batch_info):
    try:
        # client = boto3.client('s3')
        output_filename = f"{config['DB_NAME']}_{config['TABLE_NAME']}_{batch_info['batch_number']}.csv"
        s3_client.upload_file("buffer_data.csv", config['S3_BUCKET'], output_filename)
        print("data written to S3")
    except Exception as e:
        print(f"Upload to S3 failed with error: {e}")
        return False
    
    return True

def write_data_to_local_file(data):
    try:
        with open("buffer_data.csv", 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['actor_id', 'first_name', 'last_name', 'last_update'])
            writer.writerows(data)
    except Exception as e:
        print(f"Exception during writing to Buffer file: {e}")
        return False
    return True

# else read from primary_key > pickle file content to limit batch_size
def sql_table_reader(config, batch_info, file_path):
    # Establish connection with mysql.
    cnx = connect_to_mysql(config)
    if not cnx.is_connected():
        print("Cannot connect to the Database")
        return False
    try:
        where_condition = (
            f"where {config['TABLE_PRIMARY_KEY']} > {batch_info['last_processed_pk']}"
        )
        # Get latest batch info
        with cnx.cursor() as cursor:
            cursor.execute(
                f"Select * from {config['TABLE_NAME']} {where_condition} limit {config['BATCH_SIZE']}"
            )
            rows = cursor.fetchall()
            if not rows:
                print("No more records to process")
                return True
            # write row to a file for sake of bufferring
            if write_data_to_local_file(rows):
                print(f"{rows[-1]}")

            # process and transform the data
            processed_data = process_data(rows)

            # write transformed data to s3
            is_success = write_to_s3(config, batch_info)
            # if success then update batch info and update tracker
            if is_success:
                batch_info.update(
                    {
                        "last_processed_pk": rows[-1][int(config["INDEX_OF_PK"])],
                        "timestamp": datetime.now().isoformat(),
                        "batch_number": batch_info["batch_number"] + 1,
                        "table_name": config["TABLE_NAME"],
                        "database": config["DB_NAME"],
                    }
                )

                update_batch_tracker(batch_info, file_path)
    except mysql.connector.Error as err:
        print(f"Error reading from Mysql DB: {err}")
        return False

    finally:
        cnx.close()



def main():
    config = dotenv_values()
    tracker_file_path = config["FILE_PATH"]
    print(f"tracker configuration: {tracker_file_path}")
    latest_batch_info = read_batch_tracker(tracker_file_path)
    print(f"latest batch info: {latest_batch_info}")
    
    # v5
    sql_table_reader(config, latest_batch_info, tracker_file_path)


if __name__ == "__main__":
    main()
