import pandas as pd
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi


from config import (
                USERNAME,
                PASSWORD,
                CLUSTER_URL,
                INPUT_FILE_EMPLOYEES,
                INPUT_FILE_EMPLOYERS,
                OUTPUT_FILE_EMPLOYEES,
                OUTPUT_FILE_EMPLOYERS

)

def excel_to_json(input_file, output_file):
    # Read Excel file
    df = pd.read_excel(input_file)

    # convert datetime to string for json serialization
    for column in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[column] = df[column].astype(str)

    # Convert DataFrame to dictionary
    data = df.to_dict(orient="records")

    # Write JSON file with specified format
    with open(output_file, "w") as json_file:
        json.dump(data, json_file, indent=4, separators=(",", ": "))

def insert_json_to_mongodb(json_file, collection):
    """Inserts data from a JSON file into a MongoDB collection."""
    with open(json_file, "r") as file:
        data = json.load(file)
        result = collection.insert_many(data)
        return len(result.inserted_ids)




if __name__ == "__main__":
        try:
            # Create MongoDB URI
            mongo_uri = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_URL}/?retryWrites=true&w=majority&appName=Data_Migration"

            # Create a new client and connect to the server
            client = MongoClient(mongo_uri, server_api=ServerApi('1'))

            # Send a ping to confirm a successful connection
            try:
                client.admin.command('ping')
                print("Pinged your deployment. You successfully connected to MongoDB!")
            except Exception as e:
                print(f"Failed to connect to MongoDB: {e}")
                raise e  # Re-raise the exception to be caught by the outer try-except

            # Access database
            db = client['DM-Cluster']

            # Access collections
            employers_collection = db['Employers']
            employees_collection = db['Employees']

            print("Start run")

            # Convert Excel files to JSON
            excel_to_json(INPUT_FILE_EMPLOYERS, OUTPUT_FILE_EMPLOYERS)
            excel_to_json(INPUT_FILE_EMPLOYEES, OUTPUT_FILE_EMPLOYEES)
            print("Excel files have been successfully converted to JSON.")

            # Insert JSON data into MongoDB
            count_employers = insert_json_to_mongodb(OUTPUT_FILE_EMPLOYERS, employers_collection)
            count_employees = insert_json_to_mongodb(OUTPUT_FILE_EMPLOYEES, employees_collection)

            print(f"{count_employers} JSON records have been successfully imported into the Employers MongoDB collection.")
            print(f"{count_employees} JSON records have been successfully imported into the Employees MongoDB collection.")


        except Exception as e:
            print(f"An error occurred: {e}")





