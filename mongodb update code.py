import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# MongoDB connection using environment variables
connection_string = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME")
collection_name = os.getenv("MONGO_COLLECTION_NAME")

client = MongoClient(connection_string)
db = client[db_name]
collection = db[collection_name]
# List of expected columns in the schema
expected_columns = [
    "Code", "Title", "departments", "Meant for", "Date Of Issue", 
    "Link", "PDF Link", "PDF Filename", "S3 URL", "Extraction Timestamp", "Withdrawn Date"
]

# Function to process and update data
def update_mongodb_from_csv(doc_type, file_path):
    # Read CSV file and ensure all expected columns are present
    df = pd.read_csv(file_path)
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = 'NA'  # Fill missing columns with 'NA'
    
    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        doc_code = row["Document Code"]
        title = row["Title"]
        department = row["departments"].split(';') if row["departments"] != 'NA' else []
        meant_for = row["Meant for"].split(';') if row["Meant for"] != 'NA' else []
        withdrawn_date = row["Withdrawn Date"] if row["Withdrawn Date"] != 'NA' else None
        
        revision_entry = {
            "date of issue": row["Date Of Issue"],
            "Link": row["Link"],
            "PDF Link": row["PDF Link"],
            "PDF Filename": row["PDF Filename"],
            "S3 URL": row["S3 URL"],
            "Extraction Timestamp": row["Extraction Timestamp"]
        }
        
        # Check if a document with the same doc code and title already exists
        existing_doc = collection.find_one({"doc code": doc_code, "title": title})
        
        if existing_doc:
            # Check if the date of issue in the new revision already exists
            existing_dates = [rev["date of issue"] for rev in existing_doc["revisions"]]
            
            if revision_entry["date of issue"] not in existing_dates:
                # Append the new revision entry if the date of issue is different
                collection.update_one(
                    {"doc code": doc_code, "title": title},
                    {
                        "$push": {"revisions": revision_entry},
                        "$set": {
                            "departments": list(set(existing_doc["departments"] + department)),
                            "Meant for": list(set(existing_doc["Meant for"] + meant_for)),
                            "Withdrawn Date": withdrawn_date,
                            "Update Timestamp": None,
                            "Verification Timestamp": None
                        }
                    }
                )
        else:
            # Create a new document
            new_doc = {
                "Type": doc_type,
                "doc code": doc_code,
                "title": title,
                "departments": department,
                "Meant for": meant_for,
                "revisions": [revision_entry],
                "Withdrawn Date": withdrawn_date,
                "Update Timestamp": None,
                "Verification Timestamp": None
            }
            collection.insert_one(new_doc)

# Test with a single CSV file
# csv_file = "RBI_Circulars_Metadata.csv"
# doc_type = "Circulars"
# csv_file = "rbi_notifications_with_metadata.csv"
# doc_type = "Notifications"
# csv_file = "RBI_PressReleases_Metadata_20250108_213658.csv"
# doc_type = "Press Releases"
csv_file = "rbi_master_directions_metadata.csv"
doc_type = "Master Directions"

update_mongodb_from_csv(doc_type, csv_file)

print("MongoDB update completed!")