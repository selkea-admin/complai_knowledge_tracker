# import os
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from datetime import datetime

# # Create folders to save circular PDFs
# os.makedirs("Circulars_PDFs", exist_ok=True)

# # Initialize list to store circular metadata
# circulars_data = []

# # Function to ensure correct URL format
# def make_absolute_url(link):
#     if link.startswith("http"):
#         return link
#     else:
#         return f"https://website.rbi.org.in{link}"

# # Helper function to create a revision entry
# def create_revision(extraction_timestamp=None, update_timestamp=None, verification_timestamp=None, change_details="Initial extraction", updated_by="web_scraper_bot", status="pending"):
#     return {
#         "version": len(circulars_data) + 1,  # Increment version based on the length of the data
#         "extraction_timestamp": extraction_timestamp,
#         "update_timestamp": update_timestamp,
#         "verification_timestamp": verification_timestamp,
#         "change_details": change_details,
#         "updated_by": updated_by,
#         "status": status
#     }

# # Loop through pages with `start` from 1 to 6
# for start in range(1, 2):  # Adjust range as needed
#     url = f"https://website.rbi.org.in/web/rbi/notifications/rbi-circulars?delta=10&start={start}"
#     print(f"Fetching page {start}...")

#     response = requests.get(url)
#     soup = BeautifulSoup(response.content, "html.parser")

#     # Locate each circular on the page
#     circular_rows = soup.select("tbody > tr")

#     for row in circular_rows:
#         # Extract metadata
#         circular_number = row.select_one("td a").get_text(strip=True)
#         circular_link = make_absolute_url(row.select_one("td a")["href"])
#         date_of_issue = row.select("td")[1].get_text(strip=True)
#         department = row.select("td")[2].get_text(strip=True)
#         subject = row.select("td")[3].get_text(strip=True)
#         meant_for = row.select("td")[4].get_text(strip=True)

#         # Download the main circular PDF
#         pdf_filename = os.path.join("Circulars_PDFs", f"{subject}.pdf")
#         pdf_status = "Download failed"
#         try:
#             pdf_response = requests.get(circular_link)
#             with open(pdf_filename, "wb") as pdf_file:
#                 pdf_file.write(pdf_response.content)
#             pdf_status = "Downloaded"
#         except Exception as e:
#             print(f"Failed to download main circular PDF: {e}")

#         # Fetch each circular's details to find additional links
#         filtered_links = []
#         try:
#             circular_response = requests.get(circular_link)
#             circular_soup = BeautifulSoup(circular_response.content, "html.parser")

#             # Collect all filtered links found within the circular
#             for link in circular_soup.find_all("a", href=True):
#                 if "pdf" in link["href"].lower():  # Assuming links to downloadable files contain "pdf"
#                     filtered_pdf_link = make_absolute_url(link['href'])
#                     filtered_links.append(filtered_pdf_link)

#             # Create a revision entry for version control
#             extraction_time = datetime.now().isoformat()
#             revision_entry = create_revision(
#                 extraction_timestamp=extraction_time,
#                 change_details="Extracted metadata and links from the circular",
#                 status="verified" if pdf_status == "Downloaded" else "pending"
#             )

#             # Store metadata and filtered links in dictionary format
#             circular_data = {
#                 "Code": circular_number,
#                 "Link": circular_link,
#                 "Date Of Issue": date_of_issue,
#                 "Department": department,
#                 "Title": subject,
#                 "Meant For": meant_for,
#                 "PDF Status": pdf_status,
#                 "Filtered Links": filtered_links,
#                 "Revisions": [revision_entry]
#             }

#             circulars_data.append(circular_data)

#         except requests.exceptions.RequestException as e:
#             print(f"Failed to fetch circular details: {e}")

# # Save all circular metadata to a CSV
# df = pd.DataFrame(circulars_data)
# df.to_csv("RBI_Circulars_Metadata.csv", index=False)

# print("Data saved in 'RBI_Circulars_Metadata.csv'. Main circular PDFs downloaded in 'Circulars_PDFs' folder.")

import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
BUCKET_NAME = "rbi-docs-storage"  # Replace with your bucket name
s3_client = boto3.client("s3")  # Ensure AWS credentials are properly configured

# Create folders to save circular PDFs
os.makedirs("Circulars_PDFs", exist_ok=True)

# Initialize list to store circular metadata
circulars_data = []

# Function to ensure correct URL format
def make_absolute_url(link):
    if link.startswith("http"):
        return link
    else:
        return f"https://website.rbi.org.in{link}"

# Helper function to sanitize filenames
def sanitize_filename(title, timestamp):
    sanitized = re.sub(r"[^\w\s-]", "", title).strip().replace(" ", "_")
    sanitized = re.sub(r"[-]+", "_", sanitized)  # Replace multiple underscores
    limited_filename = sanitized[:50]  # Limit length to 50 characters
    return f"{limited_filename}_{timestamp}.pdf"

# Function to upload file to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return s3_url
    except NoCredentialsError:
        print("AWS credentials not available.")
        return None

# Loop through pages with `start` from 1 to 6
for start in range(1, 201):  # Adjust range as needed
    url = f"https://website.rbi.org.in/web/rbi/notifications/rbi-circulars?delta=10&start={start}"
    print(f"Fetching page {start}...")

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate each circular on the page
    circular_rows = soup.select("tbody > tr")

    for row in circular_rows:
        # Extract metadata
        circular_number = row.select_one("td a").get_text(strip=True)
        circular_link = make_absolute_url(row.select_one("td a")["href"])
        # Extract and format the 'Date of Issue'
        date_of_issue_raw = row.select("td")[1].get_text(strip=True)
        try:
            # Parse the date and reformat it explicitly to 'Jan 03, 2025' style
            date_of_issue = datetime.strptime(date_of_issue_raw, "%d.%m.%Y").strftime("%b %d, %Y")
        except ValueError:
            # If the date is already in the desired format or parsing fails, keep it as is
            date_of_issue = date_of_issue_raw
        department = row.select("td")[2].get_text(strip=True)
        subject = row.select("td")[3].get_text(strip=True)
        meant_for = row.select("td")[4].get_text(strip=True)

        # Generate sanitized PDF filename
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        pdf_filename = sanitize_filename(subject, timestamp)
        pdf_filepath = os.path.join("Circulars_PDFs", pdf_filename)

        # Download and save the main circular PDF
        s3_url = None
        try:
            pdf_response = requests.get(circular_link)
            with open(pdf_filepath, "wb") as pdf_file:
                pdf_file.write(pdf_response.content)

            # Upload to S3
            s3_key = f"Circulars/{pdf_filename}"
            s3_url = upload_to_s3(pdf_filepath, BUCKET_NAME, s3_key)

        except Exception as e:
            print(f"Failed to download or upload main circular PDF: {e}")

        # Fetch each circular's details to find additional links
        try:
            circular_response = requests.get(circular_link)
            circular_soup = BeautifulSoup(circular_response.content, "html.parser")

            # Extract additional PDF links (if needed, not currently used)

            # Timestamps
            extraction_time = datetime.now().isoformat()
            update_time = None  # Placeholder for future updates
            verification_time = None  # Placeholder for future verification

            # Store metadata
            circular_data = {
                "Code": circular_number,
                "Link": circular_link,
                "Date Of Issue": date_of_issue,
                "Department": department,
                "Title": subject,
                "Meant For": meant_for,
                "PDF Filename": pdf_filename,
                "S3 URL": s3_url,
                "Extraction Timestamp": extraction_time,
                "Update Timestamp": update_time,
                "Verification Timestamp": verification_time,
            }

            circulars_data.append(circular_data)

        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch circular details: {e}")

# Save all circular metadata to a CSV
df = pd.DataFrame(circulars_data)
df.to_csv("RBI_Circulars_Metadata.csv", index=False)

print("Data saved in 'RBI_Circulars_Metadata.csv'. Main circular PDFs downloaded in 'Circulars_PDFs' folder.")
