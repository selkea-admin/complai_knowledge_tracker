from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import time
import pandas as pd
import requests
from datetime import datetime
import re
import boto3
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
BUCKET_NAME = "rbi-docs-storage"
FOLDER_NAME = "MasterDirections"
s3_client = boto3.client('s3')

# Create folders to store scraped files and PDFs
folder = "RBI_Directions"
pdf_folder = "RBI_PDFs"
os.makedirs(folder, exist_ok=True)
os.makedirs(pdf_folder, exist_ok=True)

# Setup Chrome driver
chrome_options = Options()
chrome_options.add_argument("--headless")  # Uncomment this to run in headless mode
chrome_options.add_argument("--disable-notifications")
driver = webdriver.Chrome(options=chrome_options)

# Function to sanitize filenames
def sanitize_filename(filename):
    sanitized = re.sub(r"[^a-zA-Z0-9_\-]", "_", filename)
    return sanitized[:50]

# Function to upload file to S3
def upload_to_s3(file_path, object_name):
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, object_name)
        s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{object_name}"
        return s3_url
    except NoCredentialsError:
        print("AWS credentials not found.")
        return None
    except Exception as e:
        print(f"Error uploading {file_path} to S3: {e}")
        return None

# Function to extract document code and departments
def extract_code_and_departments(direction_link):
    try:
        # Visit the direction page
        driver.get(direction_link)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)  # Wait for JavaScript content to load

        # Get page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract all text content
        content_sections = []
        for tag in soup.find_all(['p', 'div', 'span']):  # Include relevant tags
            text = tag.get_text(strip=True)
            if text:
                content_sections.append(text)

        # Extract document code and departments
        document_code = None
        departments = None
        for section in content_sections:
            if re.match(r'^RBI\/.*$', section):  # Pattern for document code
                document_code = section
            elif any(keyword in section.lower() for keyword in ['chairman', 'managing director', 'chief executive officer']):
                departments = section

        return document_code, departments

    except Exception as e:
        print(f"Error extracting document code and departments: {e}")
        return None, None

# Function to extract table data and upload PDFs
def extract_table():
    data = []
    for i in range(1, limit + 1):
        try:
            # Extract title, date, and link for each direction entry
            title = driver.find_element(By.XPATH, f"/html/body/div[3]/section/div/div[10]/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a/div[1]/span").text
            date_text = driver.find_element(By.XPATH, f"/html/body/div[3]/section/div/div[10]/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/div[1]/span").text
            direction_link = driver.find_element(By.XPATH, f"/html/body/div[3]/section/div/div[10]/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a").get_attribute("href")

            # Extract document code and departments
            document_code, departments = extract_code_and_departments(direction_link)

            # Download and upload PDF if available
            pdf_url = None
            pdf_filename = None
            for link in driver.find_elements(By.TAG_NAME, "a"):
                href = link.get_attribute("href")
                if href and "pdf" in href.lower():
                    pdf_filename = sanitize_filename(f"{title}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
                    pdf_path = os.path.join(pdf_folder, pdf_filename)
                    with open(pdf_path, "wb") as pdf_file:
                        pdf_file.write(requests.get(href).content)

                    # Upload to S3
                    pdf_url = upload_to_s3(pdf_path, f"{FOLDER_NAME}/{pdf_filename}")

            # Append data
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data.append({
                "Title": title,
                "Date Of Issue": date_text,
                "Link": direction_link,
                "Document Code": document_code,
                "Departments": departments,
                "PDF Filename": pdf_filename,
                "S3 URL": pdf_url,
                "Extraction Timestamp": timestamp,
                "Update Timestamp": timestamp,
                "Verification Timestamp": timestamp,
            })

            driver.back()
            time.sleep(3)

        except Exception as e:
            print(f"Error processing entry {i}: {e}")
            continue

    return data

# Initialize an empty DataFrame to store all data
all_data_df = pd.DataFrame()

# Loop through pages to collect data
for page in range(1, 14):  # Adjust the range for the total number of pages
    link = f"https://website.rbi.org.in/web/rbi/notifications/master-directions?delta=100&start={page}"
    driver.get(link)
    time.sleep(5)

    # Count the number of rows to set the limit
    rows = driver.find_elements(By.XPATH, "/html/body/div[3]/section/div/div[10]/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/div")
    limit = len(rows)

    if limit > 0:
        # Extract data for the current page
        data = extract_table()
        df = pd.DataFrame(data)
        all_data_df = pd.concat([all_data_df, df], ignore_index=True)

# Final save to the CSV file
csv_filename = os.path.join(folder, "rbi_master_directions_metadata.csv")
all_data_df.to_csv(csv_filename, index=False)

# # Upload final CSV to S3
# csv_s3_url = upload_to_s3(csv_filename, f"{FOLDER_NAME}/rbi_master_directions_final.csv")
# print(f"CSV uploaded to S3: {csv_s3_url}")

# Close the browser
driver.quit()