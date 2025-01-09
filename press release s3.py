from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import os
import csv
from datetime import datetime as dt
import boto3
from botocore.exceptions import NoCredentialsError
import re

# Create directory structure
current_dir = os.getcwd()
data_dir = os.path.join(current_dir, "RBI_Data")
pdf_dir = os.path.join(data_dir, "Press_Releases", "PDFs")
csv_dir = os.path.join(data_dir, "Press_Releases", "CSV")

# Create directories if they don't exist
for dir_path in [data_dir, pdf_dir, csv_dir]:
    os.makedirs(dir_path, exist_ok=True)

# AWS S3 Configuration
S3_BUCKET_NAME = "rbi-docs-storage"  # Replace with your S3 bucket name
S3_FOLDER_NAME = "PressReleases"
s3 = boto3.client("s3")

# Base URL for scraping
BASE_URL = "https://website.rbi.org.in/web/rbi/press-releases?delta=10&start="

# Set up Selenium WebDriver
driver = webdriver.Chrome()

# Sanitize filenames to avoid invalid characters
def sanitize_filename(filename, max_length=100):
    sanitized = re.sub(r'[^A-Za-z0-9_-]+', '_', filename)
    return sanitized[:max_length]

# Upload PDF to S3
def upload_to_s3(file_path, s3_filename):
    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, f"{S3_FOLDER_NAME}/{s3_filename}")
        s3_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{S3_FOLDER_NAME}/{s3_filename}"
        print(f"Uploaded to S3: {s3_url}")
        return s3_url
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None

# Extract date from element, handling the 'NEW' tag
def extract_date(date_element):
    date_text = date_element.text.strip().replace('NEW', '').strip()
    try:
        return dt.strptime(date_text, "%b %d, %Y")
    except ValueError as e:
        print(f"Error parsing date '{date_text}': {e}")
        return None

# Prepare a list to store the extracted data
data = []

# Loop through pages from start=1 to start=200
for page_num in range(1, 201):  # From page 1 to 200
    url = BASE_URL + str(page_num)
    print(f"Processing page: {url}")
    
    # Load the page
    driver.get(url)
    
    # Wait for the page to load
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "container")))
    except Exception as e:
        print(f"Error loading page {page_num}: {e}")
        continue  # Skip to the next page if this one fails to load

    # Store the main window handle
    main_window = driver.current_window_handle

    # Locate all press release blocks on the current page
    press_releases = driver.find_elements(By.CLASS_NAME, "notification-row-each")
    print(f"Found {len(press_releases)} press releases on page {page_num}")

    if press_releases:
        # First collect all the URLs and data from the main page
        press_release_data = []
        for release in press_releases:
            try:
                date_element = release.find_element(By.CLASS_NAME, "notification-date")
                release_date = extract_date(date_element)  # Extract date

                if release_date:  # Process all dates
                    title_element = release.find_element(By.CLASS_NAME, "mtm_list_item_heading")
                    press_release_data.append({
                        'date': release_date,
                        'title': title_element.text.strip(),
                        'url': title_element.get_attribute("href")
                    })
            except Exception as e:
                print(f"Error collecting initial data on page {page_num}: {e}")

        # Process each press release URL as before
        for item in press_release_data:
            try:
                print(f"\nProcessing: {item['title']}")
                
                # Open URL in a new tab
                driver.execute_script(f"window.open('{item['url']}', '_blank');")
                
                # Switch to the new tab
                new_window = [window for window in driver.window_handles if window != main_window][-1]
                driver.switch_to.window(new_window)
                
                # Wait for page load
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "migrated-data-wrap")))

                # Extract press release code
                try:
                    press_code_element = driver.find_element(By.XPATH, "//b[contains(text(),'Press Release')]")
                    press_code = press_code_element.text.strip()
                except Exception as e:
                    print(f"Error extracting press code: {e}")
                    press_code = f"PR_{dt.now().strftime('%Y%m%d%H%M%S')}"

                # Extract PDF link
                try:
                    pdf_element = driver.find_element(By.CLASS_NAME, "matomo_download")
                    pdf_url = pdf_element.get_attribute("href")
                except Exception as e:
                    print(f"Error extracting PDF link: {e}")
                    driver.close()
                    driver.switch_to.window(main_window)
                    continue

                # Generate timestamps
                current_time = dt.now()
                extraction_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

                # Create sanitized filename with timestamp
                timestamp = current_time.strftime("%Y%m%d%H%M%S")
                pdf_filename = f"{sanitize_filename(press_code)}_{timestamp}.pdf"
                local_pdf_path = os.path.join(pdf_dir, pdf_filename)

                # Download the PDF
                pdf_response = requests.get(pdf_url)
                with open(local_pdf_path, "wb") as pdf_file:
                    pdf_file.write(pdf_response.content)

                # Upload to S3 and get S3 URL
                s3_url = upload_to_s3(local_pdf_path, pdf_filename)

                # Append the extracted information to the list
                data.append({
                    "PDF Filename": pdf_filename,
                    "Type": "Press Release",
                    "Title": item['title'],
                    "Date of Issue": item['date'].strftime("%b %d, %Y"),
                    "Link": item['url'],
                    "Code": press_code,
                    "PDF Link": pdf_url,
                    "S3 URL": s3_url if s3_url else "",
                    "Extraction Timestamp": extraction_time
                })

                print(f"Processed and downloaded: {item['title']}")

                # Close the tab and switch back to main window
                driver.close()
                driver.switch_to.window(main_window)

            except Exception as e:
                print(f"Error processing press release: {e}")
                # Make sure we're back on the main window
                if len(driver.window_handles) > 1:
                    driver.close()
                driver.switch_to.window(main_window)

# Close the Selenium WebDriver
driver.quit()

# Generate timestamp for CSV filename
csv_timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
output_file = os.path.join(csv_dir, f"RBI_PressReleases_Metadata_{csv_timestamp}.csv")

# Save the extracted data to a CSV file
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=[
        "PDF Filename", "Type", "Title", "Date of Issue", "Link", "Code", 
        "PDF Link", "S3 URL", "Extraction Timestamp"
    ])
    writer.writeheader()
    writer.writerows(data)

print(f"Metadata extraction and PDF download completed. Saved to {output_file}.")
