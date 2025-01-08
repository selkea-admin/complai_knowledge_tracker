# import os
# import requests
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# import time
# import pandas as pd
# import re
# import boto3
# from botocore.exceptions import NoCredentialsError

# # AWS S3 Configuration
# S3_BUCKET_NAME = "rbi-docs-storage"
# S3_FOLDER_NAME = "Notifications/"
# s3 = boto3.client("s3")

# # Create a folder to store the scraped files locally
# folder = "RBI_Directions_Notifications"
# pdf_folder = os.path.join(folder, "PDFs")
# if not os.path.exists(folder):
#     os.mkdir(folder)
# if not os.path.exists(pdf_folder):
#     os.mkdir(pdf_folder)

# months = ["January", "February", "March", "April", "May", "June",
#           "July", "August", "September", "October", "November", "December"]

# def extract_until_month(text, months):
#     for month in months:
#         if month in text:
#             return text.split(month, 1)[0]
#     return text

# chrome_options = Options()
# chrome_options.add_argument("--disable-notifications")
# chrome_options.add_argument("--start-maximized")

# # Initialize WebDriver
# driver = webdriver.Chrome(options=chrome_options)

# # Sanitize filenames to avoid invalid characters
# def sanitize_filename(filename):
#     return re.sub(r'[^A-Za-z0-9_-]+', '_', filename)

# # Upload PDF to S3
# def upload_to_s3(file_path, s3_filename):
#     try:
#         s3.upload_file(file_path, S3_BUCKET_NAME, f"{S3_FOLDER_NAME}/{s3_filename}")
#         print(f"Uploaded to S3: {s3_filename}")
#     except FileNotFoundError:
#         print("The file was not found")
#     except NoCredentialsError:
#         print("Credentials not available")

# # Extract table data
# def extract_table(limit):
#     data = []
#     for i in range(1, limit + 1):
#         try:
#             title = driver.find_element(
#                 By.XPATH,
#                 f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a/div[1]/span"
#             ).text
#         except:
#             title = "N/A"
        
#         try:
#             date = driver.find_element(
#                 By.XPATH,
#                 f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/div[1]/span"
#             ).text
#         except:
#             date = "N/A"

#         try:
#             link = driver.find_element(
#                 By.XPATH,
#                 f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a"
#             ).get_attribute("href")
#         except:
#             link = "N/A"

#         try:
#             code = driver.find_element(
#                 By.XPATH,
#                 f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a/div[1]/div/div[2]"
#             ).get_attribute('innerHTML')
#             code = extract_until_month(str(code), months).replace("<p>", "").replace("</p>", "").strip()
#         except:
#             code = "N/A"
        
#         try:
#             pdf_link = driver.find_element(
#                 By.XPATH,
#                 f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/div[4]/a"
#             ).get_attribute("href")
#         except:
#             pdf_link = "N/A"

#         if pdf_link != "N/A":
#             try:
#                 pdf_filename = f"{sanitize_filename(code)}.pdf" if code != "N/A" else f"Notification_{i}.pdf"
#                 pdf_filepath = os.path.join(pdf_folder, pdf_filename)

#                 pdf_response = requests.get(pdf_link, stream=True)
#                 if pdf_response.status_code == 200:
#                     with open(pdf_filepath, "wb") as pdf_file:
#                         pdf_file.write(pdf_response.content)
#                     print(f"Downloaded PDF: {pdf_filename}")
                    
#                     # Upload to S3
#                     upload_to_s3(pdf_filepath, pdf_filename)
#                 else:
#                     print(f"Failed to download PDF: {pdf_link}, Status code: {pdf_response.status_code}")
#             except Exception as e:
#                 print(f"Error downloading PDF from {pdf_link}: {e}")

#         data.append({
#             "Title": title,
#             "Date Of Issue": date,
#             "Link": link,
#             "Code": code,
#             "PDF Link": pdf_link
#         })
#     return data

# all_data_df = pd.DataFrame()

# for page in range(1, 201):
#     url = f"https://website.rbi.org.in/web/rbi/notifications?delta=10&start={page}"
#     driver.get(url)
#     time.sleep(5)

#     try:
#         rows = driver.find_elements(
#             By.XPATH,
#             "//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div"
#         )
#         limit = len(rows)
#     except Exception as e:
#         print(f"Error locating rows: {e}")
#         limit = 0

#     if limit > 0:
#         data = extract_table(limit)
#         df = pd.DataFrame(data)
#         all_data_df = pd.concat([all_data_df, df], ignore_index=True)

# csv_filename = os.path.join(folder, "rbi_notifications.csv")
# all_data_df.to_csv(csv_filename, index=False)

# print(all_data_df)

# driver.quit()

import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# AWS S3 Configuration
S3_BUCKET_NAME = "rbi-docs-storage"
S3_FOLDER_NAME = "Notifications"
s3 = boto3.client("s3")

# Create a folder to store the scraped files locally
folder = "RBI_Notifications"
pdf_folder = os.path.join(folder, "PDFs")
if not os.path.exists(folder):
    os.mkdir(folder)
if not os.path.exists(pdf_folder):
    os.mkdir(pdf_folder)

months = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]

def extract_until_month(text, months):
    for month in months:
        if month in text:
            return text.split(month, 1)[0]
    return text

chrome_options = Options()
chrome_options.add_argument("--disable-notifications")
chrome_options.add_argument("--start-maximized")

# Initialize WebDriver
driver = webdriver.Chrome(options=chrome_options)

# Sanitize filenames to avoid invalid characters
def sanitize_filename(filename, max_length=100):
    sanitized = re.sub(r'[^A-Za-z0-9_-]+', '_', filename)
    return sanitized[:max_length]

# Upload PDF to S3
def upload_to_s3(file_path, s3_filename):
    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, f"{S3_FOLDER_NAME}/{s3_filename}")
        s3_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{S3_FOLDER_NAME}{s3_filename}"
        print(f"Uploaded to S3: {s3_url}")
        return s3_url
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None

# Extract table data
def extract_table(limit):
    data = []
    for i in range(1, limit + 1):
        try:
            title = driver.find_element(
                By.XPATH,
                f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a/div[1]/span"
            ).text
        except:
            title = "N/A"

        try:
            date = driver.find_element(
                By.XPATH,
                f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/div[1]/span"
            ).text
        except:
            date = "N/A"

        try:
            link = driver.find_element(
                By.XPATH,
                f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a"
            ).get_attribute("href")
        except:
            link = "N/A"

        try:
            code = driver.find_element(
                By.XPATH,
                f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/a/div[1]/div/div[2]"
            ).get_attribute('innerHTML')
            code = extract_until_month(str(code), months).replace("<p>", "").replace("</p>", "").strip()
        except:
            code = "N/A"

        try:
            pdf_link = driver.find_element(
                By.XPATH,
                f"//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div[{i}]/div/div/div[4]/a"
            ).get_attribute("href")
        except:
            pdf_link = "N/A"

        pdf_filename = "N/A"
        s3_url = "N/A"
        extraction_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        update_time = extraction_time
        verify_time = extraction_time

        if pdf_link != "N/A":
            try:
                # Add timestamp for uniqueness
                timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                pdf_filename = f"{sanitize_filename(code)}_{timestamp}.pdf" if code != "N/A" else f"Notification_{i}_{timestamp}.pdf"
                pdf_filepath = os.path.join(pdf_folder, pdf_filename)

                pdf_response = requests.get(pdf_link, stream=True)
                if pdf_response.status_code == 200:
                    with open(pdf_filepath, "wb") as pdf_file:
                        pdf_file.write(pdf_response.content)
                    print(f"Downloaded PDF: {pdf_filename}")

                    # Upload to S3
                    s3_url = upload_to_s3(pdf_filepath, pdf_filename)
                else:
                    print(f"Failed to download PDF: {pdf_link}, Status code: {pdf_response.status_code}")
            except Exception as e:
                print(f"Error downloading PDF from {pdf_link}: {e}")

        data.append({
            "Title": title,
            "Date Of Issue": date,
            "Link": link,
            "Code": code,
            "PDF Link": pdf_link,
            "PDF Filename": pdf_filename,
            "S3 URL": s3_url,
            "Extraction Timestamp": extraction_time,
            "Update Timestamp": update_time,
            "Verification Timestamp": verify_time
        })

    return data

# Scrape data from multiple pages
all_data_df = pd.DataFrame()

for page in range(1, 201):
    url = f"https://website.rbi.org.in/web/rbi/notifications?delta=10&start={page}"
    driver.get(url)
    time.sleep(5)

    try:
        rows = driver.find_elements(
            By.XPATH,
            "//*[@id='portlet_com_liferay_portal_search_web_search_results_portlet_SearchResultsPortlet_INSTANCE_whkb']/div/div[2]/div/div/div/div/div[2]/div/div/div"
        )
        limit = len(rows)
    except Exception as e:
        print(f"Error locating rows: {e}")
        limit = 0

    if limit > 0:
        data = extract_table(limit)
        df = pd.DataFrame(data)
        all_data_df = pd.concat([all_data_df, df], ignore_index=True)

# Save data to CSV
csv_filename = os.path.join(folder, "rbi_notifications_with_metadata.csv")
all_data_df.to_csv(csv_filename, index=False)

print(f"CSV saved at: {csv_filename}")
driver.quit()
