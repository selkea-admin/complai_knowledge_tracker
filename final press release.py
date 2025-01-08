from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import os
import csv
from datetime import datetime as dt

# Specify the base URL and the cutoff date
BASE_URL = "https://website.rbi.org.in/web/rbi/press-releases?delta=10&start="
CUTOFF_DATE = dt.strptime("Dec 01, 2024", "%b %d, %Y")  # Adjust as needed

# Set up Selenium WebDriver
driver = webdriver.Chrome()

# Create a directory to save PDFs
os.makedirs("RBI_PressReleases", exist_ok=True)

# Prepare a list to store the extracted data
data = []

page_number = 1
stop_processing = False

while not stop_processing:
    # Construct the URL for the current page
    url = BASE_URL + str(page_number)
    print(f"Processing page: {url}")

    # Load the page
    driver.get(url)

    # Wait for the page to load
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "container")))
    except Exception as e:
        print(f"Error loading page: {e}")
        break

    # Locate all press release blocks on the current page
    press_releases = driver.find_elements(By.CLASS_NAME, "notification-row-each")

    if not press_releases:  # Stop if no press releases are found (end of pagination)
        print("No more press releases found. Stopping.")
        break

    for release in press_releases:
        try:
            # Extract the date of the press release
            date_text = release.find_element(By.CLASS_NAME, "notification-date").text.strip()
            release_date = dt.strptime(date_text, "%b %d, %Y")

            # Skip press releases older than the cutoff date
            if release_date < CUTOFF_DATE:
                print("Reached older press releases. Stopping.")
                stop_processing = True
                break

            # Extract the title, press release URL, and PDF link
            title_element = release.find_element(By.CLASS_NAME, "mtm_list_item_heading")
            title = title_element.text.strip()
            press_release_url = title_element.get_attribute("href")

            # Visit the detailed press release page to get the code and PDF link
            driver.get(press_release_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "migrated-data-wrap")))

            # Extract the press release code
            press_code_element = driver.find_element(By.XPATH, "//b[contains(text(),'Press Release')]")
            press_code = press_code_element.text.strip()

            # Extract the PDF link
            pdf_element = driver.find_element(By.CLASS_NAME, "matomo_download")
            pdf_url = pdf_element.get_attribute("href")

            # Download the PDF
            pdf_response = requests.get(pdf_url)
            pdf_filename = f"RBI_PressReleases/{release_date.strftime('%b %d, %Y')} - {title[:50]}.pdf"
            with open(pdf_filename, "wb") as pdf_file:
                pdf_file.write(pdf_response.content)

            # Append the extracted information to the list
            data.append({
                "PDF Filename": pdf_filename,
                "Type": "Press Release",
                "title": title,
                "Date of Issue": release_date.strftime("%b %d, %Y"),  # Format date as "Nov 29, 2024"
                "Link": press_release_url,
                "Code": press_code,
                "Filtered Links": "",  # Empty column
                "Dept": "",  # Empty column
                "Status": ""  # Empty column
            })

            print(f"Processed and downloaded: {title}")

        except Exception as e:
            print(f"Error processing a press release: {e}")

    # Move to the next page
    page_number += 1

# Close the Selenium WebDriver
driver.quit()

# Save the extracted data to a CSV file
output_file = "RBI_PressReleases_Metadata_final.csv"
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=[
        "PDF Filename", "Type", "title", "Date of Issue", "Link", "Code", 
        "Filtered Links", "Dept", "Status"
    ])
    writer.writeheader()
    writer.writerows(data)

print(f"Metadata extraction and PDF download completed. Saved to {output_file}.")
