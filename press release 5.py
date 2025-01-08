from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import csv
from datetime import datetime as dt

# Specify the base URL and the cutoff date
BASE_URL = "https://website.rbi.org.in/web/rbi/press-releases?delta=10&start="
CUTOFF_DATE = dt.strptime("Nov 01, 2024", "%b %d, %Y")  # Change as required

# Set up Selenium WebDriver
driver = webdriver.Chrome()

# Prepare a list to store the extracted data
data = []

page_number = 1  # Start with page 1
stop_processing = False  # Flag to stop processing when old data is reached

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

            # Extract the title and link to the press release
            title_element = release.find_element(By.CLASS_NAME, "mtm_list_item_heading")
            title = title_element.text.strip()
            press_release_url = title_element.get_attribute("href")

            # Navigate to the press release page to get the press release code
            driver.execute_script("window.open(arguments[0]);", press_release_url)
            driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

            try:
                # Wait for the page to load
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "migrated-data-wrap")))

                # Extract the press release code
                press_release_code = driver.find_element(By.XPATH, "//b[contains(text(), 'Press Release')]").text
            except Exception as e:
                print(f"Error fetching press release code: {e}")
                press_release_code = "N/A"

            # Close the tab and switch back to the main window
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

            # Append the extracted information to the list
            data.append({
                "Title": title,
                "Date": date_text,
                "URL": press_release_url,
                "Press Release Code": press_release_code
            })

            print(f"Processed: {title} (Code: {press_release_code})")

        except Exception as e:
            print(f"Error processing a press release: {e}")

    # Move to the next page
    page_number += 1

# Close the Selenium WebDriver
driver.quit()

# Save the extracted data to a CSV file
output_file = "RBI_PressReleases_Metadata_with_Code.csv"
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["Title", "Date", "URL", "Press Release Code"])
    writer.writeheader()
    writer.writerows(data)

print(f"Metadata extraction completed. Saved to {output_file}.")
