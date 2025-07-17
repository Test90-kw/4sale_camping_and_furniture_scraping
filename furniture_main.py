# Import necessary modules for async programming, data handling, file operations, logging, and typing
import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path

# Import custom modules for scraping and uploading
from DetailsScraper import DetailsScraping
from SavingOnDriveFurniture import SavingOnDriveFurniture


class FurnitureMainScraper:
    # Initialize the scraper with furniture data and default settings
    def __init__(self, furnitures_data: Dict[str, List[Tuple[str, int]]]):
        self.furnitures_data = furnitures_data  # Dict of furniture category and list of (url_template, page_count)
        self.chunk_size = 2  # Number of categories to process in one chunk
        self.max_concurrent_links = 2  # Max concurrent requests using semaphore
        self.logger = logging.getLogger(__name__)  # Logger instance
        self.setup_logging()  # Set up logging
        self.temp_dir = Path("temp_files")  # Directory to temporarily save files
        self.temp_dir.mkdir(exist_ok=True)  # Create temp folder if it doesn't exist
        self.upload_retries = 3  # Number of times to retry uploading
        self.upload_retry_delay = 15  # Delay between retries in seconds
        self.page_delay = 3  # Delay between page requests in scraping
        self.chunk_delay = 10  # Delay between chunks

    def setup_logging(self):
        """Initialize logging configuration."""
        stream_handler = logging.StreamHandler()  # Log to console
        file_handler = logging.FileHandler("scraper.log")  # Log to file

        # Basic logging configuration with format and handlers
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[stream_handler, file_handler],
        )
        self.logger.setLevel(logging.INFO)  # Set logger level
        print("Logging setup complete.")  # Notify logging setup

    async def scrape_furniture(self, furniture_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single category."""
        self.logger.info(f"Starting to scrape {furniture_name}")
        card_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Target date = yesterday

        async with semaphore:  # Limit concurrency
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)  # Fill page number in template
                    scraper = DetailsScraping(url)  # Create scraper instance
                    try:
                        cards = await scraper.get_card_details()  # Scrape details from page
                        for card in cards:
                            # Only include if published yesterday
                            if card.get("date_published") and card.get("date_published", "").split()[0] == yesterday:
                                card_data.append(card)
                        await asyncio.sleep(self.page_delay)  # Delay between pages
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return card_data  # Return collected data

    async def save_to_excel(self, furniture_name: str, card_data: List[Dict]) -> str:
        """Save scraped data to an Excel file."""
        if not card_data:
            self.logger.info(f"No data to save for {furniture_name}, skipping Excel file creation.")
            return None

        # Replace problematic characters in filename
        safe_name = furniture_name.replace('/', '_').replace('\\', '_')
        excel_file = Path(f"{safe_name}.xlsx")
        try:
            df = pd.DataFrame(card_data)  # Convert to DataFrame
            df.to_excel(excel_file, index=False)  # Save to Excel
            self.logger.info(f"Successfully saved data for {furniture_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            self.logger.info(f"Checking local files before upload: {files}")
            for file in files:
                self.logger.info(f"File {file} exists: {os.path.exists(file)}, size: {os.path.getsize(file) if os.path.exists(file) else 'N/A'}")

            folder_id = drive_saver.get_folder_id(yesterday)  # Get folder ID for yesterday
            if not folder_id:
                self.logger.info(f"Creating new folder for date: {yesterday}")
                folder_id = drive_saver.create_folder(yesterday)  # Create if not exist
                if not folder_id:
                    raise Exception("Failed to create or get folder ID")
                self.logger.info(f"Created new folder '{yesterday}' with ID: {folder_id}")

            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            file_id = drive_saver.upload_file(file, folder_id)  # Upload file
                            if not file_id:
                                raise Exception("Upload returned no file ID")
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} with ID: {file_id}")
                            break
                        else:
                            self.logger.error(f"File not found for upload: {file}")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            self.logger.info(f"Retrying after {self.upload_retry_delay} seconds...")
                            await asyncio.sleep(self.upload_retry_delay)
                            drive_saver.authenticate()  # Re-authenticate before retry
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")
        except Exception as e:
            self.logger.error(f"Error in upload process: {e}")
            raise

        return uploaded_files  # Return list of uploaded file paths
    
    async def scrape_all_furnitures(self):
        """Scrape all categories and handle uploads."""
        self.temp_dir.mkdir(exist_ok=True)

        # Authenticate and setup Google Drive
        try:
            credentials_json = os.environ.get("FURNITURE_GCLOUD_KEY_JSON")
            if not credentials_json:
                raise EnvironmentError("FURNITURE_GCLOUD_KEY_JSON environment variable not found")
            else:
                self.logger.info("Environment variable FURNITURE_GCLOUD_KEY_JSON is set.")

            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDriveFurniture(credentials_dict)
            drive_saver.authenticate()

            # Verify Drive access
            self.logger.info("Testing Drive API access...")
            try:
                drive_saver.service.files().get(fileId=drive_saver.parent_folder_id).execute()
                self.logger.info("Successfully accessed parent folder")
            except Exception as e:
                self.logger.error(f"Failed to access parent folder: {e}")
                return
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split furniture data into chunks
        furnitures_chunks = [
            list(self.furnitures_data.items())[i : i + self.chunk_size]
            for i in range(0, len(self.furnitures_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(furnitures_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(furnitures_chunks)}")

            tasks = []
            for furniture_name, urls in chunk:
                task = asyncio.create_task(self.scrape_furniture(furniture_name, urls, semaphore))
                tasks.append((furniture_name, task))
                await asyncio.sleep(2)  # Delay between starting each task

            pending_uploads = []
            for furniture_name, task in tasks:
                try:
                    card_data = await task
                    if card_data:
                        excel_file = await self.save_to_excel(furniture_name, card_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {furniture_name}: {e}")

            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                # Delete local files after upload
                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            # Delay before processing next chunk
            if chunk_index < len(furnitures_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)


# Main program execution starts here
if __name__ == "__main__":
    # Define the furniture categories and URLs
    furnitures_data = {
        "نشتري الاثاث المستعمل": [("https://www.q84sale.com/ar/furniture/wanted-furniture/{}", 6)],
        "غرف النوم": [("https://www.q84sale.com/ar/furniture/bedrooms/{}", 5)],
        "غرف الجلوس": [("https://www.q84sale.com/ar/furniture/living-room/{}", 1)],
        "الطاولات": [("https://www.q84sale.com/ar/furniture/tables/{}", 1)],
        "ديوانيات": [("https://www.q84sale.com/ar/furniture/dewaneyah/{}", 3)],
        "مطابخ": [("https://www.q84sale.com/ar/furniture/kitchens/{}", 1)],
        "الديكور والزينة": [("https://www.q84sale.com/ar/furniture/home-decoration/{}", 1)],
        "ادوات منزلية": [("https://www.q84sale.com/ar/furniture/home-supplies/{}", 1)],
        "أثاث مكتبي": [("https://www.q84sale.com/ar/furniture/office-furniture/{}", 1)],
        "تنجيد": [("https://www.q84sale.com/ar/furniture/upholstery/{}", 3)],
        "المفروشات": [("https://www.q84sale.com/ar/furniture/textiles/{}", 5)],
    }

    # Define main coroutine to run the scraper
    async def main():
        scraper = FurnitureMainScraper(furnitures_data)
        await scraper.scrape_all_furnitures()

    # Run the main coroutine
    asyncio.run(main())
