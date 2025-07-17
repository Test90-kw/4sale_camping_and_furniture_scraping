# Import necessary modules
import asyncio  # For running async operations
import pandas as pd  # For handling dataframes and Excel writing
import os  # For file operations and environment variables
import json  # For parsing JSON credentials
import logging  # For logging events and errors
from datetime import datetime, timedelta  # For date/time calculations
from typing import Dict, List, Tuple  # Type hints for dictionaries and lists
from pathlib import Path  # For file path management
from DetailsScraper import DetailsScraping  # Custom class to scrape individual detail pages
from SavingOnDriveCamping import SavingOnDriveCamping  # Custom class for Google Drive operations


# Main class to handle camping data scraping and uploading
class CampingMainScraper:
    def __init__(self, campings_data: Dict[str, List[Tuple[str, int]]]):
        # Store camping data: dict of category names to list of (URL, page count)
        self.campings_data = campings_data

        # Define how many categories to scrape in parallel (chunks)
        self.chunk_size = 2

        # Max concurrent page requests per chunk
        self.max_concurrent_links = 2

        # Create a logger instance
        self.logger = logging.getLogger(__name__)
        self.setup_logging()  # Set up logging output

        # Directory to store temporary files (Excel)
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)

        # Retry parameters for file upload
        self.upload_retries = 3
        self.upload_retry_delay = 15  # seconds

        # Delay settings
        self.page_delay = 3  # Between pages
        self.chunk_delay = 10  # Between chunks

    def setup_logging(self):
        """Initialize logging configuration."""
        # Log to both console and file
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("scraper.log")

        # Set logging format and level
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[stream_handler, file_handler],
        )
        self.logger.setLevel(logging.INFO)
        print("Logging setup complete.")

    async def scrape_camping(self, camping_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single category."""
        self.logger.info(f"Starting to scrape {camping_name}")
        card_data = []

        # Determine yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Limit concurrent access using semaphore
        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)  # Format URL with page number
                    scraper = DetailsScraping(url)
                    try:
                        cards = await scraper.get_card_details()  # Get card listings
                        for card in cards:
                            # Only save cards posted yesterday
                            if card.get("date_published") and card.get("date_published", "").split()[0] == yesterday:
                                card_data.append(card)
                        await asyncio.sleep(self.page_delay)  # Delay between pages
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return card_data

    async def save_to_excel(self, camping_name: str, card_data: List[Dict]) -> str:
        """Save scraped data to an Excel file."""
        if not card_data:
            self.logger.info(f"No data to save for {camping_name}, skipping Excel file creation.")
            return None

        # Make filename safe by removing problematic characters
        safe_name = camping_name.replace('/', '_').replace('\\', '_')
        excel_file = Path(f"{safe_name}.xlsx")

        try:
            df = pd.DataFrame(card_data)  # Convert to DataFrame
            df.to_excel(excel_file, index=False)  # Save as Excel
            self.logger.info(f"Successfully saved data for {camping_name}")
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

            # Ensure the destination folder exists
            folder_id = drive_saver.get_folder_id(yesterday)
            if not folder_id:
                self.logger.info(f"Creating new folder for date: {yesterday}")
                folder_id = drive_saver.create_folder(yesterday)
                if not folder_id:
                    raise Exception("Failed to create or get folder ID")
                self.logger.info(f"Created new folder '{yesterday}' with ID: {folder_id}")

            # Upload each file with retries
            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            file_id = drive_saver.upload_file(file, folder_id)
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

        return uploaded_files

    async def scrape_all_campings(self):
        """Scrape all categories and handle uploads."""
        self.temp_dir.mkdir(exist_ok=True)

        # Set up Google Drive authentication
        try:
            credentials_json = os.environ.get("CAMPINGS_GCLOUD_KEY_JSON")
            if not credentials_json:
                raise EnvironmentError("CAMPINGS_GCLOUD_KEY_JSON environment variable not found")
            else:
                self.logger.info("Environment variable CAMPINGS_GCLOUD_KEY_JSON is set.")

            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDriveCamping(credentials_dict)
            drive_saver.authenticate()
            self.logger.info("Testing Drive API access...")
            try:
                # Test access to the parent folder
                drive_saver.service.files().get(fileId=drive_saver.parent_folder_id).execute()
                self.logger.info("Successfully accessed parent folder")
            except Exception as e:
                self.logger.error(f"Failed to access parent folder: {e}")
                return
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split all categories into chunks of 2 for processing
        campings_chunks = [
            list(self.campings_data.items())[i : i + self.chunk_size]
            for i in range(0, len(self.campings_data), self.chunk_size)
        ]

        # Semaphore to control concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        # Process each chunk
        for chunk_index, chunk in enumerate(campings_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(campings_chunks)}")

            tasks = []
            for camping_name, urls in chunk:
                task = asyncio.create_task(self.scrape_camping(camping_name, urls, semaphore))
                tasks.append((camping_name, task))
                await asyncio.sleep(2)  # Short delay between starting tasks

            pending_uploads = []
            for camping_name, task in tasks:
                try:
                    card_data = await task
                    if card_data:
                        excel_file = await self.save_to_excel(camping_name, card_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {camping_name}: {e}")

            # Upload files and delete locally after upload
            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            if chunk_index < len(campings_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

if __name__ == "__main__":
    # Define all camping categories and URLs with number of pages to scrape
    campings_data = {
        "نطاطيات": [("https://www.q84sale.com/ar/camping/trampoline-for-rent/{}", 2)],
        "أغراض المخيمات": [("https://www.q84sale.com/ar/camping/camping-stuff/{}", 5)],
        "الخيام": [("https://www.q84sale.com/ar/camping/tents/{}", 4)],
        "معدات الصيد": [("https://www.q84sale.com/ar/camping/hunting-equipment/{}", 1)],
        "مولدات كهربائية": [("https://www.q84sale.com/ar/camping/generators/{}", 3)],
        "كشتات": [("https://www.q84sale.com/ar/camping/picnics/{}", 2)],
        "كبائن متنقله": [("https://www.q84sale.com/ar/camping/caravans/{}", 6)],
        "الطاقة الشمسية": [("https://www.q84sale.com/ar/camping/solar-power/{}", 2)],
        "للايجار مخيم": [("https://www.q84sale.com/ar/camping/camps-for-rent/{}", 5)],
        "الفحم": [("https://www.q84sale.com/ar/camping/coals/{}", 1)],
        "ستلايت البر": [("https://www.q84sale.com/ar/camping/satellite-camping/{}", 1)],
        "حفلات البر": [("https://www.q84sale.com/ar/camping/barbecue/{}", 1)],
    }


    
    async def main():
        scraper = CampingMainScraper(campings_data)
        await scraper.scrape_all_campings()


    asyncio.run(main())
