import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
from urllib.parse import urljoin

class NFAMADownloader:
    def __init__(self, download_path="./nfama_downloads"):
        """
        Initialize the NFAMA downloader
        
        Args:
            download_path (str): Path where files will be downloaded
        """
        self.base_url = "https://vff.no/siste-m%C3%A5ned"
        self.download_path = os.path.abspath(download_path)
        self.driver = None
        
        # Create download directory if it doesn't exist
        os.makedirs(self.download_path, exist_ok=True)
        
    def setup_driver(self):
        """Setup Chrome webdriver with download preferences"""
        chrome_options = Options()
        
        # Configure download settings
        prefs = {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # --- CHANGE MADE HERE ---
        # Run in headless mode (browser window will not appear)
        chrome_options.add_argument("--headless")
        
        # Additional options to handle popups and overlays
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            # Setting window size is still useful in headless mode
            # as it can affect how responsive pages are rendered.
            self.driver.set_window_size(1920, 1080) 
            return True
        except Exception as e:
            print(f"Error setting up Chrome driver: {e}")
            print("Make sure ChromeDriver is installed and in PATH")
            return False
    
    def navigate_to_source(self):
        """Navigate to the NFAMA statistics page"""
        try:
            print(f"Navigating to: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Dismiss any popups/cookie banners immediately
            time.sleep(2)  # Give popups time to appear
            self.dismiss_popups()
            
            print("Page loaded successfully")
            return True
            
        except TimeoutException:
            print("Timeout waiting for page to load")
            return False
        except Exception as e:
            print(f"Error navigating to source: {e}")
            return False
    
    def dismiss_popups(self):
        """Dismiss any popups, cookie banners, or overlays"""
        try:
            # Common selectors for cookie banners and popups
            popup_selectors = [
                "button[id*='accept']",
                "button[class*='accept']",
                "button[class*='cookie']",
                ".cookie-banner button",
                "#cookie-banner button",
                "button[id*='close']",
                ".popup-close",
                ".modal-close",
                "[data-dismiss='modal']"
            ]
            
            for selector in popup_selectors:
                try:
                    # Use a short wait time to avoid slowing down the script if no popup exists
                    popup_button = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if popup_button.is_displayed():
                        self.driver.execute_script("arguments[0].click();", popup_button)
                        print(f"Dismissed popup using selector: {selector}")
                        time.sleep(1)
                        break
                except:
                    continue
                    
        except Exception as e:
            print(f"Note: Could not dismiss popups: {e}")

    def download_file_by_text(self, link_text, expected_filename_pattern=None):
        """
        Download a file by finding a link with specific text
        
        Args:
            link_text (str): Text content of the link to find
            expected_filename_pattern (str): Pattern to verify downloaded file
        
        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            print(f"Looking for download link containing: '{link_text}'")
            
            # Dismiss any popups first
            self.dismiss_popups()
            
            # Find the download link by partial text match
            link_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, link_text))
            )
            
            # Get the download URL
            download_url = link_element.get_attribute("href")
            print(f"Found download URL: {download_url}")
            
            # Scroll the element into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", link_element)
            time.sleep(1)
            
            # Try multiple click methods
            click_successful = False
            
            # Method 1: Regular click
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, link_text))
                )
                link_element.click()
                click_successful = True
                print(f"Clicked download link for: {link_text} (regular click)")
            except:
                pass
            
            # Method 2: JavaScript click if regular click failed
            if not click_successful:
                try:
                    self.driver.execute_script("arguments[0].click();", link_element)
                    click_successful = True
                    print(f"Clicked download link for: {link_text} (JavaScript click)")
                except:
                    pass
            
            # Method 3: Direct download using requests if click failed
            if not click_successful:
                try:
                    print(f"Attempting direct download for: {link_text}")
                    response = requests.get(download_url)
                    if response.status_code == 200:
                        # Extract filename from URL
                        filename = download_url.split('/')[-1]
                        filepath = os.path.join(self.download_path, filename)
                        
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        
                        click_successful = True
                        print(f"Direct download successful for: {link_text}")
                    else:
                        print(f"Direct download failed with status: {response.status_code}")
                except Exception as direct_error:
                    print(f"Direct download error: {direct_error}")
            
            if click_successful:
                # Wait a bit for download to start
                time.sleep(3)
                return True
            else:
                print(f"All click methods failed for: {link_text}")
                return False
            
        except TimeoutException:
            print(f"Could not find download link for: {link_text}")
            return False
        except Exception as e:
            print(f"Error downloading {link_text}: {e}")
            return False
    
    def download_nfama_files(self):
        """Download both required NFAMA files"""
        files_to_download = [
            "Norske personkunder",
            "Pensjonsmidler med fondsvalg"
        ]
        
        successful_downloads = []
        failed_downloads = []
        
        for file_text in files_to_download:
            print(f"\n--- Downloading: {file_text} ---")
            
            if self.download_file_by_text(file_text):
                successful_downloads.append(file_text)
                print(f"✓ Successfully initiated download for: {file_text}")
            else:
                failed_downloads.append(file_text)
                print(f"✗ Failed to download: {file_text}")
        
        return successful_downloads, failed_downloads
    
    def wait_for_downloads(self, timeout=60):
        """Wait for downloads to complete by checking for .crdownload files"""
        print("\nWaiting for downloads to complete...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check for any .crdownload files (Chrome's partial download files)
            crdownload_files = [f for f in os.listdir(self.download_path) 
                              if f.endswith('.crdownload')]
            
            if not crdownload_files:
                print("All downloads completed!")
                break
                
            print(f"Still downloading... ({len(crdownload_files)} files remaining)")
            time.sleep(2)
        else:
            print("Timeout waiting for downloads to complete")
    
    def list_downloaded_files(self):
        """List all files in the download directory"""
        try:
            files = os.listdir(self.download_path)
            excel_files = [f for f in files if f.endswith(('.xlsx', '.xls'))]
            
            print(f"\nDownloaded files in {self.download_path}:")
            for file in excel_files:
                file_path = os.path.join(self.download_path, file)
                file_size = os.path.getsize(file_path)
                print(f"  - {file} ({file_size:,} bytes)")
            
            return excel_files
            
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def run(self):
        """Main execution method"""
        print("NFAMA Data Downloader Starting...")
        print(f"Download directory: {self.download_path}")
        
        try:
            # Setup webdriver
            if not self.setup_driver():
                return False
            
            # Navigate to the source page
            if not self.navigate_to_source():
                return False
            
            # Download the required files
            successful, failed = self.download_nfama_files()
            
            # Wait for downloads to complete
            self.wait_for_downloads()
            
            # List downloaded files
            downloaded_files = self.list_downloaded_files()
            
            # Print summary
            print(f"\n=== Download Summary ===")
            print(f"Successful downloads: {len(successful)}")
            print(f"Failed downloads: {len(failed)}")
            print(f"Files in download folder: {len(downloaded_files)}")
            
            if failed:
                print(f"Failed to download: {', '.join(failed)}")
            
            return len(failed) == 0
            
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
            
        finally:
            if self.driver:
                self.driver.quit()
                print("Browser closed")

def main():
    """Main function to run the downloader"""
    # You can customize the download path here
    downloader = NFAMADownloader(download_path="./nfama_data")
    
    success = downloader.run()
    
    if success:
        print("\n✓ All downloads completed successfully!")
    else:
        print("\n✗ Some downloads failed. Check the logs above.")

if __name__ == "__main__":
    main()