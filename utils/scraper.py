import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import time
from urllib.parse import urljoin

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    SELENIUM_AVAILABLE = True

    # Try to import webdriver_manager for automatic driver management
    try:
        from webdriver_manager.chrome import ChromeDriverManager

        WEBDRIVER_MANAGER_AVAILABLE = True
    except ImportError:
        WEBDRIVER_MANAGER_AVAILABLE = False

except ImportError:
    SELENIUM_AVAILABLE = False
    WEBDRIVER_MANAGER_AVAILABLE = False
    print("Warning: Selenium not installed. Install with: pip install selenium webdriver-manager")


class BirdSpeciesScraper:
    """Scraper for bird species data from aves.regoch.net."""

    def __init__(self, base_url: str, timeout: int = 30, retry_attempts: int = 3):
        """
        Initialize the scraper.

        Args:
            base_url: Base URL to scrape
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts
        """
        self.base_url = base_url
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.driver = None

    def scrape_species_list(self) -> List[Dict[str, Any]]:
        """
        Scrape the list of all bird species from the HTML table.

        Returns:
            List of species data dictionaries with full GBIF details
        """

        return self._scrape_with_selenium()

    def _scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """
        Scrape species list using Selenium to handle JavaScript rendering.

        Returns:
            List of species data dictionaries
        """
        species_list = []

        try:
            # Setup headless Chrome
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')

            # Use webdriver_manager if available for automatic version matching
            if WEBDRIVER_MANAGER_AVAILABLE:
                print("Using webdriver-manager for automatic ChromeDriver version matching...")
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                print("Using system ChromeDriver (may cause version mismatch issues)...")
                print("Tip: Install webdriver-manager with: pip install webdriver-manager")
                self.driver = webdriver.Chrome(options=chrome_options)

            self.driver.set_page_load_timeout(self.timeout)

            # Load the page
            index_url = urljoin(self.base_url, 'index.html')
            print(f"Loading page with Selenium: {index_url}")
            self.driver.get(index_url)

            # Wait for the table to be populated with data
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#speciesTable tbody tr')))

            # Give JavaScript a moment to finish rendering
            time.sleep(1)

            # Pagination loop - keep clicking "Next" until no more pages
            page_number = 1
            all_species_keys = []  # Collect all species keys first to avoid issues with page changes
            previous_first_key = None  # Track first item of previous page to detect when we're stuck

            while True:
                print(f"Processing page {page_number}...")

                # Wait for table to be populated
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#speciesTable tbody tr')))
                time.sleep(0.5)  # Small delay for JavaScript to finish

                # Get the page source after JavaScript execution
                soup = BeautifulSoup(self.driver.page_source, 'lxml')

                # Find all species links on current page
                table = soup.find('table', id='speciesTable')
                if not table:
                    print("Species table not found")
                    break

                tbody = table.find('tbody')
                if not tbody:
                    print("Table body not found")
                    break

                species_links = tbody.find_all('a')
                print(f"Found {len(species_links)} species on page {page_number}")

                # Extract species keys from current page
                current_page_keys = []
                for link in species_links:
                    href = link.get('href', '')
                    if 'details.html?id=' in href:
                        species_key = href.split('id=')[-1]
                        current_page_keys.append(species_key)
                        if species_key not in all_species_keys:
                            all_species_keys.append(species_key)

                # Check if we're on the last page by comparing first item with previous page
                if current_page_keys:
                    current_first_key = current_page_keys[0]
                    if previous_first_key == current_first_key:
                        print(f"Reached last page (page {page_number}): first item unchanged after clicking Next")
                        break
                    previous_first_key = current_first_key

                # Try to click the "Next" button
                try:
                    next_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Next')]")

                    # Check if button is disabled (last page reached)
                    if not next_button.is_enabled():
                        print("Reached last page (Next button disabled)")
                        break

                    # Click the Next button
                    next_button.click()
                    print(f"Clicked Next button, moving to page {page_number + 1}")

                    # Small delay for JavaScript to update the table
                    time.sleep(0.5)
                    page_number += 1

                except Exception as e:
                    print(f"No more pages or error clicking Next button: {e}")
                    break

            print(f"Total species found across all pages: {len(all_species_keys)}")

            # Now scrape details for all collected species
            for idx, species_key in enumerate(all_species_keys, 1):
                details_url = urljoin(self.base_url, f'details.html?id={species_key}')

                print(f"Scraping species {idx}/{len(all_species_keys)}: key={species_key}")

                # Use Selenium to scrape details page (reuses self.driver)
                species_data = self.scrape_species_page(details_url, species_key)
                if species_data:
                    species_list.append(species_data)

                # Small delay between requests
                time.sleep(0.3)

        except Exception as e:
            print(f"Error during Selenium scraping: {e}")
        finally:
            # Clean up driver after all scraping is done
            if self.driver:
                self.driver.quit()
                self.driver = None

        return species_list

    def scrape_species_page(self, url: str, species_key: str) -> Dict[str, Any]:
        """
        Scrape individual species details page.

        Args:
            url: URL of the species details page
            species_key: GBIF species key

        Returns:
            Dictionary containing species data in GBIF format
        """
        return self._scrape_species_page_with_selenium(url, species_key)

    def _scrape_species_page_with_selenium(self, url: str, species_key: str) -> Dict[str, Any]:
        """
        Scrape species details page using Selenium (for JavaScript-rendered content).

        Args:
            url: URL of the species details page
            species_key: GBIF species key

        Returns:
            Dictionary containing species data in GBIF format
        """
        species_data: Dict[str, Any] = {'key': int(species_key)}

        try:
            # Ensure driver exists
            if not self.driver:
                chrome_options = Options()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--window-size=1920,1080')

                if WEBDRIVER_MANAGER_AVAILABLE:
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    self.driver = webdriver.Chrome(options=chrome_options)

                self.driver.set_page_load_timeout(self.timeout)

            # Load the details page
            self.driver.get(url)

            # Wait for the details to be populated by JavaScript
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#details dd')))

            # Give JavaScript time to finish rendering
            time.sleep(0.3)

            # Parse the rendered HTML
            soup = BeautifulSoup(self.driver.page_source, 'lxml')

            # Find the details definition list
            details_dl = soup.find('dl', id='details')
            if not details_dl:
                print(f"Details section not found for key={species_key}")
                return {}

            # Parse all dt/dd pairs
            dts = details_dl.find_all('dt')
            dds = details_dl.find_all('dd')

            if len(dts) != len(dds):
                print(f"Warning: Mismatch between labels ({len(dts)}) and values ({len(dds)})")

            # Create a mapping of labels to values
            for dt, dd in zip(dts, dds):
                label = dt.text.strip().rstrip(':').lower()
                value = dd.text.strip()

                if not value:
                    continue

                # Map HTML labels to GBIF field names
                if label == 'scientific name':
                    species_data['scientificName'] = value
                elif label == 'canonical name':
                    species_data['canonicalName'] = value
                elif label == 'rank':
                    species_data['rank'] = value
                elif label == 'kingdom':
                    species_data['kingdom'] = value
                elif label == 'phylum':
                    species_data['phylum'] = value
                elif label == 'class':
                    species_data['class'] = value
                elif label == 'order':
                    species_data['order'] = value
                elif label == 'family':
                    species_data['family'] = value
                elif label == 'genus':
                    species_data['genus'] = value

            # Validate we have at least the key fields
            if 'scientificName' not in species_data:
                print(f"Missing scientific name for key={species_key}")
                return {}

        except Exception as e:
            print(f"Error parsing species data with Selenium for key={species_key}: {e}")
            return {}

        return species_data

    def close(self):
        """Clean up resources (e.g., Selenium driver)."""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
