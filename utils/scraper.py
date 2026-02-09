"""Web scraper for bird species data."""

import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import time
from urllib.parse import urljoin


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

    def scrape_species_list(self) -> List[Dict[str, Any]]:
        """
        Scrape the list of all bird species.

        Returns:
            List of species data dictionaries
        """
        species_list = []

        # Get the main page
        soup = self._fetch_page(self.base_url)
        if not soup:
            return species_list

        # Find all species links (adjust selectors based on actual site structure)
        species_links = soup.find_all('a', class_='species-link')

        print(f"Found {len(species_links)} species to scrape")

        for idx, link in enumerate(species_links, 1):
            species_url = urljoin(self.base_url, link.get('href', ''))

            print(f"Scraping species {idx}/{len(species_links)}: {species_url}")

            species_data = self.scrape_species_page(species_url)
            if species_data:
                species_list.append(species_data)

            # Be nice to the server
            time.sleep(0.5)

        return species_list

    def scrape_species_page(self, url: str) -> Dict[str, Any]:
        """
        Scrape individual species page.

        Args:
            url: URL of the species page

        Returns:
            Dictionary containing species data
        """
        soup = self._fetch_page(url)
        if not soup:
            return {}

        # Extract data (adjust selectors based on actual site structure)
        species_data = {}

        try:
            # Taxonomy ID
            taxonomy_elem = soup.find('span', class_='taxonomy-id')
            if taxonomy_elem:
                species_data['taxonomy_id'] = taxonomy_elem.text.strip()

            # Species name
            name_elem = soup.find('h1', class_='species-name')
            if name_elem:
                species_data['species_name'] = name_elem.text.strip()

            # Common name
            common_elem = soup.find('span', class_='common-name')
            if common_elem:
                species_data['common_name'] = common_elem.text.strip()

            # Scientific name
            scientific_elem = soup.find('span', class_='scientific-name')
            if scientific_elem:
                species_data['scientific_name'] = scientific_elem.text.strip()

            # Family
            family_elem = soup.find('span', class_='family')
            if family_elem:
                species_data['family'] = family_elem.text.strip()

            # Order
            order_elem = soup.find('span', class_='order')
            if order_elem:
                species_data['order'] = order_elem.text.strip()

            # Additional data
            additional_data = {}
            data_table = soup.find('table', class_='species-data')
            if data_table:
                rows = data_table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        key = cols[0].text.strip()
                        value = cols[1].text.strip()
                        additional_data[key] = value

            species_data['additional_data'] = additional_data

        except Exception as e:
            print(f"Error parsing species data: {e}")
            return {}

        return species_data

    def _fetch_page(self, url: str) -> BeautifulSoup:
        """
        Fetch and parse a web page.

        Args:
            url: URL to fetch

        Returns:
            BeautifulSoup object or None
        """
        for attempt in range(self.retry_attempts):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'lxml')
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Failed to fetch {url} after {self.retry_attempts} attempts")
                    return None
        return None
