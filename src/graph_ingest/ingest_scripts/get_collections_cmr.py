import os
import json
import pandas as pd
import requests
from urllib.parse import quote
from tqdm import tqdm
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from typing import Tuple, Dict, Any, List, Optional

# Import centralized configuration and logging functions
from graph_ingest.common.config_reader import load_config, AppConfig
from graph_ingest.common.logger_setup import setup_logger


class MetadataFetcher:
    """Handles fetching, parsing, and saving dataset metadata from CMR API."""
    
    def __init__(self, config: AppConfig) -> None:
        """
        Initialize the metadata fetcher with configuration.
        
        Args:
            config (AppConfig): Application configuration object.
        """
        self.config = config
        self.base_dir = config.paths.dataset_metadata_directory
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.logger = setup_logger(
            __name__, 
            f"collection_fetch_{timestamp}.log", 
            level=logging.INFO, 
            file_level=logging.INFO
        )
        
        # Statistics tracking
        self.success_count = 0
        self.failure_count = 0
        self.missing_cmr_id_count = 0
        self.unknown_frequency_count = 0
        self.conflict_count = 0
        
    def load_dataframe(self) -> pd.DataFrame:
        """
        Load the source DOIs CSV file into a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing DOIs to fetch metadata for.
        """
        dois_file: str = self.config.paths.source_dois_directory
        return pd.read_csv(dois_file)
    
    def extract_frequency(self, metadata: Dict[str, Any]) -> Tuple[str, bool]:
        """
        Extract frequency information from the metadata by checking the EntryTitle and Abstract.

        Args:
            metadata (dict): The UMM metadata for the dataset.

        Returns:
            Tuple[str, bool]: A tuple (frequency, conflict_flag) where:
                - frequency: the determined frequency (or "Unknown")
                - conflict_flag: True if abstract and long name disagree on frequency.
        """
        frequency_keywords = ["daily", "hourly", "monthly", "weekly"]

        long_name = metadata.get("EntryTitle", "").lower()
        abstract = metadata.get("Abstract", "").lower()

        abstract_frequency = next((freq for freq in frequency_keywords if freq in abstract), "Unknown")
        long_name_frequency = next((freq for freq in frequency_keywords if freq in long_name), "Unknown")

        conflict = False
        if abstract_frequency != "Unknown" and long_name_frequency != "Unknown" and abstract_frequency != long_name_frequency:
            conflict = True
            self.logger.warning(
                f"Frequency conflict for dataset '{metadata.get('EntryTitle', '')}': "
                f"Abstract='{abstract_frequency}', LongName='{long_name_frequency}'"
            )

        frequency = long_name_frequency if long_name_frequency != "Unknown" else abstract_frequency
        return frequency, conflict
    
    def save_metadata(self, doi: str, umm: Dict[str, Any], data_center: str) -> None:
        """
        Save metadata to a JSON file in the appropriate directory.
        
        Args:
            doi (str): The DOI associated with the metadata.
            umm (Dict[str, Any]): The metadata to save.
            data_center (str): The data center short name for directory organization.
        """
        # Create a directory for the data center based on its ShortName
        center_dir = os.path.join(self.base_dir, data_center)
        os.makedirs(center_dir, exist_ok=True)

        # Save metadata as JSON
        filename = f"{doi.replace('/', '_')}.json"
        filepath = os.path.join(center_dir, filename)
        
        with open(filepath, "w") as f:
            json.dump(umm, f)
            
        self.logger.info(f"Successfully saved metadata for DOI {doi} to {filepath}")

    def fetch_metadata(self, doi: str) -> Optional[Dict[str, Any]]:
        """
        Fetch metadata from CMR API using the provided DOI.
        
        Args:
            doi (str): The DOI to fetch metadata for.
            
        Returns:
            Optional[Dict[str, Any]]: The metadata if successful, None otherwise.
        """
        try:
            encoded_doi = quote(doi, safe="")
            url = f"https://cmr.earthdata.nasa.gov/search/collections.umm_json?doi={encoded_doi}"
            response = requests.get(url)
            response.raise_for_status()
            
            if response.status_code == 200 and response.json().get("items"):
                item = response.json()["items"][0]
                umm = item.get("umm", {})
                cmr_id = item.get("meta", {}).get("concept-id")
                
                if not cmr_id:
                    self.logger.warning(f"Metadata retrieved but CMR ID is missing for DOI {doi}")
                    return None
                
                if umm:
                    umm['CMR_ID'] = cmr_id
                    return umm
                    
            self.logger.error(
                f"Failed to fetch metadata for DOI {doi}, response status: {response.status_code}"
            )
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching metadata for DOI {doi}: {e}")
            return None

    def process_doi(self, doi: str) -> Tuple[str, bool, bool, str, bool]:
        """
        Process a single DOI: fetch metadata, extract frequency, and save results.

        Args:
            doi (str): The DOI to process.

        Returns:
            Tuple[str, bool, bool, str, bool]: A tuple containing:
                - doi: The DOI that was processed.
                - success_flag: Whether the fetch was successful.
                - has_cmr_id_flag: Whether a valid CMR ID was found.
                - frequency: Extracted frequency ("Unknown" if not found).
                - conflict_flag: Whether there was a frequency conflict.
        """
        umm = self.fetch_metadata(doi)
        
        if not umm:
            return doi, False, False, "Unknown", False
            
        # Extract frequency info
        frequency, conflict = self.extract_frequency(umm)
        umm['Frequency'] = frequency
        
        # Get data center info for file organization
        data_center = umm.get("DataCenters", [{}])[0].get("ShortName", "Unknown")
        
        # Save the metadata
        self.save_metadata(doi, umm, data_center)
        
        return doi, True, True, frequency, conflict

    def run(self) -> None:
        """
        Fetch, process, and save metadata for all DOIs in the source file.
        Reports summary statistics upon completion.
        """
        # Load the DOIs from the CSV file
        df: pd.DataFrame = self.load_dataframe()

        # Determine the number of threads to use for parallel downloads
        num_threads: int = multiprocessing.cpu_count()
        self.logger.info(f"Starting download with {num_threads} parallel threads")

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_doi = {
                executor.submit(self.process_doi, doi): doi for doi in df["DOI_NAME"]
            }
            
            for future in tqdm(as_completed(future_to_doi), total=len(df), desc="Downloading Metadata"):
                doi, success, has_cmr_id, frequency, conflict = future.result()
                
                if success:
                    self.success_count += 1
                    if not has_cmr_id:
                        self.missing_cmr_id_count += 1
                    if frequency == "Unknown":
                        self.unknown_frequency_count += 1
                    if conflict:
                        self.conflict_count += 1
                else:
                    self.failure_count += 1

        self._report_statistics()
        
    def _report_statistics(self) -> None:
        """Generate and log summary statistics about the metadata fetch operation."""
        total_collections = self.success_count + self.failure_count
        
        unknown_frequency_percentage = (
            (self.unknown_frequency_count / total_collections) * 100 
            if total_collections > 0 else 0
        )
        
        conflict_percentage = (
            (self.conflict_count / total_collections) * 100 
            if total_collections > 0 else 0
        )

        stats = [
            f"Successfully downloaded metadata for {self.success_count} DOIs.",
            f"Failed to download metadata for {self.failure_count} DOIs.",
            f"Retrieved metadata but missing CMR ID for {self.missing_cmr_id_count} collections.",
            f"Frequency could not be determined for {self.unknown_frequency_count} collections "
            f"({unknown_frequency_percentage:.2f}% of total collections).",
            f"Frequency conflicts between Abstract and LongName for {self.conflict_count} collections "
            f"({conflict_percentage:.2f}% of total collections)."
        ]
        
        # Print and log statistics
        for stat in stats:
            print(stat)
            self.logger.info(stat)


def main() -> None:
    """
    Main entry point for downloading metadata from CMR for a list of DOIs,
    saving the results, and logging summary statistics.
    """
    config = load_config()
    fetcher = MetadataFetcher(config)
    fetcher.run()


if __name__ == "__main__":
    main()