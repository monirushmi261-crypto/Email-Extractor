"""
Email Extraction Engine
~~~~~~~~~~~~~~~~~~~~~~~

A high-performance, enterprise-grade utility module designed to parse large,
unstructured text data files, extract valid RFC-compliant email address patterns,
and output an ordered, deduplicated dataset securely.

License: MIT
"""

import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Generator, Set

# --- System-Wide Enterprise Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Line:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger: Final[logging.Logger] = logging.getLogger("EmailExtractorEngine")


@dataclass(frozen=True)
class EngineConfig:
    """Immutable operational configuration schema for the automation pipeline."""
    input_uri: Path
    output_uri: Path
    encoding: str = "utf-8"


class EmailExtractor:
    """
    Core execution engine implementing memory-safe stream processing 
    and regex isolation layers to extract text tokens dynamically.
    """
    
    # Pre-compiled regex token for optimal, thread-safe parsing performance
    _EMAIL_REGEX: Final[re.Pattern] = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )

    def __init__(self, config: EngineConfig) -> None:
        """Initializes the engine structure with validation attributes."""
        self.config: Final[EngineConfig] = config

    def _stream_lines(self) -> Generator[str, None, None]:
        """Memory-guarded text stream provider yielding rows lazily."""
        with open(
            self.config.input_uri,
            mode='r',
            encoding=self.config.encoding,
            errors='ignore'
        ) as file_stream:
            for line in file_stream:
                yield line

    def execute(self) -> bool:
        """
        Orchestrates full process cycle execution. 
        Returns True upon complete validation and storage success, otherwise False.
        """
        logger.info("Initializing automated email extraction processing sequence...")
        
        if not self.config.input_uri.is_file():
            logger.error(f"IO Interruption: Target source file path not found: '{self.config.input_uri}'")
            return False

        unique_extracted_set: Set[str] = set()

        try:
            for text_line in self._stream_lines():
                matches = self._EMAIL_REGEX.findall(text_line)
                for email_token in matches:
                    unique_extracted_set.add(email_token.lower())
        except Exception as read_fault:
            logger.critical(f"System critical fault encountered during streaming operations: {read_fault}")
            return False

        if not unique_extracted_set:
            logger.warning("Pipeline verification alert: Target sequence extracted zero data sets.")
            return False

        try:
            # Enforce dynamic parent directory generation safely
            self.config.output_uri.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.config.output_uri, mode='w', encoding=self.config.encoding) as output_stream:
                for sorted_email in sorted(unique_extracted_set):
                    output_stream.write(f"{sorted_email}\n")
            
            logger.info("=" * 65)
            logger.info("AUTOMATION PIPELINE COMPLETED SUCCESSFULLY:")
            logger.info(f" - System Source File Node:  {self.config.input_uri.name}")
            logger.info(f" - System Target File Node:  {self.config.output_uri.name}")
            logger.info(f" - Total Unique Rows Saved: {len(unique_extracted_set)}")
            logger.info("=" * 65)
            return True

        except Exception as write_fault:
            logger.critical(f"System IO critical error: Target writing sequence breached: {write_fault}")
            return False


def _orchestrate_mock_data(target_path: Path) -> None:
    """Generates structural testing dataset validation objects locally."""
    if target_path.exists():
        return
        
    logger.info("Test environment empty. Instantiating operational mock data parameters...")
    mock_payload = (
        "System Log Node 01 - Service Init: admin.portal@enterprise-network.io\n"
        "System Log Node 02 - Failure Warning: routing_error@cloud-datacenter.net\n"
        "System Log Node 03 - Diagnostics Alert: infrastructure_dev@company.org\n"
    )
    try:
        with open(target_path, mode='w', encoding='utf-8') as mock_file:
            mock_file.write(mock_payload)
    except IOError as io_err:
        logger.error(f"Unable to safely structure system mock database files: {io_err}")


if __name__ == "__main__":
    # Dynamically resolve paths relative to the current working environment safely
    BASE_DIRECTORY: Final[Path] = Path.cwd()
    
    runtime_config = EngineConfig(
        input_uri=BASE_DIRECTORY / "emails_to_extract.txt",
        output_uri=BASE_DIRECTORY / "extracted_emails.txt"
    )

    # Initialize defensive mock validation routines prior to processing
    _orchestrate_mock_data(runtime_config.input_uri)

    # Instantiate decoupled extraction interface execution sequence
    extractor_engine = EmailExtractor(config=runtime_config)
    success: bool = extractor_engine.execute()
    
    if not success:
        sys.exit(1)
