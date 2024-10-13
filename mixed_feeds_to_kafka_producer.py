import argparse
import asyncio
import glob
import gzip
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, Any

import yaml

from kafkaproducer import KafkaProducer

# Set up root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='log_processor.log', filemode='a')
root_logger = logging.getLogger()


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load the configuration from a YAML file.

    This function reads the specified YAML file and returns its contents as a dictionary.

    Args:
        config_file (str): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the specified configuration file is not found.
        yaml.YAMLError: If there's an error parsing the YAML file.
        Exception: For any other unexpected errors during config loading.
    """
    logger = logging.getLogger(__name__)

    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        logger.info(f"Configuration loaded successfully from {config_file}")
        return config

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML in configuration file {config_file}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading configuration from {config_file}: {str(e)}")
        raise


def setup_logger(name, log_file, level=logging.INFO):
    """
    Set up a logger for a specific feed.

    Args:
        name (str): Name of the logger (usually server_feed).
        log_file (str): Path to the log file.
        level (int): Logging level.

    Returns:
        logging.Logger: Configured logger object.
    """
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


async def process_log_file(file_path: str, feed_name: str, server_name: str, kafka_producer: KafkaProducer,
                           logger: logging.Logger, batch_size: int) -> Dict[str, Any]:
    """
    Process a single log file, sending its contents to Kafka.

    Args:
        file_path (str): Path to the log file.
        feed_name (str): Name of the feed being processed.
        server_name (str): Name of the server being processed.
        kafka_producer (KafkaProducer): Kafka producer object.
        logger (logging.Logger): Logger for this specific feed.
        batch_size (int): Number of messages to batch before sending to Kafka.

    Returns:
        Dict[str, Any]: A dictionary containing the total and failed row counts.
    """
    total_rows = 0
    failed_rows = 0
    batch_data = []

    try:
        # Extract file name from file path
        file_name = os.path.basename(file_path)

        with gzip.open(file_path, 'rt') as file:
            for line in file:
                total_rows += 1
                try:
                    # Create headers
                    headers = [
                        ('feed_name', feed_name.encode('utf-8')),
                        ('file_name', file_name.encode('utf-8'))
                    ]

                    # Add server_name to headers only if it's not empty
                    if server_name:
                        headers.append(('server_name', server_name.encode('utf-8')))

                    # Correct way to append to batch_data
                    batch_data.append((line.strip(), headers))

                    if len(batch_data) >= batch_size:
                        await kafka_producer.produce_messages(batch_data)
                        batch_data = []
                except Exception as e:
                    logger.error(f"Error processing line in file {file_path}: {str(e)}")
                    failed_rows += 1

        if batch_data:
            await kafka_producer.produce_messages(batch_data)

    except Exception as e:
        logger.exception(f"Error processing file {file_path}: {str(e)}")
        failed_rows = total_rows

    return {"total_rows": total_rows, "failed_rows": failed_rows}


async def process_feed_async(server: str, feed: str, input_config: Dict[str, Any], kafka_producer: KafkaProducer):
    """
    Process all log files for a specific feed.

    Args:
        server (str): Server name.
        feed (str): Feed name.
        input_config (Dict[str, Any]): Configuration dictionary.
        kafka_producer (KafkaProducer): Kafka producer object.
    """
    base_dir = input_config['base_directory']
    log_dir = os.path.join(base_dir, server, feed, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    logger = setup_logger(f"{server}_{feed}", os.path.join(log_dir, f"{feed}.log"))

    logger.info(f"Starting processing for {server}/{feed}")

    batch_size = input_config['kafka_load_batch_size']

    now = datetime.now()
    process_date = now.replace(minute=30, second=0, microsecond=0) - timedelta(hours=1)

    # Find the correct feed configuration
    feed_config = next((f for f in input_config['feeds']['daily'] + input_config['feeds']['hourly'] if f['name'] == feed), None)
    if not feed_config:
        logger.error(f"Configuration for feed '{feed}' not found")
        return

    # Use the feed-specific date format and file pattern
    date_format = feed_config['date_format']
    file_pattern = feed_config['file_pattern']

    # Format the date string according to the feed's date format
    date_str = process_date.strftime(date_format)

    # Handle different date/time placeholders in the file pattern
    if '{datetime}' in file_pattern:
        formatted_pattern = file_pattern.format(datetime=date_str)
    elif '{date}' in file_pattern and '{hour}' in file_pattern:
        hour_str = process_date.strftime('%H')
        formatted_pattern = file_pattern.format(date=date_str.split('-')[0], hour=hour_str)
    elif '{date}' in file_pattern:
        formatted_pattern = file_pattern.format(date=date_str)
    else:
        logger.error(f"Unsupported file pattern format for feed '{feed}'")
        return

    directory = os.path.join(base_dir, server, feed) if server else os.path.join(base_dir, feed)
    files = glob.glob(os.path.join(directory, formatted_pattern))

    for file_path in files:
        logger.info(f"Processing file: {file_path}")

        processed_dir = os.path.join(directory, input_config['output_directories']['processed'], date_str)
        partial_dir = os.path.join(directory, input_config['output_directories']['partially_processed'], date_str)
        failed_dir = os.path.join(directory, input_config['output_directories']['failed'], date_str)

        for dir_path in [processed_dir, partial_dir, failed_dir]:
            os.makedirs(dir_path, exist_ok=True)

        result = await process_log_file(file_path, feed, server, kafka_producer, logger, batch_size)

        if result['failed_rows'] == result['total_rows']:
            # Complete failure
            dest_path = os.path.join(failed_dir, os.path.basename(file_path))
            os.rename(file_path, dest_path)
            logger.error(f"File completely failed: {file_path}")
        elif result['failed_rows'] > 0:
            # Partial failure
            dest_path = os.path.join(partial_dir, os.path.basename(file_path))
            with gzip.open(file_path, 'rt') as src, gzip.open(dest_path, 'wt') as dest:
                for i, line in enumerate(src):
                    if i >= result['total_rows'] - result['failed_rows']:
                        dest.write(line)
            os.remove(file_path)
            logger.warning(f"File partially processed: {file_path}")
        else:
            # Complete success
            dest_path = os.path.join(processed_dir, os.path.basename(file_path))
            os.rename(file_path, dest_path)
            logger.info(f"File successfully processed: {file_path}")

    logger.info(f"Finished processing for {server}/{feed}")


async def main(feed_name: str, input_config: Dict):
    """
    Main function to orchestrate the feed processing.
    """
    kafka_producer = KafkaProducer(input_config['kafka_config_file'])

    # Check if the feed is hourly or daily
    if feed_name in ['spur', 'rrp']:
        # Process without servers - sending server as empty for daily feeds
        await process_feed_async("", feed_name, input_config, kafka_producer)
    else:
        # Process with servers for hourly feeds
        servers = input_config['servers'].get(feed_name, [])
        if not servers:
            print(f"No servers found for feed: {feed_name}")
            return
        tasks = []
        for server in servers:
            task = asyncio.create_task(process_feed_async(server, feed_name, input_config, kafka_producer))
            tasks.append(task)
        await asyncio.gather(*tasks)

    await kafka_producer.async_flush()

    # Handle failed messages
    failed_messages = kafka_producer.get_failed_messages()
    if failed_messages:
        if feed_name in ['spur', 'rrp']:
            failed_dir = os.path.join(input_config['base_directory'], feed_name, input_config['failed_dir'])
        else:
            failed_dir = os.path.join(input_config['base_directory'], input_config['servers'][0], feed_name, input_config['failed_dir'])
        os.makedirs(failed_dir, exist_ok=True)
        failed_file = os.path.join(failed_dir, f'failed_messages_{feed_name}.log')
        kafka_producer.write_failed_messages(failed_file)


def get_allowed_feeds(config: dict) -> list:
    """
    Extract the list of allowed feed names from the configuration dictionary.

    This function parses the 'feeds' section of the provided configuration,
    extracting feed names from both 'daily' and 'hourly' subsections.

    Args:
        config (dict): A dictionary containing the full configuration.
                       Expected to have a 'feeds' key with 'daily' and 'hourly' subsections.

    Returns:
        list: A list of feed names allowed by the configuration.

    Raises:
        KeyError: If the expected keys ('feeds', 'daily', 'hourly') are not found in the config.
        TypeError: If the 'daily' or 'hourly' sections are not lists.
        ValueError: If a feed in the configuration doesn't have a 'name' field.

    Example:
        >>> config = {
        ...     'feeds': {
        ...         'daily': [{'name': 'spur'}, {'name': 'rrp'}],
        ...         'hourly': [{'name': 'tds'}, {'name': 'esam'}, {'name': 'ice'}]
        ...     }
        ... }
        >>> get_allowed_feeds(config)
        ['spur', 'rrp', 'tds', 'esam', 'ice']
    """
    try:
        feeds = config['feeds']
        daily_feeds = feeds['daily']
        hourly_feeds = feeds['hourly']

        if not isinstance(daily_feeds, list) or not isinstance(hourly_feeds, list):
            raise TypeError("Daily and hourly feeds must be lists")

        allowed_feeds = []

        for feed_list in [daily_feeds, hourly_feeds]:
            for feed in feed_list:
                if 'name' not in feed:
                    raise ValueError(f"Feed is missing 'name' field: {feed}")
                allowed_feeds.append(feed['name'])

        return allowed_feeds

    except KeyError as e:
        raise KeyError(f"Missing expected key in configuration: {e}")
    except Exception as e:
        raise ValueError(f"Error processing feed configuration: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a specific feed and load to kafka")
    parser.add_argument("config_file", help="Path to the YAML configuration file", metavar="CONFIG_FILE")
    parser.add_argument("feed_name", help="Name of the feed to process", metavar="FEED_NAME")

    if len(sys.argv) < 3:
        parser.print_help()
        print("\nError: Both config_file and feed_name are required")
        print("Example usage: python script_name.py config.yaml spur")
        sys.exit(1)

    args = parser.parse_args()

    try:
        config = load_config(args.config_file)
        allowed_feeds = get_allowed_feeds(config)

        if args.feed_name not in allowed_feeds:
            print(f"Error: '{args.feed_name}' is not a valid feed name")
            print(f"Allowed feed names are: {', '.join(allowed_feeds)}")
            print(f"Example usage: python {sys.argv[0]} {args.config_file} {allowed_feeds[0]}")
            sys.exit(1)

        # If we get here, we have a valid feed name
        asyncio.run(main(args.feed_name, config))

    except (KeyError, ValueError) as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
