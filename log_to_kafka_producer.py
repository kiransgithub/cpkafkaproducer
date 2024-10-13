import asyncio
import glob
import gzip
import json
import logging
import os
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


def process_feed(server: str, feed: str, config: Dict[str, Any], kafka_producer: KafkaProducer):
    asyncio.run(process_feed_async(server, feed, config, kafka_producer))


async def process_feed_async(server: str, feed: str, config: Dict[str, Any], kafka_producer: KafkaProducer):
    """
    Process all log files for a specific feed.

    Args:
        server (str): Server name.
        feed (str): Feed name.
        config (Dict[str, Any]): Configuration dictionary.
        kafka_producer (KafkaProducer): Kafka producer object.
    """
    base_dir = config['base_directory']
    log_dir = os.path.join(base_dir, server, feed, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    logger = setup_logger(f"{server}_{feed}", os.path.join(log_dir, f"{feed}.log"))

    logger.info(f"Starting processing for {server}/{feed}")

    batch_size = config['kafka_load_batch_size']

    now = datetime.now()
    process_date = now.replace(minute=30, second=0, microsecond=0) - timedelta(hours=1)
    date_str = process_date.strftime("%Y-%m-%d")
    hour_str = process_date.strftime("%HZ")

    file_pattern = f"{config['file_pattern'].format(hour_str)}"
    directory = os.path.join(base_dir, server, feed)
    files = glob.glob(os.path.join(directory, file_pattern))

    for file_path in files:
        logger.info(f"Processing file: {file_path}")

        processed_dir = os.path.join(directory, config['processed_dir'], date_str)
        partial_dir = os.path.join(directory, config['partially_processed_dir'], date_str)
        failed_dir = os.path.join(directory, config['failed_dir'], date_str)

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


async def main():
    """
    Main function to orchestrate the log processing.
    """
    config = load_config('log_feed_config.yaml')
    kafka_producer = KafkaProducer(config['kafka_config_file'])
    servers = config['servers']
    feed_names = config['feed_name']
    max_threads = config['max_threads']

    # Set max_threads to the no.of feeds to be processed or max_threads
    max_threads = min(len(servers) * len(feed_names), max_threads)

    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        tasks = []
        for server in servers:
            for feed in feed_names:
                # Use the executor to run process_feed in a separate thread
                task = loop.run_in_executor(
                    executor,
                    process_feed,
                    server,
                    feed,
                    config,
                    kafka_producer
                )
                tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
    # Flush the Kafka producer after all processing is done
    await kafka_producer.async_flush()
    # Write failed messages for each feed
    for server in servers:
        for feed in feed_names:
            failed_messages = kafka_producer.get_failed_messages(feed)
            if failed_messages:
                failed_dir = os.path.join(config['base_directory'], server, feed, config['failed_dir'])
                os.makedirs(failed_dir, exist_ok=True)
                failed_file = os.path.join(failed_dir, f'failed_messages_{feed}.log')
                kafka_producer.write_failed_messages(failed_file, feed)


if __name__ == "__main__":
    asyncio.run(main())
