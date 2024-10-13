# kafka_producer.py
import os
import asyncio
from threading import Lock
from typing import Dict, Any, List, Tuple
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import yaml
from confluent_kafka import Producer, KafkaError, KafkaException


class KafkaProducer:
    """
    A thread-safe Kafka producer wrapper class that handles message production,
    logging, and failed message management.
    """

    def __init__(self, config_file: str, log_dir: str = 'logs'):
        """
        Initialize the KafkaProducer with configuration and set up logging.

        Args:
            config_file (str): Path to the Kafka configuration YAML file.
            log_dir (str): Directory to store log files. Defaults to 'logs'.
        """
        self.config, self.topic = self.load_kafka_config(config_file)
        self.producer = Producer(self.config)
        self.failed_messages = []
        self.failed_messages_lock = Lock()  # Thread-safe lock for failed_messages
        self.logger = self.setup_logger(log_dir)

    @staticmethod
    def load_kafka_config(yaml_file: str) -> Dict[str, Any]:
        """
        Load Kafka configuration from a YAML file.

        Args:
            yaml_file (str): Path to the YAML configuration file.

        Returns:
            Dict[str, Any]: Kafka configuration and topic name.

        Raises:
            ConfigurationError: If there's an error loading the configuration.
        """
        try:
            with open(yaml_file, 'r') as file:
                config = yaml.safe_load(file)
            kafka_conf = config['kafka_conf']
            # Convert list of bootstrap servers to comma-separated string
            if isinstance(kafka_conf.get('bootstrap.servers'), list):
                kafka_conf['bootstrap.servers'] = ','.join(kafka_conf['bootstrap.servers'])
            logging.info(f"Loaded Kafka configuration: bootstrap.servers={kafka_conf.get('bootstrap.servers')}, "
                         f"topic={config.get('topic_name')}")
            return kafka_conf, config['topic_name']
        except (FileNotFoundError, KeyError, yaml.YAMLError) as e:
            raise ConfigurationError(f"Error loading Kafka configuration: {str(e)}")

    def setup_logger(self, log_dir: str) -> logging.Logger:
        """
        Set up a logger with both size-based and time-based rotation.

        Args:
            log_dir (str): Directory to store log files.

        Returns:
            logging.Logger: Configured logger object.
        """
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        logger = logging.getLogger("KafkaProducer")
        logger.setLevel(logging.INFO)

        # Size-based rotation: 5 MB per file, keep 5 backup files
        size_handler = RotatingFileHandler(
            os.path.join(log_dir, 'kafka_producer.log'),
            maxBytes=5 * 1024 * 1024,
            backupCount=5
        )

        # Time-based rotation: rotate at midnight, keep 30 days of logs
        time_handler = TimedRotatingFileHandler(
            os.path.join(log_dir, 'kafka_producer.log'),
            when="midnight",
            interval=1,
            backupCount=30
        )

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        size_handler.setFormatter(formatter)
        time_handler.setFormatter(formatter)

        logger.addHandler(size_handler)
        logger.addHandler(time_handler)

        return logger

    def delivery_report(self, err, msg):
        """
        Callback function for Kafka producer to report the delivery status of a message.

        Args:
            err: Error object if delivery failed, None if successful.
            msg: Message object that was delivered or failed.
        """
        if err is not None:
            self.logger.error(f'Message delivery failed: {err}')
            with self.failed_messages_lock:
                self.logger.info(f'Appending to failed_message list for retrying...')
                self.failed_messages.append(msg.value().decode('utf-8'))
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    async def produce_message(self, message: str, headers: List[Tuple[str, str]]):
        """
        Asynchronously produce a single message to Kafka.

        Args:
            :param message: The message to be produced.
            :param headers: Header key value pairs to be added to the message
        """
        try:
            self.producer.produce(self.topic, value=message.encode('utf-8'), headers=headers,
                                  callback=self.delivery_report)
            self.producer.poll(0)  # Trigger delivery reports
        except BufferError as e:
            self.logger.error(f"Local producer queue is full ({len(self.producer)} messages awaiting delivery): {e}")
            await asyncio.sleep(1)  # Wait a second before retrying
            await self.produce_message(message, headers)  # Recursive retry
        except KafkaException as ke:
            self.logger.error(f"Kafka error in produce_message: {ke}")
            if ke.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                self.logger.critical(f"Authorization failed for topic {self.topic}. Check Kafka permissions.")
            with self.failed_messages_lock:
                self.logger.info(f'Appending to failed_message list due to Kafka error')
                self.failed_messages.append((message, headers))
        except Exception as e:
            self.logger.error(f"Unexpected error in produce_message: {e}")
            with self.failed_messages_lock:
                self.logger.info(f'Appending to failed_message list for retrying...')
                self.failed_messages.append((message, headers))

    async def produce_messages(self, messages: List[Tuple[str, List[Tuple[str, str]]]]):
        """
        Asynchronously produce multiple messages to Kafka.

        Args:
            :param messages: Messages to be produced
        """
        for message, headers in messages:
            await self.produce_message(message, headers)
        await self.async_flush()

    def flush(self):
        """
        Flush any remaining messages in the producer queue.
        """
        self.logger.info("Flushing messages...")
        self.producer.flush()

    async def async_flush(self):
        """
        Async flush any remaining messages in the producer queue.
        """
        self.logger.info("Asynchronously flushing messages...")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.producer.flush)

    def get_failed_messages(self) -> List[Dict[str, Any]]:
        """
        Get the list of failed messages in a thread-safe manner.

        Returns:
            List[Dict[str, Any]]: List of failed messages.
        """
        with self.failed_messages_lock:
            return self.failed_messages.copy()  # Return a copy to avoid external modifications

    def write_failed_messages(self, filename: str):
        """
        Write failed messages to a file.

        Args:
            filename (str): Name of the file to write failed messages to.
        """
        try:
            with self.failed_messages_lock, open(filename, 'w') as f:
                for message in self.failed_messages:
                    if isinstance(message, tuple):
                        f.write(f"{message[0]}\n")  # Write the message content
                    else:
                        f.write(f"{message}\n")  # Write the message as is
            self.logger.info(f"Failed messages written to {filename}")
        except IOError as e:
            self.logger.error(f"Error writing failed messages to file: {e}")


class ConfigurationError(Exception):
    """Raised when there's an error in configuration."""
    pass
