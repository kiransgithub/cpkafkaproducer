# Kafka Producer Examples with Python

Hey there! 👋 This repository contains some pretty cool Python scripts that act as Kafka producers. Honestly, it's all about getting data from various sources and sending it to Kafka topics. Let's break it down for you.

## What's this all about?

Basically, we've got two main scripts here:

1. `log_to_kafka_producer.py`: This bad boy handles log files.
2. `mixed_feeds_to_kafka_producer.py`: This one's a bit more versatile, dealing with different types of feeds.

## What do these scripts do?

### Log to Kafka Producer

This script is all about processing log files and sending them to Kafka. Here's the deal:

- It reads log files from specific directories.
- Processes them based on the feed and server configuration.
- Sends the data to Kafka in batches.
- Handles any failures and keeps track of processed files.

### Mixed Feeds to Kafka Producer

This script is like the Swiss Army knife of Kafka producers. It can handle different types of feeds:

- Daily feeds like 'daily_feed_1' and 'daily_feed_2'.
- Hourly feeds like 'hourly_feed_1', 'hourly_feed_2', and 'hourly_feed_3'.
- Each feed has its own file pattern and date format.
- It processes files for multiple servers (for hourly feeds).

## Configuration

You'll have to set up a few configuration files:

1. `mixed_feeds_config.yaml`: This is where you define your feeds, servers, and processing details.
2. `kafka_config.yaml`: All your Kafka-related settings go here.

## How to use it?

Time to get your hands dirty! Here's how you run the mixed feeds producer:

```
python mixed_feeds_to_kafka_producer.py mixed_feeds_config.yaml <feed_name>
```

Replace `<feed_name>` with one of the feeds defined in your config (like 'daily_feed_1', 'daily_feed_2', 'hourly_feed_1', etc.).

## What else should you know?

- The scripts use asynchronous programming (asyncio) for better performance.
- There's a custom `KafkaProducer` class that handles the nitty-gritty of sending messages to Kafka.
- Failed messages are logged and can be retried.
- The code is set up to handle gzipped files, so no worries about compression.

## Dependencies

You'll need to install a few things:

- Python 3.7+
- confluent-kafka
- PyYAML

Just run `pip install confluent-kafka pyyaml` and you should be good to go.

## A word of caution

This code is dealing with some potentially sensitive data (like log files). Make sure you've got the right permissions and you're following your organization's data handling policies.

That's pretty much it! Feel free to dive into the code and adapt it to your needs. If you run into any issues or have questions, don't hesitate to reach out. Happy Kafka-ing! 🚀# Kafka Producer Examples with Python

Hey there! 👋 This repository contains some pretty cool Python scripts that act as Kafka producers. Honestly, it's all about getting data from various sources and sending it to Kafka topics. Let's break it down for you.

## What's this all about?

Basically, we've got two main scripts here:

1. `log_to_kafka_producer.py`: This bad boy handles log files.
2. `mixed_feeds_to_kafka_producer.py`: This one's a bit more versatile, dealing with different types of feeds.

## What do these scripts do?

### Log to Kafka Producer

This script is all about processing log files and sending them to Kafka. Here's the deal:

- It reads log files from specific directories.
- Processes them based on the feed and server configuration.
- Sends the data to Kafka in batches.
- Handles any failures and keeps track of processed files.

### Mixed Feeds to Kafka Producer

This script is like the Swiss Army knife of Kafka producers. It can handle different types of feeds:

- Daily feeds like 'daily_feed_1' and 'daily_feed_2'.
- Hourly feeds like 'hourly_feed_1', 'hourly_feed_2', and 'hourly_feed_3'.
- Each feed has its own file pattern and date format.
- It processes files for multiple servers (for hourly feeds).

## Configuration

You'll have to set up a few configuration files:

1. `mixed_feeds_config.yaml`: This is where you define your feeds, servers, and processing details.
2. `kafka_config.yaml`: All your Kafka-related settings go here.

## How to use it?

Time to get your hands dirty! Here's how you run the mixed feeds producer:

```
python mixed_feeds_to_kafka_producer.py mixed_feeds_config.yaml <feed_name>
```

Replace `<feed_name>` with one of the feeds defined in your config (like 'daily_feed_1', 'daily_feed_2', 'hourly_feed_1', etc.).

## What else should you know?

- The scripts use asynchronous programming (asyncio) for better performance.
- There's a custom `KafkaProducer` class that handles the nitty-gritty of sending messages to Kafka.
- Failed messages are logged and can be retried.
- The code is set up to handle gzipped files, so no worries about compression.

## Dependencies

You'll need to install a few things:

- Python 3.7+
- confluent-kafka
- PyYAML

Just run `pip install confluent-kafka pyyaml` and you should be good to go.

That's pretty much it! Feel free to dive into the code and adapt it to your needs. If you run into any issues or have questions, don't hesitate to reach out. Happy Kafka-ing! 🚀