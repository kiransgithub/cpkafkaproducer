import argparse
import yaml
from confluent_kafka.admin import AdminClient


def load_kafka_config(yaml_file):
    with open(yaml_file, 'r') as yaml_file_obj:
        yaml_content = yaml.safe_load(yaml_file_obj)

    kafka_conf = yaml_content.get('kafka_conf', {})

    # Convert bootstrap.servers to a comma-separated string if it's a list
    if isinstance(kafka_conf.get('bootstrap.servers'), list):
        kafka_conf['bootstrap.servers'] = ','.join(kafka_conf['bootstrap.servers'])

    # Ensure numeric values are not strings
    for key in ['compression.level', 'request.required.acks']:
        if key in kafka_conf:
            kafka_conf[key] = int(kafka_conf[key])

    return kafka_conf


def list_kafka_topics(config):
    admin_client = AdminClient(config)

    try:
        futures = admin_client.list_topics(timeout=10)

        # The list_topics() method returns a ClusterMetadata object
        metadata = futures.topics

        print("Available topics:")
        for topic, topic_metadata in metadata.items():
            print(f"- {topic}")
            print(f"  Partitions: {len(topic_metadata.partitions)}")
            print(f"  Is Internal: {topic_metadata.is_internal}")
    except Exception as e:
        print(f"Error listing topics: {e}")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="List Kafka topics using configuration from a YAML file. Ex: /opt/apps/confluent/kafka_conf.yaml")
    parser.add_argument("--kafka_config_yaml", required=True,
                        help="Path to the Kafka configuration YAML file Ex: /opt/apps/confluent/kafka_conf.yaml")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    try:
        kafka_conf = load_kafka_config(args.kafka_config_yaml)

        # Remove unnecessary configuration for AdminClient
        kafka_conf.pop('compression.codec', None)
        kafka_conf.pop('compression.level', None)
        kafka_conf.pop('request.required.acks', None)

        list_kafka_topics(kafka_conf)
    except FileNotFoundError:
        print(f"Error: The file {args.kafka_config_yaml} was not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing the YAML file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
