import sys
import math
import json
from fileinput import filename

import yaml
import datetime
import hashlib
from uuid import uuid4
from confluent_kafka import Producer

jsonstring1 = """ {"name": "Gal", "email": "gadot84@gmail.com", "salary": "8345.5"} """
jsonstring2 = """ {"name": "Dwayne", "email": "johnson2gmail.com", "salary": "7345.75"} """
jsonstring3 = """ {"name": "Jason", "email": "jason9@gmail.com", "salary": "3345.25"} """
jsonv1 = jsonstring1.encode()
jsonv2 = jsonstring2.encode()
jsonv3 = jsonstring3.encode()

max_messages = None


def delivery_report(errmsg, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        errmsg (KafkaError): The error that occurred while message producing.
        msg (actual message): The message that was produced.
    Note:
        In the delivery report callback the message.key() and message.value()
        will be the binary format as encoded by any configured Serializers and
        not necessarily the same as the original object passed to produce()
    To manually trigger callbacks for a batch of key and value to delivery
    reports, we would recommend a bound callback or lambda where you pass
    the same key, value and headers.
    """
    if errmsg is not None:
        print('Delivery failed for Message: {} : {}'.format(msg.key(), errmsg))
    else:
        print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))


if len(sys.argv) > 1:
    yaml_file = sys.argv[1]
    with open(yaml_file, 'r') as yaml_file_obj:
        yaml_content = yaml.load(yaml_file_obj, yaml.Loader)

for yaml_kafka_conf_key in yaml_content['kafka_conf'].keys():
    if type(yaml_content['kafka_conf'][yaml_kafka_conf_key]) == list:
        kafka_conf[yaml_kafka_conf_key] = yaml_content['kafka_conf'][yaml_kafka_conf_key]
    else:
        kafka_conf[yaml_kafka_conf_key] = [yaml_content['kafka_conf'][yaml_kafka_conf_key]]

kafka_topic_name = None
if 'topic_name' in yaml_content.keys():
    kafka_topic_name = yaml_content['topic_name']
else:
    kafka_topic_name = 'nf_topic.empdev'

messages_per_sending = yaml_content['messages_per_sending']

# Change your Kafka Topic Name here. For this example, let's assume our Kafka Topic has 3 Partitions ==> 0, 1, 2
# We are assuming that we are producing to all partitions.

# If we want to read the same message from a Java Consumer Program
# example: java -DKEY_DESERIALIZER_CLASS_CONFIG=ByteArrayDeserializer.class
#          -DVALUE_DESERIALIZER_CLASS_CONFIG=ByteArrayDeserializer.class

myjkssecret = 'yourJksPassword'
# You can call "remote API to get JKS password instead of hardcoding like above
kafka_topic_name = 'nf_topic.empdev'
kafka_topic_name = yaml_content['topic_name']

# Change your Kafka Topic Name here. For this example let's assume our Kafka Topic has 3 Partitions ==> 0, 1, 2
# We are assuming that we are producing to all partitions.

# If we want to read the same message from a Java Consumer Program
# example: java -DKEY_DESERIALIZER_CLASS_CONFIG=ByteArrayDeserializer.class
#          -DVALUE_DESERIALIZER_CLASS_CONFIG=ByteArrayDeserializer.class

myjkssecret = 'yourJksPassword'
# You can call "remote API to get JKS password instead of hardcoding like above

kafka_start_time_UTC = datetime.datetime.utcnow()

# print("Start Time UTC: ", kafka_start_time_UTC)

kafka_conf = {
    'bootstrap.servers': '<bootstrap servers comma separated list>',
    'security.protocol': 'SSL',
    'ssl.key.location': 'rsyslog.key',
    'ssl.certificate.location': 'rsyslog.crt',
    'ssl.ca.location': 'ss1/ca.crt',
    'compression.codec': 'gzip'
}

print("Connecting to Kafka topic...")
producer = Producer(kafka_conf)
# Producer instance using the Kafka configuration

# Trigger any available delivery report callbacks from previous produce() calls
producer.poll(0)

line_counter = 0
sum_of_string_lengths = 0

try:
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    producer.produce(topic=kafka_topic_name, key=str(uuid4()), value=jsonv1, on_delivery=delivery_report)
    producer.produce(topic=kafka_topic_name, key=str(uuid4()), value=jsonv2, on_delivery=delivery_report)
    producer.produce(topic=kafka_topic_name, key=str(uuid4()), value=jsonv3, on_delivery=delivery_report)

    for stdin_line in sys.stdin:
        line_counter += 1
        sum_of_string_lengths = sum_of_string_lengths + len(stdin_line)
        producer.produce(topic=kafka_topic_name,
                         key=filename,
                         headers={
                             'linenum': line_counter.to_bytes(math.ceil(int.bit_length(line_counter) / 8), 'big') + str(
                                 line_counter),
                             'SHA256': hashlib.sha256(stdin_line.encode('UTF-8')).digest(),
                             'SHA256': hashlib.sha256(
                                 (filename + ":" + str(line_counter) + ":" + stdin_line).encode('UTF-8')).hexdigest()
                         })
        if line_counter > max_messages:
            producer.flush()

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()
    # print("Line count (inside TRY): " + str(line_counter))

except Exception as e:
    print("Exception happened: ", e)

kafka_end_time_UTC = datetime.datetime.utcnow()
print(json.dumps({
    "End_Time": str(kafka_end_time_UTC),
    "Start_Time": str(kafka_start_time_UTC),
    "Line_Count": line_counter,
    "Total_String_Length": sum_of_string_lengths,
    "Duration_Seconds": (kafka_end_time_UTC - kafka_start_time_UTC).total_seconds()
}))

# print("LINE COUNT at end: " + str(line_counter))
print("Stopping Kafka producer")
