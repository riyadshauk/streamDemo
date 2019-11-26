import oci
from base64 import b64encode, b64decode
import csv
from oci_sdk_wrappers import StreamWrapper

# this is a demo of how to publish some dummy data to a stream in Oracle Streaming Analytics
def publish_csv_messages(client, stream_id):
    message_list = []
    with open('mtlukens20190531.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            if row[-1] == 'observationtime':
                continue
            key = row[-1] # timestamp string ie: 2019-05-31T00:00:02.882Z
            value = str(row)
            encoded_key = b64encode(key.encode()).decode()
            encoded_value = b64encode(value.encode()).decode()
            message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))

        print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
        messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
        put_message_result = client.put_messages(stream_id, messages)

        # The put_message_result can contain some useful metadata for handling failures
        for entry in put_message_result.data.entries:
            if entry.error:
                print("Error ({}) : {}".format(entry.error, entry.error_message))
            else:
                print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))


# create return stream wrapper from StreamExample instance
publish_sw = StreamWrapper('InfomgtStreamsDemo', 1, 'ocid1.compartment.oc1..aaaaaaaa7lawiz4jty5scuoalmy7hxnscsx2llbdmxzs3xbak4gpmtb4vyda', None)

# Publish some messages to the stream
publish_csv_messages(publish_sw.stream_client, publish_sw.s_id)