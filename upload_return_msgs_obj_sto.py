from base64 import b64decode
from oci_sdk_wrappers import ObjectCrud, StreamWrapper

# create return stream wrapper from StreamExample instance
return_sw = StreamWrapper('InfoMgtReturnStream', 1, 'ocid1.compartment.oc1..aaaaaaaa7lawiz4jty5scuoalmy7hxnscsx2llbdmxzs3xbak4gpmtb4vyda')

# create simple message loop
partition_cursor = return_sw.get_cursor_by_partition(return_sw.stream_client, return_sw.s_id, partition="0")
message_loop_generator = return_sw.simple_message_loop(return_sw.stream_client, return_sw.s_id, partition_cursor)

objectCrud = ObjectCrud()

# upload each message from return stream to object storage
batch_size = 95 # should be 96 (@todo look into why target/return stream is missing first row, but source stream gets first row from CSV – problem should be somewhere in OSA pipeline)
batch = None
messages = []
for message in message_loop_generator:
    if batch == None:
        batch = message.offset / batch_size
    decoded_message = b64decode(message.value.encode()).decode()
    messages.append(decoded_message)
    if len(messages) == batch_size:
        objectCrud.upload_new_object('StreamBucket', batch, str(messages))
        batch = None
        messages = []