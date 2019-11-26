from base64 import b64decode
from oci_sdk_wrappers import ObjectCrud, StreamWrapper

# create return stream wrapper from StreamExample instance
return_sw = StreamWrapper('InfoMgtReturnStream', 1, 'ocid1.compartment.oc1..aaaaaaaa7lawiz4jty5scuoalmy7hxnscsx2llbdmxzs3xbak4gpmtb4vyda', None)

# create simple message loop
partition_cursor = return_sw.get_cursor_by_partition(return_sw.stream_client, return_sw.s_id, partition="0")
message_loop_generator = return_sw.simple_message_loop(return_sw.stream_client, return_sw.s_id, partition_cursor)

objectCrud = ObjectCrud('~/.oci/oss_stream_objectstorage_config')

# upload each message from return stream to object storage
for message in message_loop_generator:
    objectCrud.upload_new_object('OSSBucket', message.offset, message.value)