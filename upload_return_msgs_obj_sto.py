from base64 import b64decode
from oci_sdk_wrappers import ObjectCrud, StreamWrapper

# create return stream wrapper
return_sw = StreamWrapper("InfoMgtReturnStream", 1, "ocid1.compartment.oc1..aaaaaaaa7lawiz4jty5scuoalmy7hxnscsx2llbdmxzs3xbak4gpmtb4vyda")

# create simple message loop
partition_cursor = return_sw.get_cursor_by_partition(return_sw.stream_client, return_sw.s_id, partition="0")
csv_rows = 97 # one row for CSV header (which should get sent from OSA pipeline), 96 'data' rows thereafter
message_loop_generator = return_sw.simple_message_loop(return_sw.stream_client, return_sw.s_id, partition_cursor, csv_rows)

objectCrud = ObjectCrud()

# upload each message from return stream to object storage
batch = None
csv_body = ""
rows_in_csv_so_far = 0
for message in message_loop_generator:
    if batch == None:
        batch = message.offset / csv_rows
    decoded_row = b64decode(message.value.encode()).decode()
    csv_body += decoded_row + "\n"
    rows_in_csv_so_far += 1
    # leaving these two print statements so others can easily verify, in case residual messages are currently retained in the streams.
    # print('rows_in_csv_so_far:', rows_in_csv_so_far)
    # print('decoded_row:', decoded_row)
    if rows_in_csv_so_far == csv_rows:
        objectCrud.upload_new_object("StreamBucket", batch, csv_body)
        batch = None
        rows_in_csv_so_far = 0