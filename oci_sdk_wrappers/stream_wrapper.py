# coding: utf-8
# Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.

import oci
import sys
import time
from base64 import b64encode, b64decode

# ==========================================================
# This file provides an example of basic streaming usage
# * - List streams
# * - Get a Stream
# * - Create a Stream
# * - Delete a Stream
# * - Publish to a Stream
# * - Consume from a stream partition using cursor
# * - Consume from a stream using a group cursor
# Documentation : https://docs.cloud.oracle.com/iaas/Content/Streaming/Concepts/streamingoverview.htm

# Usage : python stream_example.py <compartment id>

class StreamWrapper:
    def __init__(self, STREAM_NAME, PARTITIONS, compartment_ocid, config_filename = None):
        STREAM_NAME = STREAM_NAME or "SdkExampleStream"
        PARTITIONS = PARTITIONS or 1

        config_filename = config_filename or "~/.oci/config"

        # Load the default configuration
        config = oci.config.from_file(config_filename)

        # Create a StreamAdminClientCompositeOperations for composite operations.
        stream_admin_client = oci.streaming.StreamAdminClient(config)
        stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

        # compartment = sys.argv[1]
        self.compartment = compartment_ocid

        # We  will reuse a stream if its already created.
        # This will utilize list_streams() to determine if a stream exists and return it, or create a new one.
        stream = self.get_or_create_stream(stream_admin_client, self.compartment, STREAM_NAME,
                                    PARTITIONS, stream_admin_client_composite).data

        print("Created (or using already created) Stream {} with id : {}".format(stream.name, stream.id))

        # Streams are assigned a specific endpoint url based on where they are provisioned.
        # Create a stream client using the provided message endpoint.
        self.stream_client = oci.streaming.StreamClient(config, service_endpoint=stream.messages_endpoint)
        self.s_id = stream.id

    def publish_example_messages(self, client, stream_id):
        # Build up a PutMessagesDetails and publish some messages to the stream
        message_list = []
        for i in range(100):
            key = "key" + str(i)
            value = "value" + str(i)
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


    def get_or_create_stream(self, client, compartment_id, stream_name, partition, sac_composite):

        list_streams = client.list_streams(compartment_id, name=stream_name,
                                        lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
        if list_streams.data:
            # If we find an active stream with the correct name, we'll use it.
            print("An active stream {} has been found".format(stream_name))
            sid = list_streams.data[0].id
            return self.get_stream(sac_composite.client, sid)

        print("No Active stream  {} has been found; Creating it now.".format(stream_name))
        print("Creating stream {} with {} partitions.".format(stream_name, partition))

        # Create stream_details object that need to be passed while creating stream.
        stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition,
                                                                compartment_id=self.compartment, retention_in_hours=24)

        # Since stream creation is asynchronous; we need to wait for the stream to become active.
        response = sac_composite.create_stream_and_wait_for_state(
            stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
        return response


    def get_stream(self, admin_client, stream_id):
        return admin_client.get_stream(stream_id)


    def delete_stream(self, client, stream_id):
        print("Deleting Stream {}".format(stream_id))
        print("Stream deletion is an asynchronous operation, give it some time to complete.")
        client.delete_stream_and_wait_for_state(stream_id, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_DELETED])


    def get_cursor_by_partition(self, client, stream_id, partition):
        print("Creating a cursor for partition {}".format(partition))
        cursor_details = oci.streaming.models.CreateCursorDetails(
            partition=partition,
            type=oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON)
        response = client.create_cursor(stream_id, cursor_details)
        cursor = response.data.value
        return cursor


    def simple_message_loop(self, client, stream_id, initial_cursor, limit):
        cursor = initial_cursor
        while True:
            get_response = client.get_messages(stream_id, cursor, limit=limit)
            # No messages to process. return.
            if not get_response.data:
                pass
                # return

            # Process the messages
            print("Read {} messages".format(len(get_response.data)))
            for message in get_response.data:
                # print("message:", message)
                # print("decoded message value: {}".format(b64decode(message.value.encode()).decode()))
                yield message

            # get_messages is a throttled method; clients should retrieve sufficiently large message
            # batches, as to avoid too many http requests.
            time.sleep(1)
            # use the next-cursor for iteration
            cursor = get_response.headers["opc-next-cursor"]


    def get_cursor_by_group(self, sc, sid, group_name, instance_name):
        print("Creating a cursor for group {}, instance {}".format(group_name, instance_name))
        cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name, instance_name=instance_name,
                                                                    type=oci.streaming.models.
                                                                    CreateGroupCursorDetails.TYPE_TRIM_HORIZON,
                                                                    commit_on_get=True)
        response = sc.create_group_cursor(sid, cursor_details)
        return response.data.value

# sw = StreamWrapper('InfoMgtReturnStream', 1, 'ocid1.compartment.oc1..aaaaaaaa7lawiz4jty5scuoalmy7hxnscsx2llbdmxzs3xbak4gpmtb4vyda', None)

# # Publish some messages to the stream
# sw.publish_example_messages(sw.stream_client, sw.s_id)

# # Use a cursor for getting messages; each get_messages call will return a next-cursor for iteration.
# # There are a couple kinds of cursors.
# # A cursor can be created at a given partition/offset.
# # This gives explicit offset management control to the consumer.

# print("Starting a simple message loop with a partition cursor")
# partition_cursor = sw.get_cursor_by_partition(sw.stream_client, sw.s_id, partition="0")
# sw.simple_message_loop(sw.stream_client, sw.s_id, partition_cursor)

# # A cursor can be created as part of a consumer group.
# # Committed offsets are managed for the group, and partitions
# # are dynamically balanced amongst consumers in the group.
# group_cursor = sw.get_cursor_by_group(sw.stream_client, sw.s_id, "example-group", "example-instance-1")
# sw.simple_message_loop(sw.stream_client, sw.s_id, group_cursor)

# # Cleanup; remember to delete streams which are not in use.
# sw.delete_stream(stream_admin_client_composite, sw.s_id)