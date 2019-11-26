# coding: utf-8
# Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.

import filecmp
import oci
from oci.object_storage.models import CreateBucketDetails

class ObjectCrud:
    def __init__(self, config_filename):
        config_filename = config_filename or "~/.oci/config"
        self.config = oci.config.from_file(config_filename)
        self.compartment_id = self.config["tenancy"] # self.config["bucket_compartment"] # self.config["tenancy"]
        self.object_storage = oci.object_storage.ObjectStorageClient(self.config)
        self.namespace = self.object_storage.get_namespace().data

    def create_bucket(self, bucket_name):
        bucket_name = bucket_name or "python-sdk-example-bucket"
        print("Creating a new bucket {!r} in compartment {!r}".format(
        bucket_name, self.compartment_id))
        request = CreateBucketDetails()
        request.compartment_id = self.compartment_id
        request.name = bucket_name
        bucket = self.object_storage.create_bucket(self.namespace, request) # pylint: disable=unused-variable

    def upload_new_object(self, bucket_name, object_name, data):
        object_name = object_name or "python-sdk-example-object"
        data = data or b"Hello, World!"
        print("Uploading new object {!r}".format(object_name))
        obj = self.object_storage.put_object( # pylint: disable=unused-variable
            self.namespace,
            bucket_name,
            object_name,
            data)

        # Prove that the object was actually uploaded
        same_obj = self.object_storage.get_object(
            self.namespace,
            bucket_name,
            object_name)

        print("{!r} == {!r}: {}".format(
            data, same_obj.data.content,
            data == same_obj.data.content))





# print('Uploading a file to object storage')

# # First create a sample file
# sample_content = b'a' * 1024 * 1024 * 5
# with open('example_file', 'wb') as f:
#     f.write(sample_content)

# # Then upload the file to Object Storage
# example_file_object_name = 'example_file_obj'
# with open('example_file', 'rb') as f:
#     obj = object_storage.put_object(namespace, bucket_name, example_file_object_name, f)

# # Retrieve the file, streaming it into another file in 1 MiB chunks
# print('Retrieving file from object storage')
# get_obj = object_storage.get_object(namespace, bucket_name, example_file_object_name)
# with open('example_file_retrieved', 'wb') as f:
#     for chunk in get_obj.data.raw.stream(1024 * 1024, decode_content=False):
#         f.write(chunk)

# print('Uploaded and downloaded files are the same: {}'.format(filecmp.cmp('example_file', 'example_file_retrieved')))

# print("Deleting object {}".format(object_name))
# object_storage.delete_object(namespace, bucket_name, object_name)

# print("Deleting object {}".format(example_file_object_name))
# object_storage.delete_object(namespace, bucket_name, example_file_object_name)

# print("Deleting bucket {}".format(bucket_name))
# object_storage.delete_bucket(namespace, bucket_name)