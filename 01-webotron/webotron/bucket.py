# -*- coding: utf-8 -*-

"""Classes for S3 Buckets."""

from pathlib import Path
import mimetypes
from functools import reduce

import boto3
from botocore.exceptions import ClientError

from hashlib import md5
import util


class BucketManager:
    """Manage an S3 Bucket."""

    # Equal AWS default multi-Upload Chunk size
    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """Create a BucketManager object."""
        self.session = session
        self.s3 = self.session.resource('s3')

        # Usually not necessary to change
        # Set multi-upload chunk size to AWS default
        # to ensure md5 is calculated using the same chunk size
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_chunksize=self.CHUNK_SIZE,
            multipart_threshold=self.CHUNK_SIZE
        )

        # Use to store path/file to eTag value from S3
        self.manifest = {}

    def all_buckets(self):
        """Get an interator for all buckets."""
        return self.s3.buckets.all()

    def get_bucket(self, bucket_name):
        """Get a bucket by name."""
        return self.s3.Bucket(bucket_name)

    def get_region_name(self, bucket):
        """Get the bucket's region name."""
        client = self.s3.meta.client
        bucket_location = client.get_bucket_location(Bucket=bucket.name)

        # Will return for all region except us-east-1 where it
        # return None.
        return bucket_location["LocationConstraint"] or 'us-east-1'

    def get_bucket_url(self, bucket):
        """Get the website URL for this bucket."""
        return "http://{}.{}".format(
            bucket.name,
            util.get_endpoint(self.get_region_name(bucket)).host
            )

    def all_objects(self, bucket_name):
        """Get an iterator for all objects in bucket."""
        return self.s3.Bucket(bucket_name).objects.all()

    def init_bucket(self, bucket_name):
        """Crete new bucket, or return existing one by name."""
        s3_bucket = None
        try:
            s3_bucket = self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                }
            )
        except ClientError as error:
            # IF bucket already exist, just assign it to s3_bucket.
            if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                s3_bucket = self.s3.Bucket(bucket_name)
            else:
                raise error

        s3_bucket.upload_file(
            'index.html',
            'index.html',
            ExtraArgs={
                'ContentType': 'text/html'
                },
            Config=self.transfer_config)

        return s3_bucket

    def set_policy(self, bucket):
        """Set bucket policy to be readable by everyone."""
        policy = """
        {
          "Version":"2012-10-17",
          "Statement":[{
          "Sid":"PublicReadGetObject",
          "Effect":"Allow",
          "Principal": "*",
              "Action":["s3:GetObject"],
              "Resource":["arn:aws:s3:::%s/*"
              ]
            }
          ]
        }
        """ % bucket.name

        # Strip blank,CR, LF from begin or end of string.
        policy = policy.strip()

        pol = bucket.Policy()
        pol.put(Policy=policy)

    def configure_website(self, bucket):
        """Set bucket default index and error document."""
        bucket.Website().put(WebsiteConfiguration={
            'ErrorDocument': {
                'Key': 'error.html'
            },
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        })

    def load_manifest(self, bucket):
        """Load manifest for caching purposes."""
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        # Retrieve list of ETag from all object in s3 bucket
        for page in paginator.paginate(Bucket=bucket.name):
            for obj in page.get('Contents', []):
                self.manifest[obj['Key']] = obj['ETag']

    @staticmethod
    def hash_data(data):
        """Generate md5 hash for data."""
        hash = md5()
        hash.update(data)

        return hash

    def gen_etag(self, path):
        """Generate etag for file."""
        hashes = []
        with open(path, 'rb') as f:
            # Read file in chunk size and calculate MD5
            while True:
                data = f.read(self.CHUNK_SIZE)

                # Break loop at end of file
                if not data:
                    break

                hashes.append(self.hash_data(data))

            # If file is empty, no md5 hash
            if not hashes:
                return
            # If file is exactly 1 chunk i.e. < chunksize
            elif len(hashes) == 1:
                # Enclose md5 in "" as eTag = "md5-digest"
                return '"{}"'.format(hashes[0].hexdigest())
            # If multi chunk with multi hashes i.e. > chunksize
            else:
                # Take a hash of each part of the data and hashes together
                # h.digest() for h in hashes = a digest for @ of the curr hash
                # x + y = appending the digests to get 1 long string with
                # each of the digest appended to them
                # self.hash_data = and hash them

                # Reduce will take another function in this case a lambda
                # which will take 2 arguments. Reduce will take a list of
                # things and iterate over it. And just append each element
                # to the previous one. Each element is going to be digested
                # h in hashes of the hash
                hash = self.hash_data(reduce(
                                            lambda x,
                                            y: x + y,
                                            (h.digest() for h in hashes)))

                # The first part will be hash of hashes. The second part
                # will be the number of chunks of data
                return '"{}-{}"'.format(hash.hexdigest(), len(hashes))

    def upload_file(self, bucket, path, key):
        """Upload path to s3 bucket at key."""
        # Use the file ext to guess file type or default to text/plain.
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'

        # Compare generated eTag to s3 ETag
        # If equal return and don't upload
        etag = self.gen_etag(path)
        if self.manifest.get(key, '') == etag:
            print("Skipping {}, etags match".format(key))
            return

        # Transfer_config - set multi-upload chunk size
        # to match AWS default to make sure we use the
        # same value to calculate MD5
        print("Uploading {}".format(key))
        return bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
            },
            Config=self.transfer_config
        )

    def sync(self, pathname, bucket_name):
        """Sync contents of path to buckets."""
        bucket = self.s3.Bucket(bucket_name)
        self.load_manifest(bucket)
        # root is the pathname provided.
        # expanduser and resolve will convert ~/ (home) to full user path
        root = Path(pathname).expanduser().resolve()

        # Loop via target and extract all the files.
        # The for loop will call handle_directory repeatedly
        # until all the files are found.
        def handle_directory(target):
            for p in target.iterdir():
                if p.is_dir():
                    handle_directory(p)
                if p.is_file():
                    # For Win convert Key and Path to Posix before upload.
                    self.upload_file(bucket, p.as_posix(),
                                     p.relative_to(root).as_posix())

        # Call a function within it.
        handle_directory(root)
