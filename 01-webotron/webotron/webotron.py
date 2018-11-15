import boto3
import click
import mimetypes
from botocore.exceptions import ClientError
from pathlib import Path

session = boto3.Session(profile_name='myPythonAutomation')
s3 = session.resource('s3')

@click.group()
def cli():
    "Webotron deploys websites to AWS"
    pass

@cli.command('list-buckets')
def list_buckets():
    "List all s3 buckets"
    for bucket in s3.buckets.all():
        print(bucket)

@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    'List objects in an S3 bucket'
    for obj in s3.Bucket(bucket).objects.all():
        print(obj)

@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    "Create and configure S3 bucket"
    s3_bucket = None

    try:
        s3_bucket = s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': session.region_name}
        )
    except ClientError as e:
        # IF bucket already exist, just assign it to s3_bucket
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            s3_bucket = s3.Bucket(bucket)
        else:
            raise e

    s3_bucket.upload_file(
        'index.html',
        'index.html',
        ExtraArgs={'ContentType': 'text/html'})

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
    """ % s3_bucket.name

    # Strip blank,CR, LF from begin or end of string
    policy = policy.strip()

    pol = s3_bucket.Policy()
    pol.put(Policy=policy)

    ws = s3_bucket.Website()
    ws.put(WebsiteConfiguration={
        'ErrorDocument': {
            'Key': 'error.html'
        },
        'IndexDocument': {
            'Suffix': 'index.html'
        }
    })

    return

def upload_file(s3_bucket, path, key):
    "Use the file ext to guess file type or default to text/plain"
    content_type = mimetypes.guess_type(key)[0] or 'text/plain'
    s3_bucket.upload_file(
        path,
        key,
        ExtraArgs={
            'ContentType': content_type
        })

@cli.command('sync')
# Use click helper function to ensure pathname exists
@click.argument('pathname', type=click.Path(exists=True))
# Click path exists doesn't check ~ properly in Windows
# @click.argument('pathname')
@click.argument('bucket')
def sync(pathname, bucket):
    "Sync contents of PATHNAME to BUCKET"
    s3_bucket = s3.Bucket(bucket)

    # root is the pathname provided
    # expanduser and resolve will convert ~/ (home) to full user path
    root = Path(pathname).expanduser().resolve()

    # Loop via target and extract all the files
    # The for loop will call handle_directory repeatedly until all the files
    # are found
    def handle_directory(target):
        for p in target.iterdir():
            if p.is_dir(): handle_directory(p)
            if p.is_file():
                "For Windows convert Key and Path to Posix before upload"
                print("Uploading {} from {}".format(p.relative_to(root).as_posix(),p))
                upload_file(s3_bucket, p.as_posix(), p.relative_to(root).as_posix())

    # Call a function within it
    handle_directory(root)

if __name__ == '__main__':
    cli()
