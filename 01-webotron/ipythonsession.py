import boto3
session = boto3.Session(profile_name='myPythonAutomation')
s3 = session.resource('s3')
