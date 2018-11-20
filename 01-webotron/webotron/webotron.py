#!/usr/bin/Python
# -*- coding: utf-8 -*-

"""Webotron: Deploy websites with AWS.

Webotron automates the process of deploying static websites to AWS.
- Configure AWS S3 list_buckets
  - Create them
  - Set them up for static website hosting
  - Deploy local files to them
- Configure DNS with AWS Route 53
- Configure a Content Delivery Network and SSL with AWS CloudFront
"""
import boto3
import click

from bucket import BucketManager
from domain import DomainManager
from certificate import CertificateManager
from cdn import DistributionManager
import util

session = None
bucket_manager = None
domain_manager = None
cert_manager = None
dist_manager = None


@click.group()
@click.option('--profile', default=None,
              help="Use a given AWS profile.")
def cli(profile):
    """Webotron deploys websites to AWS."""
    global session, bucket_manager, domain_manager, cert_manager, dist_manager

    session_cfg = {}
    if profile:
        session_cfg['profile_name'] = profile

    session = boto3.Session(**session_cfg)
    bucket_manager = BucketManager(session)
    domain_manager = DomainManager(session)
    cert_manager = CertificateManager(session)
    dist_manager = DistributionManager(session)


@cli.command('list-buckets')
def list_buckets():
    """List all s3 buckets."""
    for bucket in bucket_manager.all_buckets():
        print(bucket)


@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    """List objects in an S3 bucket."""
    for obj in bucket_manager.all_objects(bucket):
        print(obj)


@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    """Create and configure S3 bucket."""
    s3_bucket = bucket_manager.init_bucket(bucket)
    bucket_manager.set_policy(s3_bucket)
    bucket_manager.configure_website(s3_bucket)

    return


@cli.command('sync')
# Use click helper function to ensure pathname exists.
@click.argument('pathname', type=click.Path(exists=True))
# Click path exists doesn't check ~ properly in Windows.
# @click.argument('pathname')
@click.argument('bucket')
def sync(pathname, bucket):
    """Sync contents of PATHNAME to BUCKET."""
    bucket_manager.sync(pathname, bucket)
    print("Bucket URL = " +
          bucket_manager.get_bucket_url(bucket_manager.s3.Bucket(bucket)))


@cli.command('setup-domain')
@click.argument('domain')
def setup_domain(domain):
    """Configure  DOMAIN to point to BUCKET."""
    bucket = bucket_manager.get_bucket(domain)

    zone = domain_manager.find_host_zone(domain) \
        or domain_manager.created_hosted_zone(domain)

    endpoint = util.get_endpoint(bucket_manager.get_region_name(bucket))
    domain_manager.create_s3_domain_record(zone, domain, endpoint)
    print("Domain configured: http://{}".format(domain))


@cli.command('find-cert')
@click.argument('domain')
def find_cert(domain):
    """Find certificate matching domain."""
    print(cert_manager.find_matching_cert(domain))


@cli.command('setup-cdn')
@click.argument('domain')
@click.argument('bucket')
def setup_cdn(domain, bucket):
    """Setup CloudFront CDN."""
    dist = dist_manager.find_matching_dist(domain)

    if not dist:
        cert = cert_manager.find_matching_cert(domain)
        if not cert:  # SSL is not optional at this time
            print("Error: No matching cert found.")
            return

        dist = dist_manager.create_dist(domain, cert)
        print("Waiting for distribution deployment...")
        dist_manager.await_deploy(dist)

    zone = domain_manager.find_host_zone(domain) \
        or domain_manager.created_hosted_zone(domain)

    domain_manager.create_cf_domain_record(zone, domain, dist['DomainName'])
    print("Domain configured: https://{}".format(domain))

    return


@cli.command('setup-cdn-domain')
@click.argument('domain')
def setup_cdn_domain(domain):
    """Setup CloudFront CDN Domain."""
    dist = dist_manager.find_matching_dist(domain)

    if not dist:  # Distribution must be setup first
        print("Error: Please setup distribution first.")
        return

    print("Waiting for distribution deployment...")
    dist_manager.await_deploy(dist)

    zone = domain_manager.find_host_zone(domain) \
        or domain_manager.created_hosted_zone(domain)

    domain_manager.create_cf_domain_record(zone, domain, dist['DomainName'])
    print("Domain configured: https://{}".format(domain))

    return


if __name__ == '__main__':
    cli()
