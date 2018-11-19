# -*- coding: utf-8 -*-

"""Classes for Route 53 domains."""

import uuid


class DomainManager:
    """Manage a Route 53 domain."""

    def __init__(self, session):
        """Create DomainManager object."""
        self.session = session
        self.client = self.session.client('route53')

    # Can match domain.com or
    # subdomain.domain.com
    def find_host_zone(self, domain_name):
        """Find zone matching domain_name."""
        paginator = self.client.get_paginator('list_hosted_zones')
        for page in paginator.paginate():
            for zone in page['HostedZones']:
                # Don't need to check . (-1 char) at the end
                if domain_name.endswith(zone['Name'][:-1]):
                    return zone

        return None


    # domain_name = 'subdomain.kittentest.automatingaws.net'
    # zone_name = 'automatingaws.net.'
    def created_hosted_zone(self, domain_name):
        """Created a hosted zone to match domain_name."""
        # Split domain_name with '.'
        # Extracted last two e.g. automatingaws net
        # Join them back '.' e.g. automatingaws.net
        # Append '.' in the end e.g. automatingaws.net.
        zone_name = '.'.join(domain_name.split('.')[-2:]) + '.'
        return self.client.create_hosted_zone(
            Name=zone_name,
            # Generate a random string to ensure we don't send
            # same reques multiple times
            CallerReference=str(uuid.uuid4())
        )

    def create_s3_domain_record(self, zone, domain_name, endpoint):
        """Create a domain record in zone for domain_name."""
        return self.client.change_resource_record_sets(
            HostedZoneId=zone['Id'],
            ChangeBatch={
                'Comment': 'Created by webotron',
                'Changes': [{
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': domain_name,
                        'Type': 'A',
                        'AliasTarget': {
                            'HostedZoneId': endpoint.zone,
                            'DNSName': endpoint.host,
                            'EvaluateTargetHealth': False
                        }
                    }
                }
                ]
            }
        )
