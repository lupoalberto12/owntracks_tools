#!/usr/bin/env python3

import argparse
import datetime
import json
import time
from dateutil import parser
from paho.mqtt import client, publish

class ProtocolAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, getattr(client, value))

def convert_timestamp_to_int(timestamp):
  date_time = parser.parse(timestamp)
  return int(date_time.timestamp())

parser_args = argparse.ArgumentParser(description='Import Google Location History into OwnTracks')
parser_args.add_argument('-H', '--host', default='localhost', help='MQTT host (localhost)')
parser_args.add_argument('-p', '--port', type=int, default=1883, help='MQTT port (1883)')
parser_args.add_argument('--protocol', action=ProtocolAction, default=client.MQTTv31, help='MQTT protocol (MQTTv31)')
parser_args.add_argument('--cacerts', help='Path to files containing trusted CA certificates')
parser_args.add_argument('--cert', help='Path to file containing TLS client certificate')
parser_args.add_argument('--key', help='Path to file containing TLS client private key')
parser_args.add_argument('--tls-version', help='TLS protocol version')
parser_args.add_argument('--ciphers', help='List of TLS ciphers')
parser_args.add_argument('-u', '--username', help='MQTT username')
parser_args.add_argument('-P', '--password', help='MQTT password')
parser_args.add_argument('-i', '--clientid', help='MQTT client-ID')
parser_args.add_argument('-t', '--topic', required=True, help='MQTT topic')
parser_args.add_argument('filename', help='Path to file containing JSON-formatted data from Google Location History exported by Google Takeout')
args = parser_args.parse_args()

if args.username != None:
    auth={
        'username': args.username,
        'password': args.password
    }
else:
    auth = None

if args.cacerts != None:
    tls = {
        'ca_certs': args.cacerts,
        'certfile': args.cert,
        'keyfile': args.key,
        'tls_version': args.tls_version,
        'ciphers': args.ciphers
    }
else:
    tls = None


with open(args.filename) as lh:
    lh_data = json.load(lh)
    for location in lh_data['locations']:
        messages = []
        location_keys = location.keys()
        payload = {
            '_type': 'location',
            'tid': 'Go'
        }
        if 'timestamp' in location_keys:
            payload['tst'] = convert_timestamp_to_int(location['timestamp'])
            print (payload['tst'])
        if 'latitudeE7' in location_keys:
            payload['lat'] = location['latitudeE7'] / 10000000
        if 'longitudeE7' in location_keys:
            payload['lon'] = location['longitudeE7'] / 10000000
        if 'accuracy' in location_keys:
            payload['acc'] = location['accuracy']
        if 'altitude' in location_keys:
            payload['alt'] = location['altitude']
        messages.append(
            {
                'topic': args.topic,
                'payload': json.dumps(payload),
                'qos': 2
            }
        )
        print (messages)
        publish.multiple(
            messages,
            hostname=args.host,
            port=args.port,
            client_id=args.clientid,
            auth=auth,
            tls=tls
        )
        time.sleep(3)
    del lh_data
