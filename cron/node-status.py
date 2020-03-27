#!/usr/bin/env python3

import csv
import json
import pymysql
import pymysql.cursors
import requests
import subprocess
import sys
from datetime import datetime
from io import StringIO


# MYSQL_HOST = 'localhost'
MYSQL_HOST = 'beehive-data.cels.anl.gov'

# get the node info from the database
sys.stderr.write(f'{datetime.now()} Getting node info...\n')
sys.stderr.flush()

conn = pymysql.connect(host=MYSQL_HOST, user='waggle', password='waggle',
                       db='waggle', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cursor:
    cursor.execute("""
        select
            lower(substring(node_id, 5, 12)) as node_id, reverse_ssh_port as rssh_port,
            name as vsn, opmode, project, description
        from nodes
    """)
    nodes = dict((row['node_id'], row) for row in cursor.fetchall())

for node_id, node in nodes.items():
    nodes[node_id]['node_id'] = f"0000{nodes[node_id]['node_id']}"
    if nodes[node_id]['vsn'] is None:
        nodes[node_id]['vsn'] = ''


# determine if the node has set up a reverse tunnel with beehive
sys.stderr.write(f'{datetime.now()} Getting rssh connections...\n')
sys.stderr.flush()

proc = subprocess.Popen([
    "docker exec -i beehive-sshd netstat -nlp | grep -e '^tcp ..*127.0.0.1:[0-9][0-9][0-9][0-9][0-9]..*sshd: root' | awk '{print $4}' | sed 's/127.0.0.1://'"
], shell=True, stdout=subprocess.PIPE)
stdout, _ = proc.communicate()

ports = set(str(x) for x in stdout.decode('utf-8').split('\n'))

for node_id, attrs in nodes.items():
    nodes[node_id]['rssh_connection'] = str(attrs['rssh_port']) in ports


# determine if there is a rabbitmq connection
sys.stderr.write(f'{datetime.now()} Getting rmq connections...\n')
sys.stderr.flush()

proc = subprocess.Popen([
    'docker exec -i beehive-rabbitmq rabbitmqctl list_connections user'
], shell=True, stdout=subprocess.PIPE)
stdout, _ = proc.communicate()

raw_rmq_ids = set(x for x in stdout.decode('utf-8').split('\n'))
rmq_ids = set()
for raw_node_id in raw_rmq_ids:
    parts = raw_node_id.split('-')
    if len(parts) != 2:
        continue
    rmq_ids.add(parts[1].lower().rstrip())

for node_id, attrs in nodes.items():
    nodes[node_id]['rmq_connection'] = attrs['node_id'] in rmq_ids


# get old pipeline data frames
sys.stderr.write(f'{datetime.now()} Getting V1 pipeline info...\n')
sys.stderr.flush()

proc = subprocess.Popen([
    "docker logs --since=5m beehive-loader-raw | awk '{print $1}'"
], shell=True, stdout=subprocess.PIPE)
stdout, _ = proc.communicate()

old_node_ids = set(str(x).lower() for x in stdout.decode('utf-8').split('\n'))


# get new pipeline data frames
sys.stderr.write(f'{datetime.now()} Getting V2 pipeline info...\n')
sys.stderr.flush()

proc = subprocess.Popen([
    "docker logs --since=5m beehive-data-loader | cut -d',' -f2"
], shell=True, stdout=subprocess.PIPE)
stdout, _ = proc.communicate()

new_node_ids = set(str(x).lower()[-12:]
                   for x in stdout.decode('utf-8').split('\n'))


# combine node ids
sys.stderr.write(f'{datetime.now()} Combining pipeline info...\n')
sys.stderr.flush()

data_frames = old_node_ids.union(new_node_ids)
for node_id in nodes.keys():
    has_data_frames = node_id in data_frames
    nodes[node_id]['data_frames'] = has_data_frames
    if has_data_frames:
        nodes[node_id]['rmq_connection'] = True


# dump it all out
sys.stderr.write(f'{datetime.now()} Starting to write output CSV...\n')
sys.stderr.flush()

writer = csv.DictWriter(sys.stdout, fieldnames=[
                        'node_id', 'vsn', 'rssh_port', 'project', 'opmode', 'rssh_connection', 'rmq_connection', 'data_frames', 'description'])
writer.writeheader()

rows = sorted(filter(lambda x: x['opmode'] ==
                     'up', nodes.values()), key=lambda y: y['vsn'])
writer.writerows(rows)

rows = sorted(filter(
    lambda x: x['opmode'] == 'testing', nodes.values()), key=lambda y: y['vsn'])
writer.writerows(rows)

rows = sorted(filter(
    lambda x: x['opmode'] == 'retired', nodes.values()), key=lambda y: y['vsn'])
writer.writerows(rows)

# Updated metadata
sys.stderr.write(f'Last updated on {datetime.now()}\n')
sys.stderr.flush()
