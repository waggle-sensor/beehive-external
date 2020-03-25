import requests
import csv
import sys
import os


def rename(row, k1, k2):
    row[k2] = r[k1]
    del r[k1]


print('fetching node info')
r = requests.get(
    'http://beehive1.mcs.anl.gov/api/nodes/?filter=node_id,name,reverse_ssh_port,opmode,project,description,location,lat,lon,iccid,imei')

r.raise_for_status()
rows = r.json()['data']

for r in rows:
    r['node_id'] = r['node_id'].lower().rjust(16, '0')
    rename(r, 'name', 'vsn')
    rename(r, 'reverse_ssh_port', 'rssh_port')

print('building node info')
with open('/mcs/www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv.tmp', 'w') as file:
    writer = csv.DictWriter(file, ['node_id', 'vsn', 'rssh_port', 'opmode',
                                   'project', 'description', 'location', 'lat', 'lon', 'iccid', 'imei'])

    writer.writeheader()
    writer.writerows(rows)

print('moving node info to public')
os.rename('/mcs/www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv.tmp',
          '/mcs/www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv')
