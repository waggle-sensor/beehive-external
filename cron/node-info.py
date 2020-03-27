import csv
import sys
from datetime import datetime
import pymysql
import pymysql.cursors


out = csv.DictWriter(sys.stdout, ['node_id', 'vsn', 'rssh_port',
                                  'opmode', 'project', 'description', 'location', 'lat', 'lon', 'iccid', 'imei'])
out.writeheader()

conn = pymysql.connect(host='beehive-data', user='waggle', password='waggle',
                       db='waggle', cursorclass=pymysql.cursors.DictCursor)

with conn.cursor() as cursor:
    cursor.execute("""
		select 
			lower(node_id) as node_id, name as vsn, reverse_ssh_port as rssh_port,
			opmode, project, description, location, iccid, imei
		from nodes
		order by isnull(name), name asc
	""")
    for row in cursor.fetchall():
        out.writerow(row)

sys.stderr.write(f'Last updated on {datetime.now()}\n')
sys.stderr.flush()
