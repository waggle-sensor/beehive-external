#!/usr/bin/env python3

import codecs
import csv
import io
import logging
import re
import warnings
import smtplib
import ssl
import sys
from argparse import ArgumentParser
from collections import defaultdict
from email.message import EmailMessage
from typing import Dict, Iterable, List, Tuple

import arrow
import requests
from arrow.factory import ArrowParseWarning


logging.basicConfig(
    format='%(asctime)s %(levelname)-5s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    stream=sys.stderr
)

warnings.simplefilter('ignore', ArrowParseWarning)


INFO_CSV_URL = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv'

STATUS_CSV_URL = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-status.csv'

MCS_DOWNLOADS_URL = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/index.php'

HREF_REGEX = re.compile(r'href=[\'"]?([^\'" >]+)')

SKIP_EMAIL = False


# UTILS


def get_info_csv() -> Dict[str, dict]:
    """Downloads the node-info csv from the MCS server and returns it as a
    dictionary indexed by node id:

    {
        '0000001e0610ba46': {
            'node_id': '0000001e0610ba46',
            'vsn': '004',
            'rssh_port': '50052',
            'opmode': 'up',
            'project': 'AoT_Chicago',
            'description': 'AoT Chicago (S) [C]',
            'location': 'State St & Jackson Blvd Chicago IL',
        },
    }
    """
    logging.info('downloading info csv')

    res = requests.get(INFO_CSV_URL)
    res.raise_for_status()

    nodes = {}

    reader = csv.DictReader(res.content.decode('utf-8').splitlines())
    for row in reader:
        nodes[row['node_id']] = row

    return nodes


def get_status_csv() -> Dict[str, dict]:
    """Downloads the node-status csv from the MCS server and returns it as a
    dictionary indexed by node id:

    {
        '0000001e0610ba46': {
            'node_id': '0000001e0610ba46',
            'vsn': '004',
            'project': 'AoT_Chicago',
            'rssh_port': '50052',
            'opmode': 'up',
            'ssh_connection': True,
            'rmq_connection': True,
            'data_frames': True,
            'description': 'AoT Chicago (S) [C]',
        },
    }
    """
    logging.info('downloading status csv')

    res = requests.get(STATUS_CSV_URL)
    res.raise_for_status()

    nodes = {}

    reader = csv.DictReader(res.content.decode('utf-8').splitlines())
    for row in reader:
        # convert string true/false to booleans
        for key, value in row.items():
            if str(value).lower() == 'true':
                row[key] = True
            elif str(value).lower() == 'false':
                row[key] = False
        # store row indexed by node id
        nodes[row['node_id']] = row

    return nodes


def get_recent_csvs() -> List[dict]:
    """Downloads all of the recent csv files from the MCS server and
    concatenates them into a single list of dictionaries:

    [
        {
            'timestamp': '2019-07-29 12:23:34',
            'node_id': '0000001e0610ba46',
            'subsystem': 'metsense',
            'sensor': 'htu21d',
            'parameter': 'temperature',
            'value_raw': 2845,
            'value_hrf': 28.45,
        },
    ]
    """
    logging.info('downloading recent data csvs')

    res = requests.get(MCS_DOWNLOADS_URL)
    res.raise_for_status()

    all_links = re.findall(HREF_REGEX, res.content.decode('utf-8'))
    recent_csvs = list(filter(lambda href: href.endswith('.complete.recent.csv'), all_links))

    measurements = []

    for url in recent_csvs:
        logging.info(f'... downloading recent csv {url!r}')

        res = requests.get(url)
        res.raise_for_status()

        reader = csv.DictReader(res.content.decode('utf-8').splitlines())
        for row in reader:
            # format the node id
            row['node_id'] = f'0000{row["node_id"]}'

            # parse the timestamp
            row['timestamp'] = arrow.get(row['timestamp'], 'YYYY/MM/DD HH:mm:ss')

            # format the raw value
            # set NA to None
            if row['value_raw'].lower() == 'na':
                row['value_raw'] = None
            # else parse a float or skip the row
            else:
                try:
                    row['value_raw'] = float(row['value_raw'])
                except ValueError:
                    continue

            # parse the hrf value
            # set NA to None
            if row['value_hrf'].lower() == 'na':
                row['value_hrf'] = None
            # else parse a float or skip the row
            else:
                try:
                    row['value_hrf'] = float(row['value_hrf'])
                except ValueError:
                    continue

            measurements.append(row)

    return measurements


def send_message(subject: str, message: str, recipients: list, node_ids: list, node_info: dict) -> None:
    """Builds a message with the message body and an embedded CSV of node
    information, and then sends the message with the given subject line to the
    recipients.
    """
    if SKIP_EMAIL:
        logging.info('skip email set -- not sending message')
        return

    # pad the message body
    message += f'\n\n{len(node_ids)} Nodes\n\n'

    # embed the node info as a csv in the body
    message_data = io.StringIO()
    writer = csv.writer(message_data)
    writer.writerow(['node_id', 'vsn', 'rssh_port', 'description', 'location'])
    for node_id in node_ids:
        info = node_info[node_id]
        writer.writerow([node_id, info['vsn'], info['rssh_port'], info['description'], info['location']])
    message += message_data.getvalue()

    # build the email message
    msg = EmailMessage()
    msg.set_content(message)
    msg['From'] = 'wagglealerts@gmail.com'
    msg['To'] = recipients
    msg['Subject'] = f'Waggle Alert: {subject} [{len(node_ids)} Nodes]'

    # send the email
    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls(context=context)
        server.login('wagglealerts@gmail.com', 'this is a really insecure password 1!')
        server.send_message(msg)
    except Exception as e:
        print(e)
    finally:
        server.quit()


def _join_errors_dict(node_ids: Iterable[str], error: str, node_errors: dict) -> dict:
    for node_id in node_ids:
        node_errors[node_id].append(error)
    return node_errors


# ANALYSIS


def up_but_no_ssh_conn(node_info: Dict[str, dict], node_status: Dict[str, dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are nominally up, but don't have an active
    SSH connection to Beehive.
    """
    logging.info('running up_but_no_ssh_conn analysis')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and not n['rssh_connection'],
            node_status.values()
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')

        message = 'The following nodes do not have an active SSH connection:\n\n'
        send_message(
            subject='No SSH Conns',
            message='The following nodes do not have an active SSH connection',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'No SSH Conn'


def up_and_ssh_but_no_rmq(node_info: Dict[str, dict], node_status: Dict[str, dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are nominally up and have an active
    SSH connection to Beehive, but don't have an active RMQ connection.
    """
    logging.info('running up_and_ssh_but_no_rmq')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['rssh_connection'] and not n['rmq_connection'],
            node_status.values()
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='No RMQ Conns',
            message='The following nodes do not have an active RMQ connction',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'No RMQ Conn'


def up_and_ssh_and_rmq_but_no_frames(node_info: Dict[str, dict], node_status: Dict[str, dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are nominally up and have an active
    SSH connection to Beehive and have an active RMQ connection, but aren't
    pushing data frames.
    """
    logging.info('running up_and_ssh_and_rmq_but_no_frames')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['rssh_connection'] and n['rmq_connection'] and not n['data_frames'],
            node_status.values()
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='No Data Frames',
            message='The following nodes are not sending data frames',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'No Data Frames'


def nc_rebooted(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC has rebooted within the last 600 seconds.
    """
    logging.info('running nc_rebooted')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] is not None and m['value_hrf'] < 600 and m['subsystem'] == 'nc' and m['parameter'] == 'uptime',
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Rebooted NC',
            message='The following nodes have recently rebooted their node controller',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Rebooted NC'


def ep_rebooted(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP has rebooted within the last 600 seconds.
    """
    logging.info('running ep_rebooted')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] is not None and m['value_hrf'] < 600 and m['subsystem'] == 'ep' and m['parameter'] == 'uptime',
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Rebooted NC',
            message='The following nodes have recently rebooted their node controller',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Rebooted EP'


def bcam_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active bottom camera (either NC or EP)
    """
    logging.info('running bcam_down')

    ep = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'ep' and m['parameter'] == 'bcam',
            measurements
        )
    }

    nc = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'nc' and m['parameter'] == 'bcam',
            measurements
        )
    }

    down = ep.intersection(nc)

    if len(down):
        logging.info(f'... matched {len(down)} nodes -- sending alert email')
        send_message(
            subject='BCam Down',
            message='The following node bottom cameras are down',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com', 'yongho.kim@anl.gov'],
            node_ids=down,
            node_info=node_info
        )
    else:
        logging.info('... no matches -- not sending alert email')

    return down, 'BCam Down'


def tcam_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active top camera (either NC or EP)
    """
    logging.info('running tcam_down')

    ep = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'ep' and m['parameter'] == 'tcam',
            measurements
        )
    }

    nc = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'nc' and m['parameter'] == 'tcam',
            measurements
        )
    }

    down = ep.intersection(nc)

    if len(down):
        logging.info(f'... matched {len(down)} nodes -- sending alert email')
        send_message(
            subject='TCam Down',
            message='The following node top cameras are down',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com', 'yongho.kim@anl.gov'],
            node_ids=down,
            node_info=node_info
        )
    else:
        logging.info('... no matches -- not sending alert email')

    return down, 'TCam Down'


def mic_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active microphone (either NC or EP)
    """
    logging.info('running mic_down')

    ep = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'ep' and m['parameter'] == 'mic',
            measurements
        )
    }

    nc = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['value_hrf'] == 0 and m['subsystem'] == 'nc' and m['parameter'] == 'mic',
            measurements
        )
    }

    down = ep.intersection(nc)

    if len(down):
        logging.info(f'... matched {len(down)} nodes -- sending alert email')
        send_message(
            subject='Mic Down',
            message='The following node microphones are down',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com', 'yongho.kim@anl.gov'],
            node_ids=down,
            node_info=node_info
        )
    else:
        logging.info('... no matches -- not sending alert email')

    return down, 'Mic Down'


def wwan_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active WWAN device.
    """
    logging.info('running wwan_down')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'device' and m['parameter'] == 'wwan' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='WWAN Down',
            message='The following nodes have a down WWAN',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'WWAN Down'


def lan_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active LAN device.
    """
    logging.info('running lan_down')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'device' and m['parameter'] == 'lan' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='LAN Down',
            message='The following nodes have a down LAN',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'LAN Down'


def modem_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active modem device.
    """
    logging.info('running modem_down')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'device' and m['parameter'] == 'modem' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Modem Down',
            message='The following nodes have a down modem',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Modem Down'


def coresense_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active Coresense device.
    """
    logging.info('running coresense_down')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'device' and m['parameter'] == 'coresense' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Coresense Down',
            message='The following nodes have a down Coresense',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Coresense Down'


def wagman_down(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who don't have an active Wagman device.
    """
    logging.info('running wagman_down')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'device' and m['parameter'] == 'wagman' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Wagman Down',
            message='The following nodes have a down Wagman',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Wagman Down'


def nc_fail_counts(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who have an NC fail count >= 3.
    """
    logging.info('running nc_fail_counts')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'wagman_fc' and m['parameter'] == 'nc' and m['value_hrf'] >= 3,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC High Fail Count',
            message='The following nodes have high fail counts for their node controllers',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC High FC'


def ep_fail_counts(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who have an EP fail count >= 3.
    """
    logging.info('running ep_fail_counts')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'wagman_fc' and m['parameter'] == 'ep' and m['value_hrf'] >= 3,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP High Fail Count',
            message='The following nodes have high fail counts for their edge processors',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP High FC'


def cs_fail_counts(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who have an CS fail count >= 3.
    """
    logging.info('running cs_fail_counts')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'wagman_fc' and m['parameter'] == 'cs' and m['value_hrf'] >= 3,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='CS High Fail Count',
            message='The following nodes have high fail counts for their Coresense boards',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'CS High FC'


def wagman_got_wiped(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose Wagman got wiped.
    """
    logging.info('running wagman_got_wiped')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['sensor'] == 'wagman_comm' and m['parameter'] == 'up' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Wagman Got Wiped',
            message='The following nodes have a wiped out Wagman',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'rajesh@mcs.anl.gov'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Wagman Wiped'


def stuck_cs_bootloader(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose Coresense bootloader is stuck.
    """
    logging.info('running stuck_cs_bootloader')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['parameter'] == 'samba' and m['value_hrf'] == 1,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Stuck Coresense Bootloader',
            message='The following nodes have a stuck Coresense bootloader',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Stuck CS Bootloader'


def check_nc_boot_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC boot disk is used >= 80%.
    """
    logging.info('running check_nc_boot_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'boot' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC Boot Disk Full',
            message='The following nodes have a (nearly) full boot disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC Boot High Disk Usage'


def check_nc_root_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC root disk is used >= 80%.
    """
    logging.info('running check_nc_root_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'root' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC Root Disk Full',
            message='The following nodes have a (nearly) full root disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC Root High Disk Usage'


def check_nc_rw_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC rw disk is used >= 80%.
    """
    logging.info('running check_nc_rw_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'rw' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC RW Disk Full',
            message='The following nodes have a (nearly) full rw disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC RW High Disk Usage'


def check_ep_boot_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP boot disk is used >= 80%.
    """
    logging.info('running check_ep_boot_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'boot' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP Boot Disk Full',
            message='The following nodes have a (nearly) full boot disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP Boot High Disk Usage'


def check_ep_root_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP root disk is used >= 80%.
    """
    logging.info('running check_ep_root_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'root' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP Root Disk Full',
            message='The following nodes have a (nearly) full root disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP Root High Disk Usage'


def check_ep_rw_disk_usage(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP rw disk is used >= 80%.
    """
    logging.info('running check_ep_rw_disk_usage')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep' and m['sensor'] == 'disk_used_ratio' and m['parameter'] == 'rw' and m['value_hrf'] >= 0.8,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP RW Disk Full',
            message='The following nodes have a (nearly) full rw disk',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP RW High Disk Usage'


def check_nc_rmq_service(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC RMQ service is unavailable.
    """
    logging.info('running check_nc_rmq_service')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'service_active' and m['parameter'] == 'rabbitmq' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC RMQ Service Unavailable',
            message='The following nodes have an unavailable RMQ on their NC',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC RMQ Unavailable'


def check_ep_rmq_service(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP RMQ service is unavailable.
    """
    logging.info('running check_ep_rmq_service')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep' and m['sensor'] == 'service_active' and m['parameter'] == 'rabbitmq' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP RMQ Service Unavailable',
            message='The following nodes have an unavailable RMQ on their EP',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP RMQ Unavailable'


def check_nc_coresense_service(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose Coresense service is unavailable.
    """
    logging.info('running check_nc_coresense_service')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'service_active' and m['parameter'] == 'coresense' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='Coresense Service Unavailable',
            message='The following nodes have an unavailable Coresense',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'Coresense Unavailable'


def check_nc_plugins_active(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose NC plugins aren't activated.
    """
    logging.info('running check_nc_plugins_active')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc' and m['sensor'] == 'plugins' and m['parameter'] == 'active' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='NC Plugins Inactive',
            message='The following nodes have inactive NC plugins',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'NC Plugins Inactive'


def check_ep_plugins_active(node_info: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes whose EP plugins aren't activated.
    """
    logging.info('running check_ep_plugins_active')

    nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep' and m['sensor'] == 'plugins' and m['parameter'] == 'active' and m['value_hrf'] == 0,
            measurements
        )
    }

    if len(nodes):
        logging.info(f'... matched {len(nodes)} nodes -- sending alert email')
        send_message(
            subject='EP Plugins Inactive',
            message='The following nodes have inactive EP plugins',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=nodes,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return nodes, 'EP Plugins Inactive'


def check_metsense(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are sending data, but are missing Metsense
    measurements.
    """
    logging.info('running check_metsense')

    up_nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True,
            node_status.values()
        )
    }

    metsense_nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'metsense',
            measurements
        )
    }

    missing_metsense = up_nodes - metsense_nodes

    if len(missing_metsense):
        logging.info(f'... matched {len(missing_metsense)} nodes -- sending alert email')
        send_message(
            subject='Missing Metsense',
            message='The following nodes are missing Metsense readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com'],
            node_ids=missing_metsense,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_metsense, 'Missing Metsense Data'


def check_lightsense(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are sending data, but are missing Lightsense
    measurements.
    """
    logging.info('running check_lightsense')

    up_nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True,
            node_status.values()
        )
    }

    lightsense_nodes = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'lightsense',
            measurements
        )
    }

    missing_lightsense = up_nodes - lightsense_nodes

    if len(missing_lightsense):
        logging.info(f'... matched {len(missing_lightsense)} nodes -- sending alert email')
        send_message(
            subject='Missing lightsense',
            message='The following nodes are missing Lightsense readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com'],
            node_ids=missing_lightsense,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_lightsense, 'Missing Lightsense Data'


def check_chemsense(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are built with Chemsense boards and are sending
    data, but are missing Chemsense measurements.
    """
    logging.info('running check_chemsense')

    chem_regex = re.compile('.*\[.*C.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up'  and n['data_frames'] == True and re.match(chem_regex, n['description']),
            node_status.values()
        )
    }

    chemsense = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'chemsense',
            measurements
        )
    }

    missing_chemsense = nodes - chemsense

    if len(missing_chemsense):
        logging.info(f'... matched {len(missing_chemsense)} nodes -- sending alert email')
        send_message(
            subject='Missing Chemsense',
            message='The following nodes are missing Chemsense readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com'],
            node_ids=missing_chemsense,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_chemsense, 'Missing Chemsense Data'


def check_alphasense(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are built with Alphasense boards and are sending
    data, but are missing Alphasense measurements.
    """
    logging.info('running check_alphasense')

    alpha_regex = re.compile('.*\[.*A.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up'  and n['data_frames'] == True and re.match(alpha_regex, n['description']),
            node_status.values()
        )
    }

    alphasense = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'alphasense',
            measurements
        )
    }

    missing_alphasense = nodes - alphasense

    if len(missing_alphasense):
        logging.info(f'... matched {len(missing_alphasense)} nodes -- sending alert email')
        send_message(
            subject='Missing alphasense',
            message='The following nodes are missing alphasense readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com'],
            node_ids=missing_alphasense,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_alphasense, 'Missing Alphasense Data'


def check_plantower(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes who are built with Plantower boards and are sending
    data, but are missing Plantower measurements.
    """
    logging.info('running check_plantower')

    plan_regex = re.compile('.*\[.*P.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True and re.match(plan_regex, n['description']),
            node_status.values()
        )
    }

    plantower = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'plantower',
            measurements
        )
    }

    missing_plantower = nodes - plantower

    if len(missing_plantower):
        logging.info(f'... matched {len(missing_plantower)} nodes -- sending alert email')
        send_message(
            subject='Missing Plantower',
            message='The following nodes are missing Plantower readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com'],
            node_ids=missing_plantower,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_plantower, 'Missing Plantower Data'


def check_image_classifier(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes that have the image classifier plugin running, but are
    missing car and pedestrian counts.
    """
    logging.info('running check_image_classifier')

    cls_regex = re.compile('.*\[.*Cls.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True and re.match(cls_regex, n['description']),
            node_status.values()
        )
    }

    img = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'image',
            measurements
        )
    }

    missing_img = nodes - img

    if len(missing_img):
        logging.info(f'... matched {len(missing_img)} nodes -- sending alert email')
        send_message(
            subject='Missing Image Classifier Counts',
            message='The following nodes are missing image classifier readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com', 'yongho.kim@anl.gov'],
            node_ids=missing_img,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_img, 'Missing Img Data'


def check_spl(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes that have the SPL plugin running, but are missing
    measurements.
    """
    logging.info('running check_spl')

    spl_regex = re.compile('.*\[.*S.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True and re.match(spl_regex, n['description']),
            node_status.values()
        )
    }

    spl = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'spl',
            measurements
        )
    }

    missing_spl = nodes - spl

    if len(missing_spl):
        logging.info(f'... matched {len(missing_spl)} nodes -- sending alert email')
        send_message(
            subject='Missing SPL Measurements',
            message='The following nodes are missing SPL readings',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu', 'seonghapark06@gmail.com', 'yongho.kim@anl.gov'],
            node_ids=missing_spl,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_spl, 'Missing SPL Data'


def check_nc_telemetry(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes that have the status plugin running, but are missing
    NC telemetry data.
    """
    logging.info('running check_nc_telemetry')

    telemetry_regex = re.compile('.*\[.*T.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True and re.match(telemetry_regex, n['description']),
            node_status.values()
        )
    }

    telemetry = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'nc',
            measurements
        )
    }

    missing_telemetry = nodes - telemetry

    if len(missing_telemetry):
        logging.info(f'... matched {len(missing_telemetry)} nodes -- sending alert email')
        send_message(
            subject='Missing NC Telemetry',
            message='The following nodes are missing NC telemetry data',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=missing_telemetry,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_telemetry, 'Missing NC Telemetry'


def check_ep_telemetry(node_info: Dict[str, dict], node_status: Dict[str, dict], measurements: List[dict]) -> Tuple[Iterable[str], str]:
    """Checks for nodes that have the status plugin running, but are missing
    EP telemetry data.
    """
    logging.info('running check_ep_telemetry')

    telemetry_regex = re.compile('.*\[.*T.*\].*')

    nodes = {
        node['node_id']
        for node in filter(
            lambda n: n['opmode'] == 'up' and n['data_frames'] == True and re.match(telemetry_regex, n['description']),
            node_status.values()
        )
    }

    telemetry = {
        measurement['node_id']
        for measurement in filter(
            lambda m: m['subsystem'] == 'ep',
            measurements
        )
    }

    missing_telemetry = nodes - telemetry

    if len(missing_telemetry):
        logging.info(f'... matched {len(missing_telemetry)} nodes -- sending alert email')
        send_message(
            subject='Missing EP Telemetry',
            message='The following nodes are missing EP telemetry data',
            recipients=['vforgione@uchicago.edu', 'sshahkarami@uchicago.edu'],
            node_ids=missing_telemetry,
            node_info=node_info
        )

    else:
        logging.info('... no matches -- not sending alert email')

    return missing_telemetry, 'Missing EP Telemetry'


if __name__ == '__main__':
    # get args
    parser = ArgumentParser()
    parser.add_argument('--skip-email', action='store_true')
    args = parser.parse_args()
    SKIP_EMAIL = args.skip_email
    if SKIP_EMAIL:
        logging.info('skipping sending email')

    # gather info
    node_info = get_info_csv()
    node_status = get_status_csv()
    measurements = get_recent_csvs()

    # collect all error stings
    node_errors = defaultdict(list)

    # connectivity checks
    node_ids, error = up_but_no_ssh_conn(node_info, node_status)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = up_and_ssh_but_no_rmq(node_info, node_status)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = up_and_ssh_and_rmq_but_no_frames(node_info, node_status)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # rebooted boards
    node_ids, error = nc_rebooted(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = ep_rebooted(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # devices that can be attached to either the nc or ep
    node_ids, error = bcam_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = tcam_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = mic_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # nc device checks
    node_ids, error = wwan_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = lan_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = modem_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = coresense_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = wagman_down(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # wagman checks
    node_ids, error = nc_fail_counts(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = ep_fail_counts(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = cs_fail_counts(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = wagman_got_wiped(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = stuck_cs_bootloader(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # disk usage
    node_ids, error = check_nc_boot_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_nc_root_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_nc_rw_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_boot_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_root_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_rw_disk_usage(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # service checks
    node_ids, error = check_nc_rmq_service(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_nc_coresense_service(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_rmq_service(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # plugin checks
    node_ids, error = check_nc_plugins_active(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_plugins_active(node_info, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_metsense(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_lightsense(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_alphasense(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_plantower(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_image_classifier(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_spl(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_nc_telemetry(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)
    node_ids, error = check_ep_telemetry(node_info, node_status, measurements)
    node_errors = _join_errors_dict(node_ids, error, node_errors)

    # print all errors to a CSV to stdout
    logging.info('building combined alerts csv')

    writer = csv.writer(sys.stdout)
    writer.writerow(['node_id', 'vsn', 'rssh_port', 'description', 'location', 'errors'])
    for node_id, errors in node_errors.items():
        info = node_info[node_id]
        writer.writerow([node_id, info['vsn'], info['rssh_port'], info['description'], info['location'], ', '.join(errors)])

