# update-node-info

This geneerates the `node-info.csv` file we've been publishing to `https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv`.

This used to be run on beehive1 and pulled directly from the mysql database. It's been migrated to use the beehive nodes API instead.

It's not being run by waggle@terra.mcs.anl.gov as a cronjob:

```txt
* * * * * /usr/bin/python3 /home/waggle/update-node-info.py
```
