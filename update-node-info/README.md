# update-node-info

This generates the file file we've been publishing to `https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/node-info.csv`.

This used to be run on beehive1 and pulled directly from the mysql database. It's now using the beehive nodes API instead now that we've migrated mysql. Note: The beehive nodes API is still restricted to inside MCS - if it were public, this endpoint may not be needed at all.

It's now being run by waggle@terra.mcs.anl.gov as a cronjob:

```txt
* * * * * /usr/bin/python3 /home/waggle/update-node-info.py
```

Generating this _from_ beehive at the publish site also has the advantage that we don't have to copy ssh keys to beehive just for this.
