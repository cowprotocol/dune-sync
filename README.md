# dune-sync

Components for syncing off-chain data with Dune Community Sources

# Local Development

1. Clone Repo `git clone git@github.com:cowprotocol/dune-sync.git`
2. If using VS Conde, open in devcontainer to ensure same setup as the final container
3. Several Makefile Commands `make XXX`
   Key make commands are; `install, check, test`

# Docker

### Build

```shell
docker build -t local_dune_sync .
```

You must provide valid environment variables as specified in [.env.sample](.env.sample)

### Run Local

```shell
docker run -v ${PWD}/data:/app/data --env-file .env local_dune_sync
```

### Run Remote

You will need to attach a volume and have an env file configuration. This example

- mounts `$PWD/data`
- and assumes `.env` file is in `$PWD`

```shell
docker run -v ${PWD}/data:/app/data --env-file .env ghcr.io/cowprotocol/dune-sync:latest
```


### Breaking Changes

Whenever the schema changes, we must coordinate with Dune that the data must be dropped and the table rebuilt.
For this we have provided a script `scripts/empty_bucket.py` which can be called to delete all data from their 
buckets and our backup volume. This should only be run whilst in coordination with their team about the changes. 
They will "stop the stream", drop the table on their side and restart the stream. 
In the event that a hard reset is performed without the proper coordination, 
it is likely that duplicate records will appear in their production environment (i.e. the interface). 

So, the process is:

- Contact a Dune Team Member (@dsalv)
- Mention that we need to rebuild table XYZ (because of a schema change)
- Once they are aware/prepared run
```shell
docker run -v ${PWD}/data:/app/data \
    --env-file .env \
    ghcr.io/cowprotocol/dune-sync \
    --sync-table SYNC_TABLE
```

This will empty the buckets and repopulate with the appropriate changes.
