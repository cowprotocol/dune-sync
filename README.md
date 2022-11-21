# dune-sync
Components for syncing off-chain data with Dune Community Sources


# Local Development


1. Clone Repo `git clone git@github.com:cowprotocol/dune-sync.git`
2. Several Makefile Commands `make XXX`
Key make commands are; `install, check, test`


# Docker
### Build
```shell
docker build -t local_dune_sync .
```

You must provide valid environment variables as specified in [.env.sample](.env.sample)
### Run
```shell
docker run -v ${PWD}data:/app/data --env-file .env local_dune_sync
```
