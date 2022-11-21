# dune-sync
Components for syncing off-chain data with Dune Community Sources


# Local Development


1. clone repo
2. Several Makefile commands:
```shell
make install
```
```shell
make check # (runs black, pylint and mypy --strict)
```
```shell
make test # Runs all tests
```

## Docker
### Build
```shell
docker build -t local_dune_sync .
```

You must provide valid environment variables as specified in [.env.sample](.env.sample)
### Run
```shell
docker run -v ${PWD}data:/app/data --env-file .env local_dune_sync
```
