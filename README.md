# kvrocks-backup

A simple tool to backup and restore a Kvrocks database to/from a S3 object storage.

## Make a backup
```sh
docker run --rm \
    -e AWS_ACCESS_KEY_ID=xxxxxxxxxx \
    -e AWS_SECRET_ACCESS_KEY=xxxxxxxxxx \
    -e AWS_ENDPOINT_URL=https://s3.us-east-2.amazonaws.com \
    -e AWS_REGION=us-east-2 \
    -e AWS_BUCKET=bucket-name \
    -e KVROCKS_URL=redis://host.docker.internal:6666 \
    -e KVROCKS_DIR=/kvrocks_data \
    -v ./kvrocks_data:/kvrocks_data:ro \
    ghcr.io/sneakersapi/kvrocks-backup --prefix "my-backups" backup
```

> [!TIP]
> We recommend mounting the volume using `ro` flag on Docker. 
> This might avoid unwanted restore from being started on a production instance.

## Schedule backups

This starts an internal cron-scheduler that runs backup on a defined interval.

```sh
docker run --rm \
    -e AWS_ACCESS_KEY_ID=xxxxxxxxxx \
    -e AWS_SECRET_ACCESS_KEY=xxxxxxxxxx \
    -e AWS_ENDPOINT_URL=https://s3.us-east-2.amazonaws.com \
    -e AWS_REGION=us-east-2 \
    -e AWS_BUCKET=bucket-name \
    -e KVROCKS_URL=redis://host.docker.internal:6666 \
    -e KVROCKS_DIR=/kvrocks_data \
    -v ./kvrocks_data:/kvrocks_data:ro \
    ghcr.io/sneakersapi/kvrocks-backup --prefix "my-backups" cron "0 * * * *" # hourly backup
```

## Restore a backup

> [!CAUTION]
> - This command will shutdown the server and requires a manual restart.
> - Restore process will replace content directly inside `db` folder of kvrocks.
>   This process might be dangerous and not safe for restoring a production instance.

```sh
docker run --rm \
    -e AWS_ACCESS_KEY_ID=xxxxxxxxxx \
    -e AWS_SECRET_ACCESS_KEY=xxxxxxxxxx \
    -e AWS_ENDPOINT_URL=https://s3.us-east-2.amazonaws.com \
    -e AWS_REGION=us-east-2 \
    -e AWS_BUCKET=bucket-name \
    -e KVROCKS_URL=redis://host.docker.internal:6666 \
    -e KVROCKS_DIR=/kvrocks_data \
    -v ./kvrocks_data:/kvrocks_data \
    ghcr.io/sneakersapi/kvrocks-backup restore "prefix/to/backup"
```

## Credits

| ![KicksDB](https://wsrv.nl/?w=64&h=64&url=https://kicks.dev/logo-round.png) | ![Kvrocks](https://wsrv.nl/?w=64&h=64&url=https://kvrocks.apache.org/img/logo.svg) |
|:-:|:-:|
| [KicksDB](https://kicks.dev) | [Kvrocks](https://kvrocks.apache.org/) |