package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

var RestoreCmd = &cli.Command{
	Name:        "restore",
	Usage:       "Restore a backup",
	Description: "This command will stop Kvrocks and needs a manual restart, don't use with docker always-restart.",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name:      "backup-prefix",
			UsageText: "Full prefix to backup",
		},
	},
	Action: func(ctx context.Context, c *cli.Command) error {
		s3client := ctx.Value("s3client").(*s3.Client)
		client := ctx.Value("redis").(*redis.Client)

		start := time.Now()

		log.Println("Shutting down...")
		if err := client.Shutdown(ctx).Err(); err != nil {
			// shutdown will give an error.
			// return fmt.Errorf("cannot shutdown: %w", err)
		}

		log.Println("Downloading...")
		tm := transfermanager.New(s3client)
		output, err := tm.DownloadDirectory(
			ctx,
			&transfermanager.DownloadDirectoryInput{
				Bucket:      new(c.String("bucket")),
				Destination: new(path.Join(c.String("kvrocks-dir"), "db")),
				KeyPrefix:   new(c.StringArg("backup-prefix")),
			},
		)
		if err != nil {
			return fmt.Errorf("cannot download backup: %w", err)
		}

		log.Println("Restore finished in", time.Since(start).String())
		log.Println("Downloaded files:", output.ObjectsDownloaded)
		log.Println("Failed downloads:", output.ObjectsFailed)

		return nil
	},
}
