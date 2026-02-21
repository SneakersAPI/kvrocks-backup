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

var BackupCmd = &cli.Command{
	Name:  "backup",
	Usage: "Backup Kvrocks instance now",
	Action: func(ctx context.Context, c *cli.Command) error {
		s3client := ctx.Value("s3client").(*s3.Client)
		client := ctx.Value("redis").(*redis.Client)

		return Backup(
			s3client,
			client,
			c.String("bucket"),
			c.String("kvrocks-dir"),
			c.String("prefix"),
		)
	},
}

// Backup runs `BGSAVE` on the Redis instance, polls `LASTSAVE` until
// the save is created then upload the backup folder to S3.
func Backup(s3client *s3.Client, rClient *redis.Client, bucket, dir, prefix string) error {
	ctx := context.Background()
	start := time.Now()

	ts, err := rClient.LastSave(ctx).Result()
	if err != nil {
		return fmt.Errorf("cannot read last-save: %w", err)
	}

	log.Println("Starting backup...")
	if err := rClient.BgSave(ctx).Err(); err != nil {
		return fmt.Errorf("cannot backup kvrocks: %w", err)
	}

	for {
		ls, err := rClient.LastSave(ctx).Result()
		if err != nil {
			return fmt.Errorf("cannot read last-save: %w", err)
		}

		if ls > ts {
			break
		}

		time.Sleep(2500 * time.Millisecond)
	}

	log.Println("Uploading...")
	tm := transfermanager.New(s3client)
	output, err := tm.UploadDirectory(
		ctx,
		&transfermanager.UploadDirectoryInput{
			Bucket:              new(bucket),
			Source:              new(path.Join(dir, "backup")),
			Recursive:           new(true),
			KeyPrefix:           new(prefix),
			FollowSymbolicLinks: new(true),
		},
	)
	if err != nil {
		return fmt.Errorf("could not upload to s3: %w", err)
	}

	log.Println("Upload finished in", time.Since(start).String())
	log.Println("Files uploaded:", output.ObjectsUploaded)
	log.Println("Files errors:", output.ObjectsFailed)

	return nil
}
