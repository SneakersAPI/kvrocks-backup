package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:    "kvrocks-backup",
		Usage:   "A tool for saving and restoring Kvrocks instances",
		Version: "v1.0.1",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "kvrocks-url",
				Value: Getenv("KVROCKS_URL", "redis://localhost:6666"),
				Usage: "URL to Kvrocks",
			},
			&cli.StringFlag{
				Name:  "kvrocks-dir",
				Value: Getenv("KVROCKS_DIR", "./kvrocks_data"),
				Usage: "Directory to root Kvrocks",
			},
			&cli.StringFlag{
				Name:  "bucket",
				Value: Getenv("AWS_BUCKET", ""),
				Usage: "Bucket to use for backup/restore",
			},
			&cli.StringFlag{
				Name:  "prefix",
				Usage: "Prefix in bucket",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			return nil
		},
		Commands: []*cli.Command{
			BackupCmd,
			ScheduleCmd,
			RestoreCmd,
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			dsn, err := redis.ParseURL(c.String("kvrocks-url"))
			if err != nil {
				return ctx, fmt.Errorf("error parsing kvrocks url: %w", err)
			}

			client := redis.NewClient(dsn)
			if err := client.Ping(ctx).Err(); err != nil {
				return ctx, fmt.Errorf("cannot ping kvrocks: %w", err)
			}
			ctx = context.WithValue(ctx, "redis", client)

			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return ctx, fmt.Errorf("cannot configure aws: %w", err)
			}

			s3client := s3.NewFromConfig(cfg)
			return context.WithValue(ctx, "s3client", s3client), nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
