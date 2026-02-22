package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
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

		paginator := s3.NewListObjectsV2Paginator(s3client, &s3.ListObjectsV2Input{
			Bucket: new(c.String("bucket")),
			Prefix: new(c.StringArg("backup-prefix")),
		})
		tm := transfermanager.New(s3client)

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("cannot crawl s3: %w", err)
			}

			for _, obj := range page.Contents {
				dw, err := tm.GetObject(ctx, &transfermanager.GetObjectInput{
					Bucket: new(c.String("bucket")),
					Key:    obj.Key,
				})
				if err != nil {
					return fmt.Errorf("cannot download: %w", err)
				}

				var reader io.Reader = dw.Body
				fp := path.Base(*obj.Key)

				if strings.HasSuffix(*obj.Key, ".zst") {
					reader, err = zstd.NewReader(reader)
					if err != nil {
						return fmt.Errorf("cannot decode zstd: %w", err)
					}

					fp = fp[:len(fp)-4]
				}

				f, err := os.Create(path.Join(c.String("kvrocks-dir"), "db", fp))
				if err != nil {
					return fmt.Errorf("cannot create destination file: %w", err)
				}

				if _, err = io.Copy(f, reader); err != nil {
					return fmt.Errorf("cannot copy: %w", err)
				}

				log.Println("Restored", fp)
			}
		}

		log.Println("Restore finished in", time.Since(start).String())

		return nil
	},
}
