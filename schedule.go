package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/urfave/cli/v3"
)

const BACKUP_FORMAT = "2006-01-02_15-04-05"

var ScheduleCmd = &cli.Command{
	Name:  "schedule",
	Usage: "Execute backup on a schedule",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name:      "cron",
			UsageText: "cron expression (e.g. 0 * * * *)",
		},
	},
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "replace",
			Usage: "Maintain a single backup",
		},
		&cli.BoolFlag{
			Name:  "delete",
			Usage: "Delete the backup folder after upload",
		},
		&cli.BoolFlag{
			Name:  "compress",
			Usage: "Compress files using Zstd",
		},
		&cli.IntFlag{
			Name:  "purge",
			Usage: "Purge backup older than defined number of days",
		},
	},
	Action: func(ctx context.Context, c *cli.Command) error {
		s3client := ctx.Value("s3client").(*s3.Client)
		client := ctx.Value("redis").(*redis.Client)

		scheduler := cron.New()
		_, err := scheduler.AddFunc(c.StringArg("cron"), func() {
			prefix := c.String("prefix")
			if !c.Bool("replace") {
				date := time.Now().Format(BACKUP_FORMAT)
				prefix = path.Join(prefix, date)
			}

			log.Println("Start saving to:", prefix)
			if err := Backup(
				s3client,
				client,
				c.String("bucket"),
				c.String("kvrocks-dir"),
				prefix,
				c.Bool("delete"),
				c.Bool("compress"),
			); err != nil {
				log.Printf("could not backup: %s\n", err)
			}
		})
		if err != nil {
			return fmt.Errorf("cannot register scheduler: %s", err)
		}

		if c.IsSet("purge") {
			scheduler.AddFunc("0 * * * *", func() {
				purgeBackups(s3client, c.String("bucket"), c.String("prefix"), c.Int("purge"))
			})
		}

		log.Println("Start scheduler")
		scheduler.Run()

		return nil
	},
}

func purgeBackups(s3client *s3.Client, bucket, prefix string, day int) error {
	prefixWithSuffix := prefix
	if !strings.HasSuffix(prefixWithSuffix, "/") {
		prefixWithSuffix += "/"
	}

	paginator := s3.NewListObjectsV2Paginator(s3client, &s3.ListObjectsV2Input{
		Bucket:    new(bucket),
		Prefix:    new(prefixWithSuffix),
		Delimiter: new("/"),
	})
	ctx := context.Background()
	limit := time.Now().Add(-time.Duration(day) * time.Hour * 24)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("cannot paginate s3: %w", err)
		}

		for _, obj := range page.CommonPrefixes {
			pf := path.Base(*obj.Prefix)
			ts, err := time.Parse(BACKUP_FORMAT, pf)
			if err != nil {
				return fmt.Errorf("cannot parse time: %w", err)
			}

			if ts.Before(limit) {
				if err := deletePrefix(ctx, s3client, bucket, prefixWithSuffix+pf); err != nil {
					return fmt.Errorf("cannot delete backup: %w", err)
				}
			}
		}
	}

	return nil
}

func deletePrefix(ctx context.Context, client *s3.Client, bucket, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: new(bucket),
		Prefix: new(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]s3types.ObjectIdentifier, 0, len(page.Contents))

		for _, obj := range page.Contents {
			objects = append(objects, s3types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: new(bucket),
			Delete: &s3types.Delete{
				Objects: objects,
				Quiet:   new(true),
			},
		})
		if err != nil {
			return err
		}
	}

	log.Println("Purged old backup:", prefix)
	return nil
}
