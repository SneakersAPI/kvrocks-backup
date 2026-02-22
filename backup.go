package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"
)

var BackupCmd = &cli.Command{
	Name:  "backup",
	Usage: "Backup Kvrocks instance now",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "delete",
			Usage: "Delete the backup folder after upload",
		},
		&cli.BoolFlag{
			Name:  "compress",
			Usage: "Compress files using Zstd",
		},
	},
	Action: func(ctx context.Context, c *cli.Command) error {
		s3client := ctx.Value("s3client").(*s3.Client)
		client := ctx.Value("redis").(*redis.Client)

		return Backup(
			s3client,
			client,
			c.String("bucket"),
			c.String("kvrocks-dir"),
			c.String("prefix"),
			c.Bool("delete"),
			c.Bool("compress"),
		)
	},
}

// Backup runs `BGSAVE` on the Redis instance, polls `LASTSAVE` until
// the save is created then upload the backup folder to S3.
func Backup(s3client *s3.Client, rClient *redis.Client, bucket, dir, prefix string, delete, compress bool) error {
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

	tm := transfermanager.New(s3client)
	err = filepath.WalkDir(
		path.Join(dir, "backup"),
		func(p string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return nil
			}

			f, err := os.Open(p)
			if err != nil {
				return fmt.Errorf("cannot read file: %w", err)
			}
			defer f.Close()

			pr, pw := io.Pipe()
			defer pr.Close()

			fp := path.Join(prefix, path.Base(p))

			if compress {
				fp += ".zst"
				go func() {
					defer pw.Close()

					encoder, err := zstd.NewWriter(pw)
					if err != nil {
						pw.CloseWithError(err)
						return
					}
					defer encoder.Close()

					_, err = io.Copy(encoder, f)
					if err != nil {
						pw.CloseWithError(err)
						return
					}
				}()
			} else {
				go func() {
					defer pw.Close()

					_, err := io.Copy(pw, f)
					if err != nil {
						pw.CloseWithError(err)
						return
					}
				}()
			}

			_, err = tm.UploadObject(
				ctx,
				&transfermanager.UploadObjectInput{
					Bucket: new(bucket),
					Body:   pr,
					Key:    new(fp),
				},
			)
			if err != nil {
				return fmt.Errorf("cannot put object: %w", err)
			}

			log.Println("Uploaded file:", fp)

			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("cannot upload file: %w", err)
	}

	log.Println("Upload finished in", time.Since(start).String())

	if delete {
		if err := os.RemoveAll(path.Join(dir, "backup")); err != nil {
			return fmt.Errorf("could not delete backup folder: %w", err)
		}

		log.Println("Delete backup folder")
	}

	return nil
}
