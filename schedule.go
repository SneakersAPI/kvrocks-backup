package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/urfave/cli/v3"
)

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
	},
	Action: func(ctx context.Context, c *cli.Command) error {
		s3client := ctx.Value("s3client").(*s3.Client)
		client := ctx.Value("redis").(*redis.Client)

		scheduler := cron.New()
		_, err := scheduler.AddFunc(c.StringArg("cron"), func() {
			prefix := c.String("prefix")
			if !c.Bool("replace") {
				date := time.Now().Format("2006-01-02_15-04-05")
				prefix = path.Join(prefix, date)
			}

			log.Println("Start saving to:", prefix)
			if err := Backup(
				s3client,
				client,
				c.String("bucket"),
				c.String("kvrocks-dir"),
				prefix,
			); err != nil {
				log.Printf("could not backup: %s\n", err)
			}
		})
		if err != nil {
			return fmt.Errorf("cannot register scheduler: %s", err)
		}

		log.Println("Start scheduler")
		scheduler.Run()

		return nil
	},
}
