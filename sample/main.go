package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/pgm/treeply"
)

func start(remoteAddr string, socketAddr string) error {
	log.Printf("starting...")
	workDir, err := os.MkdirTemp(os.TempDir(), "test")
	if err != nil {
		panic(err)
	}

	var remote treeply.RemoteProvider
	if strings.HasPrefix(remoteAddr, "gs://") {
		ctx := context.Background()
		if remoteAddr[len(remoteAddr)-1] != '/' {
			remoteAddr = remoteAddr + "/"
		}
		remote = treeply.NewGCSRemoteProvider(ctx, remoteAddr)
	} else {
		remote = &treeply.DirRemoteProvider{Root: remoteAddr}
	}
	fs, err := treeply.NewFileService(remote, workDir, 10000)
	if err != nil {
		panic(err)
	}

	log.Printf("create listener...")
	err = treeply.CreateListener(socketAddr, fs)
	if err != nil {
		panic(err)
	}

	return nil
}

func main() {

	app := &cli.App{
		Name:  "boom",
		Usage: "make an explosive entrance",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "listen",
				Value: "/tmp/treeply",
				Usage: "The path to bind for the socket",
			},
		},
		Action: func(ctx *cli.Context) error {
			remoteAddr := ctx.Args().Get(0)
			socketAddr := ctx.String("listen")
			return start(remoteAddr, socketAddr)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}
