package main

import (
	"fmt"
	"io"
	"log"
	"os"

	llog "log"

	"github.com/urfave/cli/v2"

	logging "github.com/ipfs/go-log/v2"
)

func init() {
	llog.SetOutput(io.Discard)
}

func main() { os.Exit(main1()) }

func main1() int {
	app := &cli.App{
		Name:   "noah",
		Usage:  "Utility for working with car files",
		Before: before,
		Commands: []*cli.Command{
			create0Cmd,
			create1Cmd,
			extractCmd,
			listCmd,
			commpCmd,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Println(err)
		fmt.Printf("%s\n", err.Error())
		return 1
	}
	return 0
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("noah", "INFO")
	return nil
}
