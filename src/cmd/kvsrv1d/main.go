package main

import (
	"fmt"
	"os"

	"dynamo-kvsrv/kvsrv1"
	"dynamo-kvsrv/tester1"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], kvsrv.StartKVServer); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
