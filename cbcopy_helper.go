//go:build cbcopy_helper

package main

import (
	"os"

	"github.com/cloudberrydb/cbcopy/helper"
)

func main() {
	defer helper.Stop()

	helper.Start()
	os.Exit(helper.ErrCode)
}
