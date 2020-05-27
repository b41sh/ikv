package main

import (
	"github.com/b41sh/ikv/internal"
)

func main() {
	client := internal.NewClient()
	client.Run()
}
