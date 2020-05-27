package main

import (
	"github.com/b41sh/ikv/internal"
)

func main() {
	server, _ := internal.NewServer()
	server.Run()
}
