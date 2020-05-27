package main

import (
	"github.com/b41sh/ikv/internal"
)

func main() {
	indexer := internal.NewIndexer()
	indexer.Run()
}
