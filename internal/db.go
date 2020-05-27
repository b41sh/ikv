package internal

import (
	"errors"
	"fmt"
	art "github.com/plar/go-adaptive-radix-tree"
)

// pos is used to store value postion in value file
type Pos struct {
	valPageId uint32
	valOffset uint32
}

type Db struct {
	ir   *IdxReader
	vr   *ValReader
	tree art.Tree
}

func NewDb() (*Db, error) {
	ir, _ := NewIdxReader()
	vr, _ := NewValReader()
	tree := art.New()

	return &Db{
		ir:   ir,
		vr:   vr,
		tree: tree,
	}, nil
}

// init db
// read index file and build adaptive-radix-tree
func (db *Db) Init() error {
	fmt.Println("building index ...")

	offset := uint32(0)
	for {
		keys, valPageIds, valOffsets, err := db.ir.Read(offset)
		if err != nil {
			break
		}
		for i := 0; i < len(keys); i++ {
			key := keys[i]
			valPageId := valPageIds[i]
			valOffset := valOffsets[i]

			pos := Pos{
				valPageId: valPageId,
				valOffset: valOffset,
			}
			db.tree.Insert(art.Key(key), art.Value(pos))
		}

		offset += defaultIdxPageSize
	}
	fmt.Println("build index success")

	return nil
}

// first search in tree index
// if key is exist, we can get a postion
// then get the value from data file
// @todo use buffer pool to store recent page
func (db *Db) Get(key string) ([]byte, error) {

	posValue, found := db.tree.Search(art.Key(key))
	if !found {
		return nil, errors.New("key not found")
	}

	pos, ok := posValue.(Pos)
	if !ok {
		return nil, errors.New("key not found")
	}
	pageOffset := uint64(pos.valPageId * defaultValPageSize)
	valOffset := uint64(pos.valOffset)

	value, err := db.vr.Read(pageOffset, valOffset)
	if err != nil {
		return nil, errors.New("key not found")
	}
	return value, nil
}
