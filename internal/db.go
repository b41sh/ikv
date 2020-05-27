package internal

import (
	"errors"
	art "github.com/plar/go-adaptive-radix-tree"
)

type Db struct {
	r    *DataStreamReader
	dr   *DataReader
	tree art.Tree
}

func NewDb() (*Db, error) {
	r, _ := NewDataStreamReader()
	dr, _ := NewDataReader()
	tree := art.New()

	return &Db{
		r:    r,
		dr:   dr,
		tree: tree,
	}, nil
}

func (db *Db) Init() error {
	for {
		keySize, err := db.r.ReadKeySize()
		if err != nil {
			break
		}
		key, _ := db.r.ReadKey(keySize)
		valSize, _ := db.r.ReadValueSize()
		offset := db.r.GetOffset()
		_ = db.r.Skip(valSize)

		pos := Pos{
			Offset:    offset,
			ValueSize: valSize,
		}

		db.tree.Insert(art.Key(key), art.Value(pos))
	}
	return nil
}

func (db *Db) Get(key string) ([]byte, error) {

	posValue, found := db.tree.Search(art.Key(key))
	if !found {
		return nil, errors.New("key not found")
	}

	pos, ok := posValue.(Pos)
	if !ok {
		return nil, errors.New("key not found")
	}

	value, err := db.dr.ReadAt(pos.ValueSize, pos.Offset)
	if err != nil {
		return nil, errors.New("key not found")
	}
	return value, nil
}
