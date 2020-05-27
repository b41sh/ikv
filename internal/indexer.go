package internal

import (
	"fmt"
)

type Indexer struct {
	r *DataStreamReader
}

func NewIndexer() *Indexer {
	r, _ := NewDataStreamReader()
	return &Indexer{
		r: r,
	}
}

// read original data file
// build index file and value file
func (idxer *Indexer) Run() {
	fmt.Println("building index ...")
	valPageId := uint32(0)
	valOffset := uint32(0)

	valPage, _ := NewValPage(defaultValPageSize)
	idxPage, _ := NewIdxPage(defaultIdxPageSize)

	valPageWriter, _ := NewValPageWriter(valFilePath)
	idxPageWriter, _ := NewIdxPageWriter(idxFilePath)

	for {
		keySize, err := idxer.r.ReadKeySize()
		if err != nil {
			_, _, _ = valPageWriter.Write(valPage)
			_, _, _ = idxPageWriter.Write(idxPage)
			break
		}
		key, _ := idxer.r.ReadKey(keySize)
		valSize, _ := idxer.r.ReadValueSize()
		//offset := idxer.r.GetOffset()
		value, _ := idxer.r.ReadValue(valSize)

		// write value page
		err = valPage.Append(valSize, value)
		if err != nil {
			_, _, _ = valPageWriter.Write(valPage)
			// current page is full, add a new one
			valPage, _ = NewValPage(defaultValPageSize)
			// @todo
			_ = valPage.Append(valSize, value)
			valPageId++
			valOffset = uint32(0)
		} else {
			valOffset++
		}

		// write index page
		err = idxPage.Append(keySize, valPageId, valOffset, key)
		if err != nil {
			_, _, _ = idxPageWriter.Write(idxPage)
			// current page is full, add a new one
			idxPage, _ = NewIdxPage(defaultIdxPageSize)
			// @todo
			_ = idxPage.Append(keySize, valPageId, valOffset, key)
		}
	}
	fmt.Println("build index success")
}
