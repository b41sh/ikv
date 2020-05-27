package internal

import (
	"bufio"
	"encoding/binary"
	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"
	"io"
	"os"
)

const (
	defaultHeaderKeySize   = 4
	defaultIdxPageSize     = 32 * 1024 * 1024
	defaultIdxfileFilename = "%09d.idx"
	idxHeaderSize          = 4 * 4
	idxFilePath            = "/tmp/00000000.idx"
)

// index page struct
// default 32mb
// 
//
// +--------+----------+------------+------------+------------+--------+
// | count  | keySizes | keyOffsets | valPageIds | valOffsets |  buf   |
// | uint32 | []uint32 |  []uint32  |  []uint32  | []uint32   | []byte |
// +--------+----------+------------+------------+------------+--------+
type IdxPage struct {
	pageSize   uint32
	usedSize   uint32
	bufOffset  uint32
	count      uint32
	keySizes   []uint32
	keyOffsets []uint32
	valPageIds []uint32
	valOffsets []uint32
	buf        []byte
}

func NewIdxPage(pageSize uint32) (*IdxPage, error) {

	usedSize := uint32(4)
	bufOffset := uint32(0)

	count := uint32(0)
	keySizes := make([]uint32, 0)
	keyOffsets := make([]uint32, 0)
	valPageIds := make([]uint32, 0)
	valOffsets := make([]uint32, 0)
	buf := make([]byte, pageSize)

	return &IdxPage{
		pageSize:   pageSize,
		usedSize:   usedSize,
		bufOffset:  bufOffset,
		count:      count,
		keySizes:   keySizes,
		keyOffsets: keyOffsets,
		valPageIds: valPageIds,
		valOffsets: valOffsets,
		buf:        buf,
	}, nil
}

// append a index item
func (p *IdxPage) Append(keySize, valPageId, valOffset uint32, key []byte) error {
	if keySize+idxHeaderSize+p.usedSize > p.pageSize {
		return errors.New("overflow")
	}
	copy(p.buf[p.bufOffset:p.bufOffset+keySize], key)
	p.keySizes = append(p.keySizes, keySize)
	p.keyOffsets = append(p.keyOffsets, p.bufOffset)
	p.valPageIds = append(p.valPageIds, valPageId)
	p.valOffsets = append(p.valOffsets, valOffset)

	p.count++
	p.bufOffset += keySize
	p.usedSize += keySize + idxHeaderSize
	return nil
}

type IdxPageWriter struct {
	f      *os.File
	enc    *IdxEncoder
	offset uint32
}

func NewIdxPageWriter(path string) (*IdxPageWriter, error) {
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
	w := bufio.NewWriter(f)

	offset := uint32(0)
	enc := NewIdxEncoder(w)

	return &IdxPageWriter{
		f:      f,
		enc:    enc,
		offset: offset,
	}, nil
}

// write index to disk
func (pw *IdxPageWriter) Write(p *IdxPage) (uint32, uint32, error) {
	if pw.f == nil {
		return 0, 0, errors.New("file error")
	}

	n, err := pw.enc.Encode(p)
	if err != nil {
		return 0, 0, err
	}
	pw.offset += n

	return pw.offset, n, nil
}

type IdxEncoder struct {
	w *bufio.Writer
}

func NewIdxEncoder(w io.Writer) *IdxEncoder {
	return &IdxEncoder{w: bufio.NewWriter(w)}
}

// encode index data to disk format
func (e *IdxEncoder) Encode(p *IdxPage) (uint32, error) {
	headerBuf := make([]byte, defaultHeaderKeySize)
	binary.BigEndian.PutUint32(headerBuf, p.count)
	if _, err := e.w.Write(headerBuf); err != nil {
		return 0, errors.Wrap(err, "failed writing index header count")
	}
	for i := uint32(0); i < p.count; i++ {
		binary.BigEndian.PutUint32(headerBuf, p.keySizes[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing index header keySize")
		}
	}
	for i := uint32(0); i < p.count; i++ {
		binary.BigEndian.PutUint32(headerBuf, p.keyOffsets[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing index header keyOffset")
		}
	}
	for i := uint32(0); i < p.count; i++ {
		binary.BigEndian.PutUint32(headerBuf, p.valPageIds[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing index header valuePageId")
		}
	}
	for i := uint32(0); i < p.count; i++ {
		binary.BigEndian.PutUint32(headerBuf, p.valOffsets[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing index header valueOffset")
		}
	}

	// page alignment
	baseOff := defaultHeaderKeySize + p.count*4*defaultHeaderKeySize
	if _, err := e.w.Write(p.buf[0 : defaultIdxPageSize-baseOff]); err != nil {
		return 0, errors.Wrap(err, "failed writing index buf")
	}
	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing index page")
	}

	return uint32(1), nil
}

type IdxReader struct {
	reader *mmap.ReaderAt
	l      uint32
	buf    []byte
}

func NewIdxReader() (*IdxReader, error) {
	reader, err := mmap.Open(idxFilePath)
	if err != nil {
		return &IdxReader{}, err
	}
	l := uint32(reader.Len())
	buf := make([]byte, defaultIdxPageSize)

	return &IdxReader{
		reader: reader,
		l:      l,
		buf:    buf,
	}, nil
}

// read data from disk
func (r *IdxReader) ReadAt(offset uint32) ([]byte, error) {
	if offset > r.l {
		return nil, errors.New("overflow")
	}
	n, err := r.reader.ReadAt(r.buf, int64(offset))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, errors.New("no data")
	}
	return r.buf, nil
}

// read data from disk and get keys
func (r *IdxReader) Read(offset uint32) ([][]byte, []uint32, []uint32, error) {
	buf, err := r.ReadAt(offset)
	if err != nil {
		return nil, nil, nil, err
	}
	kOff := 0
	tmpBuf := make([]byte, defaultHeaderKeySize)
	copy(tmpBuf, buf[kOff:kOff+defaultHeaderKeySize])
	count := binary.BigEndian.Uint32(tmpBuf)
	kOff += defaultHeaderKeySize

	baseOff := defaultHeaderKeySize + count*4*defaultHeaderKeySize
	keySizes := make([]uint32, count)
	keyOffsets := make([]uint32, count)
	valPageIds := make([]uint32, count)
	valOffsets := make([]uint32, count)
	keys := make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderKeySize])
		keySize := binary.BigEndian.Uint32(tmpBuf)
		keySizes[i] = keySize
		kOff += defaultHeaderKeySize
	}
	for i := uint32(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderKeySize])
		keyOffset := binary.BigEndian.Uint32(tmpBuf)
		keyOffsets[i] = keyOffset
		kOff += defaultHeaderKeySize
	}
	for i := uint32(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderKeySize])
		valPageId := binary.BigEndian.Uint32(tmpBuf)
		valPageIds[i] = valPageId
		kOff += defaultHeaderKeySize
	}
	for i := uint32(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderKeySize])
		valOffset := binary.BigEndian.Uint32(tmpBuf)
		valOffsets[i] = valOffset
		kOff += defaultHeaderKeySize
	}
	for i := uint32(0); i < count; i++ {
		keyBuf := make([]byte, keySizes[i])
		copy(keyBuf, buf[baseOff+keyOffsets[i]:baseOff+keyOffsets[i]+keySizes[i]])
		keys[i] = keyBuf
	}

	return keys, valPageIds, valOffsets, nil
}
