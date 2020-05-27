package internal

import (
	"golang.org/x/exp/mmap"

	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

const (
	defaultHeaderValSize   = 8
	defaultValPageSize     = 64 * 1024 * 1024
	defaultValfileFilename = "%09d.val"
	valHeaderSize          = 8 * 2
	valFilePath            = "/tmp/00000000.val"
)

// value page struct
// default 64mb
// 
//
// +--------+----------+------------+--------+
// | count  | valSizes | valOffsets |  buf   |
// | uint64 | []uint64 |  []uint64  | []byte |
// +--------+----------+------------+--------+
type ValPage struct {
	pageSize   uint64
	usedSize   uint64
	bufOffset  uint64
	count      uint64
	valSizes   []uint64
	valOffsets []uint64
	buf        []byte
}

func NewValPage(pageSize uint64) (*ValPage, error) {

	usedSize := uint64(8)
	bufOffset := uint64(0)

	count := uint64(0)
	valSizes := make([]uint64, 0)
	valOffsets := make([]uint64, 0)
	buf := make([]byte, pageSize)

	return &ValPage{
		pageSize:   pageSize,
		usedSize:   usedSize,
		bufOffset:  bufOffset,
		count:      count,
		valSizes:   valSizes,
		valOffsets: valOffsets,
		buf:        buf,
	}, nil
}

// append a value item
func (p *ValPage) Append(valSize uint64, val []byte) error {
	if valSize+valHeaderSize+p.usedSize > p.pageSize {
		return errors.New("overflow")
	}

	copy(p.buf[p.bufOffset:p.bufOffset+valSize], val)
	p.valSizes = append(p.valSizes, valSize)
	p.valOffsets = append(p.valOffsets, p.bufOffset)

	p.count++
	p.bufOffset += valSize
	p.usedSize += valSize + valHeaderSize
	return nil
}

type ValPageWriter struct {
	f      *os.File
	enc    *ValEncoder
	offset uint32
}

func NewValPageWriter(path string) (*ValPageWriter, error) {

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
	w := bufio.NewWriter(f)

	offset := uint32(0)
	enc := NewValEncoder(w)

	return &ValPageWriter{
		f:      f,
		enc:    enc,
		offset: offset,
	}, nil
}

func (pw *ValPageWriter) Write(p *ValPage) (uint32, uint32, error) {
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

type ValEncoder struct {
	w *bufio.Writer
}

func NewValEncoder(w io.Writer) *ValEncoder {
	return &ValEncoder{w: bufio.NewWriter(w)}
}

// encode value data to disk format
func (e *ValEncoder) Encode(p *ValPage) (uint32, error) {
	headerBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(headerBuf, p.count)
	if _, err := e.w.Write(headerBuf); err != nil {
		return 0, errors.Wrap(err, "failed writing value header count")
	}
	for i := uint64(0); i < p.count; i++ {
		binary.BigEndian.PutUint64(headerBuf, p.valSizes[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing value header valueSize")
		}
	}
	for i := uint64(0); i < p.count; i++ {
		binary.BigEndian.PutUint64(headerBuf, p.valOffsets[i])
		if _, err := e.w.Write(headerBuf); err != nil {
			return 0, errors.Wrap(err, "failed writing value header valueOffset")
		}
	}

	// page alignment
	baseOff := defaultHeaderValSize + p.count*2*defaultHeaderValSize
	if _, err := e.w.Write(p.buf[0 : defaultValPageSize-baseOff]); err != nil {
		return 0, errors.Wrap(err, "failed writing value buf")
	}
	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing value page")
	}

	return uint32(1), nil
}

type ValReader struct {
	reader *mmap.ReaderAt
	l      uint64
	buf    []byte
}

func NewValReader() (*ValReader, error) {
	reader, err := mmap.Open(valFilePath)
	if err != nil {
		return &ValReader{}, err
	}
	l := uint64(reader.Len())
	buf := make([]byte, defaultValPageSize)

	return &ValReader{
		reader: reader,
		l:      l,
		buf:    buf,
	}, nil
}

func (r *ValReader) ReadAt(offset uint64) ([]byte, error) {
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

// read data from disk and get values
func (r *ValReader) Read(pageOffset, valOffset uint64) ([]byte, error) {
	buf, err := r.ReadAt(pageOffset)
	if err != nil {
		return nil, err
	}
	kOff := 0
	tmpBuf := make([]byte, defaultHeaderValSize)
	copy(tmpBuf, buf[kOff:kOff+defaultHeaderValSize])
	count := binary.BigEndian.Uint64(tmpBuf)
	kOff += defaultHeaderValSize
	if valOffset > count {
		return nil, errors.New("val offset overflow")
	}

	baseOff := defaultHeaderValSize + count*2*defaultHeaderValSize
	valSizes := make([]uint64, count)
	valOffsets := make([]uint64, count)

	vals := make([][]byte, count)
	for i := uint64(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderValSize])
		valSize := binary.BigEndian.Uint64(tmpBuf)
		valSizes[i] = valSize
		kOff += defaultHeaderValSize
	}
	for i := uint64(0); i < count; i++ {
		copy(tmpBuf, buf[kOff:kOff+defaultHeaderValSize])
		valOffset := binary.BigEndian.Uint64(tmpBuf)
		valOffsets[i] = valOffset
		kOff += defaultHeaderValSize
	}
	for i := uint64(0); i < count; i++ {
		valBuf := make([]byte, valSizes[i])
		copy(valBuf, buf[baseOff+valOffsets[i]:baseOff+valOffsets[i]+valSizes[i]])
		vals[i] = valBuf
	}

	return vals[valOffset], nil
}
