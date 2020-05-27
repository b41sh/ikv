package internal

import (
	"errors"
	"golang.org/x/exp/mmap"

	"encoding/binary"
)

type Pos struct {
	Offset    uint64
	ValueSize uint64
}

type DataReader struct {
	reader *mmap.ReaderAt
	l      int64
	buf    []byte
}

func NewDataReader() (*DataReader, error) {
	reader, err := mmap.Open("/tmp/org.data")
	if err != nil {
		return &DataReader{}, err
	}

	l := int64(reader.Len())
	buf := make([]byte, 1024*1024)

	return &DataReader{
		reader: reader,
		l:      l,
		buf:    buf,
	}, nil
}

func (d *DataReader) ReadAt(size, offset uint64) ([]byte, error) {
	buf := d.buf[0:size]
	_, err := d.reader.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

type DataStreamReader struct {
	reader *mmap.ReaderAt
	l      int64
	off    int64
	roff   int
	cnt    int
	buf    []byte
	ksbuf  []byte
	vsbuf  []byte
	kbuf   []byte
}

func NewDataStreamReader() (*DataStreamReader, error) {
	reader, err := mmap.Open("/tmp/org.data")
	if err != nil {
		return &DataStreamReader{}, err
	}

	l := int64(reader.Len())
	off := int64(0)
	roff := 0
	buf := make([]byte, 1024*1024)
	ksbuf := make([]byte, 4)
	vsbuf := make([]byte, 8)
	kbuf := make([]byte, 1024)
	cnt := 0

	return &DataStreamReader{
		reader: reader,
		l:      l,
		off:    off,
		roff:   roff,
		cnt:    cnt,
		buf:    buf,
		ksbuf:  ksbuf,
		vsbuf:  vsbuf,
		kbuf:   kbuf,
	}, nil
}

func (d *DataStreamReader) ReadKeySize() (uint32, error) {
	start := 0
	end := 4
	if d.roff+end > 1024*1024 {
		koff := copy(d.ksbuf, d.buf[d.roff:])
		start += koff
		end -= koff
		d.roff += koff
	}
	if d.roff == 0 || d.roff == 1024*1024 {
		n, err := d.read()
		if n == 0 {
			return uint32(0), errors.New("EOF")
		}
		if err != nil {
			return uint32(0), err
		}
	}
	copy(d.ksbuf[start:], d.buf[d.roff:d.roff+end])
	d.roff += end

	keySize := binary.BigEndian.Uint32(d.ksbuf)
	return keySize, nil
}

func (d *DataStreamReader) ReadValueSize() (uint64, error) {
	start := 0
	end := 8
	if d.roff+end > 1024*1024 {
		koff := copy(d.vsbuf, d.buf[d.roff:])
		start += koff
		end -= koff
		d.roff += koff
	}
	if d.roff == 0 || d.roff == 1024*1024 {
		n, err := d.read()
		if n == 0 {
			return uint64(0), errors.New("EOF")
		}
		if err != nil {
			return uint64(0), err
		}
	}
	copy(d.vsbuf[start:], d.buf[d.roff:d.roff+end])
	d.roff += end

	valueSize := binary.BigEndian.Uint64(d.vsbuf)
	return valueSize, nil
}

func (d *DataStreamReader) ReadKey(keySize uint32) ([]byte, error) {
	ks := int(keySize)
	start := 0
	end := int(keySize)
	if d.roff+end > 1024*1024 {
		koff := copy(d.kbuf, d.buf[d.roff:])
		start += koff
		end -= koff
		d.roff += koff
	}
	if d.roff == 0 || d.roff == 1024*1024 {
		n, err := d.read()
		if n == 0 {
			return nil, errors.New("EOF")
		}
		if err != nil {
			return nil, err
		}
	}
	copy(d.kbuf[start:], d.buf[d.roff:d.roff+end])
	d.roff += end

	rbuf := d.kbuf[0:ks]
	return rbuf, nil
}

func (d *DataStreamReader) Skip(valueSize uint64) error {
	vs := int(valueSize)
	if d.roff+vs > 1024*1024 {
		m := 1024*1024 - d.roff
		n, err := d.read()
		if n == 0 {
			return errors.New("EOF")
		}
		if err != nil {
			return err
		}
		d.roff = vs - m
	} else {
		d.roff += vs
	}

	return nil
}

func (d *DataStreamReader) GetOffset() uint64 {
	return uint64(d.off) + uint64(d.roff)
}

func (d *DataStreamReader) read() (int, error) {
	if d.off >= d.l {
		return 0, errors.New("EOF")
	}

	n, err := d.reader.ReadAt(d.buf, d.off)
	if err != nil {
		return 0, err
	}
	d.off += int64(n)
	d.roff = 0
	d.cnt++
	return n, nil
}
