package reader

import (
	"errors"
	"io"
)

const minReadBufferSize = 16
const maxConsecutiveEmptyReads = 100

const (
	EolNl = iota
	EolCr
	EolCrNl
	EolCustom
)

var (
	ErrBufferFull    = errors.New("reader: buffer full")
	ErrNegativeCount = errors.New("reader: negative count")
	errNegativeRead  = errors.New("reader: returned negative count from Read")
)

type VirtualMethod interface {
	parseRecords(s []byte) int
}

type Reader interface {
	Read() ([]byte, error)
	Count() int
}

type ReaderBase struct {
	virm      VirtualMethod
	buf       []byte
	rd        io.Reader
	r, w      int // buf read and write positions
	err       error
	line      int
	rawBuffer []byte
}

func (b *ReaderBase) fill() {
	// Slide existing data to beginning.
	if b.r > 0 {
		copy(b.buf, b.buf[b.r:b.w])
		b.w -= b.r
		b.r = 0
	}

	if b.w >= len(b.buf) {
		panic("bufio: tried to fill full buffer")
	}

	// Read new data: try a limited number of times.
	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		n, err := b.rd.Read(b.buf[b.w:])
		if n < 0 {
			panic(errNegativeRead)
		}
		b.w += n
		if err != nil {
			b.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	b.err = io.ErrNoProgress
}

func (b *ReaderBase) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// Buffered returns the number of bytes that can be read from the current buffer.
func (b *ReaderBase) buffered() int { return b.w - b.r }

func (b *ReaderBase) readRecord() (record []byte, err error) {
	s := 0 // search start index
	for {
		// Search buffer.
		if i := b.virm.parseRecords(b.buf[b.r+s : b.w]); i >= 0 {
			i += s
			record = b.buf[b.r : b.r+i+1]
			b.r += i + 1
			break
		}

		// Pending error?
		if b.err != nil {
			record = b.buf[b.r:b.w]
			b.r = b.w
			err = b.readErr()
			break
		}

		// Buffer full?
		if b.buffered() >= len(b.buf) {
			b.r = b.w
			record = b.buf
			err = ErrBufferFull
			break
		}

		s = 0

		b.fill() // buffer is not full
	}

	return
}

func (b *ReaderBase) Read() ([]byte, error) {
	record, err := b.readRecord()

	if err == ErrBufferFull {
		b.rawBuffer = append(b.rawBuffer[:0], record...)
		for err == ErrBufferFull {
			record, err = b.readRecord()
			b.rawBuffer = append(b.rawBuffer, record...)
		}
		record = b.rawBuffer
	}

	return record, err
}

func (b *ReaderBase) Count() int {
	return b.line
}
