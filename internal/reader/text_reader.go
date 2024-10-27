package reader

import (
	"io"
)

type TextReader struct {
	ReaderBase
	EolType int
}

func NewTextReader(rd io.Reader, size int, eolType int) *TextReader {
	if size < minReadBufferSize {
		size = minReadBufferSize
	}

	r := &TextReader{ReaderBase: ReaderBase{buf: make([]byte, size), rd: rd}, EolType: eolType}
	r.virm = r

	return r
}

// reads records from r
func (b *TextReader) parseRecords(s []byte) int {
	switch b.EolType {
	case EolNl:
		return b.parseNlOrCr(s, '\n')
	case EolCr:
		return b.parseNlOrCr(s, '\r')
	default:
		return b.parseCrNl(s)
	}
}

/* find the last end of line delimiter from the back */
func (b *TextReader) parseNlOrCr(s []byte, delim byte) int {
	lastRecord := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == delim {
			lastRecord = i
			break
		}
	}

	return lastRecord
}

func (b *TextReader) parseCrNl(s []byte) int {
	lastRecord := -1
	for i := len(s) - 1; i >= 0; i-- {
		if i > 0 && s[i] == '\n' && s[i-1] == '\r' {
			lastRecord = i
			break
		}
	}

	return lastRecord
}
