package reader

import (
	"bytes"
	"io"
)

type CustomReader struct {
	ReaderBase
	lineDelimiter []byte
}

func NewCustomReader(rd io.Reader, size int, lineDelimiter string) *CustomReader {
	if size < minReadBufferSize {
		size = minReadBufferSize
	}

	delim := ""

	if lineDelimiter == `E'\n'` {
		delim = "\n"
	} else if lineDelimiter == `E'\r'` {
		delim = "\r"
	} else if lineDelimiter == `E'\r\n'` {
		delim = "\r\n"
	} else {
		delim = lineDelimiter
	}

	r := &CustomReader{ReaderBase: ReaderBase{buf: make([]byte, size), rd: rd}, lineDelimiter: []byte(delim)}
	r.virm = r

	return r
}

/* find the last end of line delimiter from the back */
func (b *CustomReader) parseRecords(s []byte) int {
	lastRecord := -1

	slen := len(s)
	dlen := len(b.lineDelimiter)

	if slen < dlen {
		return lastRecord
	}

	i := slen - dlen
	if i == 0 {
		eq := bytes.Equal(s, b.lineDelimiter)
		if !eq {
			return lastRecord
		}

		lastRecord = slen - 1
		return lastRecord
	}

	for i >= 0 {
		eq := bytes.Equal(s[i:i+dlen], b.lineDelimiter)
		if eq {
			lastRecord = i + dlen - 1
			return lastRecord
		}

		i--
	}

	return lastRecord
}
