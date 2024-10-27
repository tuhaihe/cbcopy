package reader

import (
	"io"
)

type CsvReader struct {
	ReaderBase
	EolType int
	Escape  byte
	Quote   byte
}

func NewCsvReader(rd io.Reader, size int, escape byte, quote byte, eolType int) *CsvReader {
	if size < minReadBufferSize {
		size = minReadBufferSize
	}

	r := &CsvReader{ReaderBase: ReaderBase{buf: make([]byte, size), rd: rd}, EolType: eolType, Escape: escape, Quote: quote}
	r.virm = r

	return r
}

// reads records from r
func (b *CsvReader) parseRecords(s []byte) int {
	switch b.EolType {
	case EolNl:
		return b.parseNlOrCr(s, '\n')
	case EolCr:
		return b.parseNlOrCr(s, '\r')
	default:
		return b.parseCrNl(s)
	}
}

func (b *CsvReader) parseNlOrCr(s []byte, delim byte) int {
	inQuote := false
	lastWasEsc := false
	lastRecord := -1

	for i, c := range s {
		if inQuote {
			if lastWasEsc {
				lastWasEsc = false
			} else {
				if c == b.Quote {
					inQuote = false
				} else if c == b.Escape {
					lastWasEsc = true
				}
			}
		} else if c == b.Quote {
			inQuote = true
		} else if c == delim {
			b.line++
			lastRecord = i
		}
	}

	return lastRecord
}

func (b *CsvReader) parseCrNl(s []byte) int {
	inQuote := false
	lastWasEsc := false
	lastRecord := -1
	lastch := byte(0)

	for i, c := range s {
		if inQuote {
			if lastWasEsc {
				lastWasEsc = false
			} else {
				if c == b.Quote {
					inQuote = false
				} else if c == b.Escape {
					lastWasEsc = true
				}
			}
		} else if c == b.Quote {
			inQuote = true
		} else if c == '\n' && lastch == '\r' {
			b.line++
			lastRecord = i
		}

		lastch = c
	}

	return lastRecord
}
