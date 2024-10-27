package helper

import (
	"compress/gzip"
	"io"
	"net"

	"github.com/pkg/errors"

	"github.com/golang/snappy"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// CompressType is the compression format
type CompressType int

const (
	// CompressSnappy is the Snappy compression format
	CompressSnappy = iota
	CompressGzip
)

// CompressConn is the wrapper of net.Conn, with compression
type CompressConn struct {
	net.Conn
	reader      io.Reader
	writeCloser io.WriteCloser // Snappy has an extra closer
	plainCloser io.Closer
}

func (conn *CompressConn) Read(bytes []byte) (int, error) {
	return conn.reader.Read(bytes)
}

func (conn *CompressConn) Write(bytes []byte) (int, error) {
	return conn.writeCloser.Write(bytes)
}

// Close calls snappy.Writer and net.Conn's Close() methods
func (conn *CompressConn) Close() error {
	var err1 error
	var err2 error

	// call compression writer's own Close()
	err1 = conn.writeCloser.Close()
	// close net.Conn
	err2 = conn.plainCloser.Close()

	// return the first level error
	// if snappy and net both have errors, return snappy's
	if err1 != nil {
		return err1
	}

	return err2
}

// NewCompressConn construct a CompressConn
func NewCompressConn(conn net.Conn, compressType CompressType, isReader bool) (net.Conn, error) {
	var err error
	compressConn := &CompressConn{Conn: conn}
	plainReader := io.Reader(compressConn.Conn)
	plainWriteCloser := io.WriteCloser(compressConn.Conn)
	plainCloser := io.Closer(compressConn.Conn)

	switch compressType {
	case CompressSnappy:
		compressConn.reader = snappy.NewReader(plainReader)
		compressConn.writeCloser = snappy.NewBufferedWriter(plainWriteCloser)
	case CompressGzip:
		if isReader {
			gplog.Debug("Creating gzip reader")
			compressConn.reader, err = gzip.NewReader(plainReader)
			gplog.Debug("Finished creating gzip reader")
		} else {
			compressConn.writeCloser, err = gzip.NewWriterLevel(plainWriteCloser, gzip.DefaultCompression)
		}

		if err != nil {
			return nil, err
		}
	default:
		gplog.Fatal(errors.Errorf("No recognized compression type %v", compressType), "")
	}
	compressConn.plainCloser = plainCloser

	return compressConn, nil
}

func getCompressionType(compType string) CompressType {
	var result CompressType = CompressGzip

	if compType == "snappy" {
		result = CompressSnappy
	}

	return result
}
