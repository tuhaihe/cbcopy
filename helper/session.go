package helper

import (
	"io"
	"net"

	"github.com/cloudberrydb/cbcopy/internal/reader"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Session struct {
	conn   net.Conn
	server Server
	err    error
}

func (s *Session) processRequest() {
	defer s.close()

	gplog.Debug("ConcurrentServer accept a connection from %v", s.conn.RemoteAddr())

	r, err := s.server.createReader(s.conn)
	if err != nil {
		s.err = err
		return
	}

	cr := r.(*reader.CsvReader)

	for {
		record, er := cr.Read()
		if len(record) > 0 {
			ew := s.server.write(record)
			if ew != nil {
				err = ew
				break
			}
		}

		if er == io.EOF {
			break
		}

		if er != nil {
			err = er
			break
		}
	}

	if err != nil {
		s.err = err
	}
}

func (s *Session) redirectStream() {
	defer s.close()

	gplog.Debug("SerialServer accept a connection from %v", s.conn.RemoteAddr())

	r, err := s.server.createReader(s.conn)
	if err != nil {
		s.err = err
		return
	}

	ir := r.(io.Reader)
	buf := make([]byte, 32*1024)

	for {
		nr, er := ir.Read(buf)
		if nr > 0 {
			ew := s.server.write(buf[0:nr])
			if ew != nil {
				err = ew
				break
			}
		}

		if er == io.EOF {
			break
		}

		if er != nil {
			err = er
			break
		}
	}

	if err != nil {
		s.err = err
	}
}

func (s *Session) Error() error {
	return s.err
}

func (s *Session) close() {
	gplog.Debug("Connection from %v is closed.", s.conn.RemoteAddr())

	_ = s.conn.Close()

	if s.err != nil {
		s.server.setError(s.err)
	}

	s.server.increase()
}
