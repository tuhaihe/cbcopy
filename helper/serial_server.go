package helper

import (
	"compress/gzip"
	"io"
	"net"
	"sync"

	"github.com/golang/snappy"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type SerialServer struct {
	ServerBase
	writer         io.Writer
	workerSessions chan *Session
	workerQuit     chan interface{}
	workerWg       sync.WaitGroup
}

func (s *SerialServer) Stop() {
	close(s.workerQuit)
	s.workerWg.Wait()
	gplog.Debug("SerialServer worker task stopped")

	close(s.quit)
	_ = s.listener.Close()
	s.wg.Wait()
	gplog.Debug("SerialServer server task stopped")
}

func (s *SerialServer) write(record interface{}) error {
	_, err := s.writer.Write(record.([]byte))
	return err
}

func (s *SerialServer) createReader(conn net.Conn) (interface{}, error) {
	if !s.isCompress {
		return conn, nil
	}

	if s.compressType == CompressSnappy {
		return snappy.NewReader(conn), nil
	}

	r, err := gzip.NewReader(conn)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *SerialServer) sessionMain(session *Session) {
	s.workerSessions <- session
}

func (s *SerialServer) workerMain() {
	defer s.workerWg.Done()

	for {
		select {
		case <-s.workerQuit:
			return
		case session := <-s.workerSessions:
			session.redirectStream()
			if session.Error() != nil {
				return
			}

			if s.isDone() {
				return
			}
		}
	}
}

func NewSerialServer(numConn int32, writer io.Writer, config *Config) Server {
	gplog.Debug("Creating SerialServer...")

	s := &SerialServer{ServerBase: ServerBase{
		config:       config,
		quit:         make(chan interface{}),
		numConn:      numConn,
		numFinished:  0,
		isCompress:   !config.NoCompression,
		compressType: getCompressionType(config.TransCompType)},
		writer:         writer,
		workerSessions: make(chan *Session, numConn),
		workerQuit:     make(chan interface{})}

	s.server = s
	s.wg.Add(1)
	s.workerWg.Add(1)

	go s.workerMain()
	return s
}
