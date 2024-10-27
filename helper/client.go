package helper

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Client interface {
	Open(config *Config) error
	send() error
	Close()
}

type ClientImpl struct {
	conn net.Conn
}

func NewClient() Client {
	return &ClientImpl{}
}

func (c *ClientImpl) Open(config *Config) error {
	index := 0
	if config.SegID >= 0 {
		index = config.SegID
	}

	var hostAddress string
	var netConn net.Conn
	var err error

	times := 1
	for times < 5 {
		if strings.Contains(config.Hosts[index], ":") {
			hostAddress = "[" + config.Hosts[index] + "]"
		} else {
			hostAddress = config.Hosts[index]
		}

		netConn, err = net.DialTimeout("tcp", hostAddress+":"+config.Ports[index], 32*time.Second)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
		times++
	}
	if err != nil {
		return fmt.Errorf("failed to connect to server %v: %v", hostAddress+":"+config.Ports[index], err)
	}

	gplog.Debug("Successfully established connection to %v:%v", hostAddress, config.Ports[index])

	if !config.NoCompression {
		c.conn, _ = NewCompressConn(netConn, getCompressionType(config.TransCompType), false)
	} else {
		c.conn = netConn
	}

	return nil
}

func (c *ClientImpl) send() error {
	return utils.RedirectStream(os.Stdin, c.conn)
}

func (c *ClientImpl) Close() {
}
