package helper

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"net"
	"os"
	"runtime/debug"

	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

var (
	metaFilePath  string
	wasTerminated bool
	ErrCode       int
)

type Helper struct {
	config *Config
}

func NewHelper(cfg *Config) *Helper {
	return &Helper{
		config: cfg,
	}
}

func (h *Helper) handleResolve() error {
	addr, err := net.LookupIP(h.config.ToResolve)
	if err != nil {
		return fmt.Errorf("failed to resolve %s: %v", h.config.ToResolve, err)
	}

	if h.config.SegID >= 0 {
		fmt.Printf("%d\t%s\n", h.config.SegID, addr[0].String())
	} else {
		fmt.Println(addr[0].String())
	}

	return nil
}

func (h *Helper) handleMD5XOR() error {
	hash := md5.New()
	scanner := bufio.NewScanner(os.Stdin)
	xor := make([]byte, 16) // 16 Bytes, 128 Bits

	for scanner.Scan() {
		hash.Write(scanner.Bytes())
		sum := hash.Sum(nil)
		hash.Reset()

		for id, val := range sum {
			xor[id] = val ^ xor[id]
		}
	}

	_, err := fmt.Fprintf(os.Stdout, "%x\n", xor)

	return err
}

func (h *Helper) handleListen() error {
	server := createServerImpl(h.config)

	port := server.Start()
	if server.Err() != nil {
		return server.Err()
	}

	err := h.writeFile(h.config.CmdID, h.config.SegID, port)
	if err != nil {
		return err
	}

	server.Serve()

	server.WaitForFinished()

	return server.Err()
}

func (h *Helper) handleClient() error {
	client := NewClient()

	if err := client.Open(h.config); err != nil {
		return err
	}
	defer client.Close()

	return client.send()
}

func (h *Helper) writeFile(cmdID string, segID int, port int) error {
	metaFilePath = fmt.Sprintf("/tmp/cbcopy-%s-%d.txt", h.config.CmdID, h.config.SegID)

	metaFile, err := os.Create(metaFilePath)
	if err != nil {
		return err
	}

	defer func(metaFile *os.File) {
		_ = metaFile.Close()
	}(metaFile)

	_, err = metaFile.WriteString(fmt.Sprintf("%s\t%d\t%d\n", cmdID, segID, port))

	return err
}

func (h *Helper) cleanup() {

}

func (h *Helper) Run() error {
	defer h.cleanup()

	shouldContinue, err := h.config.Parse()
	if err != nil {
		return err
	}

	if !shouldContinue {
		return nil
	}

	switch {
	case h.config.ToResolve != "":
		return h.handleResolve()
	case h.config.MD5XORMode:
		return h.handleMD5XOR()
	case h.config.ListenMode:
		return h.handleListen()
	default:
		return h.handleClient()
	}
}

func Start() {
	gplog.InitializeLogging("cbcopy_helper", "")

	// Don't print non fatal messages to stdout
	gplog.SetVerbosity(gplog.LOGERROR)

	utils.InitializeSignalHandler(Cleanup, "cbcopy_helper process", &wasTerminated)

	gplog.Debug("DoHelper enter with args %v", os.Args)

	helper := NewHelper(NewConfig())
	if err := helper.Run(); err != nil {
		gplog.FatalOnError(err)
	}

	gplog.Debug("DoHelper quit")
}

func Stop() {
	gplog.Debug("Helper stop")

	if err := recover(); err != nil {
		gplog.Error("%v", err)
		gplog.Error("%s", debug.Stack())
	}

	ErrCode = gplog.GetErrorCode()
	Cleanup(false)
}

func Cleanup(isTerminated bool) {

	if len(metaFilePath) > 0 {
		_ = os.Remove(metaFilePath)
	}
}
