package logmux

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/technoweenie/grohl"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/syslog/rfc5424"
	"github.com/flynn/flynn/pkg/syslog/rfc6587"
)

type LogMux struct {
	logc chan *rfc5424.Message

	producerwg *sync.WaitGroup

	shutdowno sync.Once
	shutdownc chan struct{}

	doneo sync.Once
	donec chan struct{}
}

func New(bufferSize int) *LogMux {
	return &LogMux{
		logc:       make(chan *rfc5424.Message, bufferSize),
		producerwg: &sync.WaitGroup{},
		shutdownc:  make(chan struct{}),
		donec:      make(chan struct{}),
	}
}

func (m *LogMux) Run(discd *discoverd.Client) error {
	conn, err := dialService(discd)
	if err != nil {
		return err
	}

	go m.drain(discd, conn)
	return nil
}

var drainHook func()

func (m *LogMux) drain(discd *discoverd.Client, conn net.Conn) {
	g := grohl.NewContext(grohl.Data{"at": "logmux_drain"})
	eventc := make(chan *discoverd.Event)

	defer close(m.donec)

	if drainHook != nil {
		drainHook()
	}

	var err error
	for {
		select {
		case msg, ok := <-m.logc:
			if !ok {
				if err := conn.Close(); err != nil {
					g.Log(grohl.Data{"status": "error", "err": err.Error()})
				}
				return
			}

			if _, err = conn.Write(rfc6587.Bytes(msg)); err != nil {
				g.Log(grohl.Data{"status": "error", "err": err.Error()})
			}
		case event := <-eventc:
			g.Log(grohl.Data{"event": event.Kind.String()})

			switch event.Kind {
			case discoverd.EventKindLeader:
				if conn.Close(); err != nil {
					g.Log(grohl.Data{"status": "error", "err": err.Error()})
				}

				if conn, err = dialService(discd); err != nil {
					// TODO(benburkert): recover from dial failure
					panic(err)
				}
			default:
				// TODO(benburkert): handle service up/down transition
			}
		}
	}
}

func dialService(discd *discoverd.Client) (net.Conn, error) {
	srv := discd.Service("logaggregator")
	ldr, err := srv.Leader()
	if err != nil {
		return nil, err
	}

	return net.Dial("tcp", ldr.Addr)
}

// Close blocks until all producers have finished, then terminates the drainer,
// and blocks until the backlog in logc has been processed.
func (m *LogMux) Close() {
	m.producerwg.Wait()

	m.doneo.Do(func() { close(m.logc) })
	<-m.donec
}

type Config struct {
	AppName, IP, JobType, JobID string
}

// Follow forwards log lines from the reader into the syslog client. Follow
// runs until the reader is closed or an error occurs. If an error occurs, the
// reader may still be open.
func (m *LogMux) Follow(r io.Reader, fd int, config Config) {
	m.producerwg.Add(1)

	if config.AppName == "" {
		config.AppName = config.JobID
	}

	hdr := &rfc5424.Header{
		Hostname: []byte(config.IP),
		AppName:  []byte(config.AppName),
		ProcID:   []byte(config.JobType + "." + config.JobID),
		MsgID:    []byte(fmt.Sprintf("ID%d", fd)),
	}

	go m.follow(r, hdr)
}

func (m *LogMux) follow(r io.Reader, hdr *rfc5424.Header) {
	defer m.producerwg.Done()

	g := grohl.NewContext(grohl.Data{"at": "logmux_follow"})
	bufr := bufio.NewReader(r)

	for {
		line, _, err := bufr.ReadLine()
		if err == io.EOF {
			return
		}
		if err != nil {
			g.Log(grohl.Data{"status": "error", "err": err.Error()})
			return
		}

		msg := rfc5424.NewMessage(hdr, line)

		select {
		case m.logc <- msg:
		default:
			// throw away msg if logc buffer is full
		}
	}
}
