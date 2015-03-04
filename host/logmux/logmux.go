package logmux

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/technoweenie/grohl"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/stream"
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

func (m *LogMux) Run(discd *discoverd.Client) {
	go m.drainTo(m.serviceConn(discd))
}

var drainHook func()

func (m *LogMux) drainTo(conn io.Writer) {
	defer close(m.donec)

	g := grohl.NewContext(grohl.Data{"at": "logmux_drain"})

	if drainHook != nil {
		drainHook()
	}

	for {
		msg, ok := <-m.logc
		if !ok {
			// shutdown
			return
		}

		_, err := conn.Write(rfc6587.Bytes(msg))
		if err != nil {
			g.Log(grohl.Data{"status": "error", "err": err.Error()})
			break
		}
	}
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

func (m *LogMux) serviceConn(discd *discoverd.Client) io.Writer {
	srv := discd.Service("logaggregator")
	cc := &condConn{
		cond: &sync.Cond{L: &sync.Mutex{}},
	}

	go watchService(srv, cc, m.donec)

	return cc
}

func watchService(srv discoverd.Service, cc *condConn, donec <-chan struct{}) {
	g := grohl.NewContext(grohl.Data{"at": "logmux_service_watch"})
	eventc := make(chan *discoverd.Event)

	var (
		err    error
		stream stream.Stream
	)

	if stream, err = srv.Watch(eventc); err != nil {
		panic(err)
	}

	for {
		select {
		case event, ok := <-eventc:
			if !ok {
				// TODO(benburkert): watch closed, rewatch??
				return
			}

			switch event.Kind {
			case discoverd.EventKindLeader:
				if conn := cc.Reset(); conn != nil {
					if err = conn.Close(); err != nil {
						g.Log(grohl.Data{"status": "error", "err": err.Error()})
					}
				}

				ldr, err := srv.Leader()
				if err != nil {
					panic(err)
					g.Log(grohl.Data{"status": "error", "err": err.Error()})
					break
				}

				conn, err := net.Dial("tcp", ldr.Addr)
				if err != nil {
					g.Log(grohl.Data{"status": "error", "err": err.Error()})
					break
				}

				cc.Set(conn)
			case discoverd.EventKindDown:
				if conn := cc.Reset(); conn != nil {
					if err = conn.Close(); err != nil {
						g.Log(grohl.Data{"status": "error", "err": err.Error()})
					}
				}
			default:
			}
		case <-donec:
			if err = stream.Close(); err != nil {
				g.Log(grohl.Data{"status": "error", "err": err.Error()})
			}

			if conn := cc.Reset(); conn != nil {
				if err = conn.Close(); err != nil {
					g.Log(grohl.Data{"status": "error", "err": err.Error()})
				}
			}
			return
		}
	}
}

type condConn struct {
	net.Conn
	cond *sync.Cond
}

func (c *condConn) Reset() net.Conn {
	c.cond.L.Lock()
	conn := c.Conn
	c.Conn = nil
	c.cond.L.Unlock()

	return conn
}

func (c *condConn) Set(conn net.Conn) {
	c.cond.L.Lock()
	c.Conn = conn
	c.cond.L.Unlock()
	c.cond.Signal()
}

func (c *condConn) Write(p []byte) (int, error) {
	c.cond.L.Lock()
	for c.Conn == nil {
		c.cond.Wait()
	}

	defer c.cond.L.Unlock()
	return c.Conn.Write(p)
}
