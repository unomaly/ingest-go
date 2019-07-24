package ingest

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Unomaly Ingest library
type Ingest struct {
	command  chan command
	shutdown chan struct{}
	options  Options
	batch    []*Event
	client   *http.Client
}

type command func(in *Ingest) bool

// Event expected by the Unomaly HTTP API
type Event struct {
	Message   string                 `json:"message"`
	Source    string                 `json:"source"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata"`
}

type Options struct {
	BatchSize        int
	FlushInterval    time.Duration
	PendingQueueSize int
	UnomalyHost      string
	APIPath          string
	SkipTLSVerify    bool
	Gzip             bool
}

var DefaultOptions = Options{
	BatchSize:        100,
	FlushInterval:    time.Second,
	PendingQueueSize: 10000,
	APIPath:          "/v1/batch",
	Gzip:             false,
}

type Option func(*Options)

// Api on the unomaly instance
func APIPath(path string) Option {
	return func(options *Options) {
		options.APIPath = path
	}
}

// Batch size. Increasing it can lead to better performance but also increase memory requirements
func BatchSize(size int) Option {
	return func(options *Options) {
		options.BatchSize = size
	}
}

// Flush interval. Default is one second
func FlushInterval(duration time.Duration) Option {
	return func(options *Options) {
		options.FlushInterval = duration
	}
}

// Skip SSL certificate verification, if you are using a self signed certificate.
func SkipTLSVerify() Option {
	return func(options *Options) {
		options.SkipTLSVerify = true
	}
}

// Compress payload using gzip
func Gzip() Option {
	return func(options *Options) {
		options.Gzip = true
	}
}

// Initialise the Unomaly ingest library. The Unomaly host is required.
func Init(UnomalyEndpoint string, options ...Option) *Ingest {

	opts := DefaultOptions
	for _, opt := range options {
		opt(&opts)
	}
	opts.UnomalyHost = UnomalyEndpoint

	if !strings.HasPrefix(UnomalyEndpoint, "http") {
		opts.UnomalyHost = "https://" + UnomalyEndpoint
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: opts.SkipTLSVerify}}

	ingest := &Ingest{
		command:  make(chan command, opts.PendingQueueSize),
		shutdown: make(chan struct{}),
		options:  opts,
		batch:    make([]*Event, 0, opts.BatchSize),
		client:   &http.Client{Transport: tr},
	}
	go ingest.Work()

	return ingest
}

func (in *Ingest) Send(ev *Event) {
	in.command <- func(in *Ingest) bool {
		in.add(ev)
		return false
	}
}

func (in *Ingest) add(ev *Event) {
	in.batch = append(in.batch, ev)
	if len(in.batch) == in.options.BatchSize {
		in.flush()
	}
}

// Flush the current batch
func (in *Ingest) Flush() error {
	errorCh := make(chan error, 1)
	in.command <- func(in *Ingest) bool {
		in.flush()
		errorCh <- nil
		return false
	}

	<-errorCh

	return nil
}

// Marshal and send current batch to Unomaly
func (in *Ingest) sendBatch() error {
	data, err := json.Marshal(in.batch)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %s", err)
	}

	var buf bytes.Buffer
	if in.options.Gzip {
		g := gzip.NewWriter(&buf)
		_, err = g.Write(data)
		if err != nil {
			return err
		}

		err = g.Close()
		if err != nil {
			return err
		}
	} else {
		buf.Write(data)
	}

	req, err := http.NewRequest("POST", in.options.UnomalyHost+in.options.APIPath, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := in.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %s", err)
	}
	return resp.Body.Close()
}

// Flush current batch
func (in *Ingest) flush() {
	if len(in.batch) != 0 {
		err := in.sendBatch()
		in.batch = make([]*Event, 0, in.options.BatchSize)
		if err != nil {
			fmt.Printf("Failed to send batch : %s\n", err)
		}
	}
}

// Flush events in the pipe and stop the event loop
func (in *Ingest) Close() error {
	done := make(chan struct{})
	in.command <- func(in *Ingest) bool {
		go func() {
			in.command <- func(in *Ingest) bool {
				close(in.command)
				return false
			}
		}()
		for cmd := range in.command {
			cmd(in)
		}
		in.flush()
		done <- struct{}{}
		return true
	}
	<-done
	return nil
}

// Main loop
func (in *Ingest) Work() {
	ticker := time.Tick(in.options.FlushInterval)
	for {
		select {
		case cmd := <-in.command:
			if stop := cmd(in); stop == true {
				return
			}
		case <-ticker:
			in.flush()
		}
	}
}
