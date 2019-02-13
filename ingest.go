package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Unomaly Ingest library
type Ingest struct {
	command  chan command
	shutdown chan struct{}
	options  Options
	batch    []*UnomalyEvent
}

type command func(in *Ingest) bool

// Event expected by the Unomaly HTTP API
type UnomalyEvent struct {
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
}

var DefaultOptions = Options{
	BatchSize:        100,
	FlushInterval:    time.Second,
	PendingQueueSize: 10000,
	APIPath:          "/v1/batch",
}

var total = 0

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

// Initialise the Unomaly ingest library. The Unomaly host is required.
func Init(UnomalyEndpoint string, options ...Option) *Ingest {

	opts := DefaultOptions
	for _, opt := range options {
		opt(&opts)
	}
	opts.UnomalyHost = UnomalyEndpoint

	ingest := &Ingest{
		command:  make(chan command, opts.PendingQueueSize),
		shutdown: make(chan struct{}),
		options:  opts,
		batch:    make([]*UnomalyEvent, 0, opts.BatchSize),
	}
	go ingest.Work()

	return ingest
}

func (in *Ingest) Send(ev *UnomalyEvent) {
	in.command <- func(in *Ingest) bool {
		in.add(ev)
		return false
	}
}

func (in *Ingest) add(ev *UnomalyEvent) {
	in.batch = append(in.batch, ev)
	total++
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

// Marhsal and send current batch to Unomaly
func (in *Ingest) sendBatch() error {
	data, err := json.Marshal(in.batch)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %s", err)
	}

	resp, err := http.Post(in.options.UnomalyHost+in.options.APIPath, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	//fmt.Println(resp.StatusCode)
	err = resp.Body.Close()

	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %s", err)
	}
	return err
}

// Flush current batch
func (in *Ingest) flush() {
	if len(in.batch) != 0 {
		err := in.sendBatch()
		in.batch = make([]*UnomalyEvent, 0, in.options.BatchSize)
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
	for {
		select {
		case cmd := <-in.command:
			if stop := cmd(in); stop == true {
				return
			}
		case <-time.Tick(in.options.FlushInterval):
			in.flush()
		}
	}
}
