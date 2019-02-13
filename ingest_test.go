package ingest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIngest(t *testing.T) {

	ingest := Init("http://localhost:6010", APIPath("/batch"), FlushInterval(time.Millisecond))
	ev := &UnomalyEvent{
		Message:   "Hello, World!",
		Source:    "Test",
		Timestamp: time.Unix(15909945, 0),
		Metadata:  nil,
	}

	err := ingest.Send(ev)
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, err)
}

func TestIngestBatch(t *testing.T) {

	t.Run("Nonimal operations", func(t *testing.T) {

		totalEvents := 1001
		batchSize := 2
		allEvents := make([]*UnomalyEvent, 0)

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, _ := ioutil.ReadAll(r.Body)
			var batch []*UnomalyEvent
			err := json.Unmarshal(data, &batch)
			fmt.Println(batch)
			allEvents = append(allEvents, batch...)
			assert.NoError(t, err)

			fmt.Printf("%s\n", data)
			w.WriteHeader(200)
			w.Write([]byte("hello"))
		}))

		ingest := Init(
			s.URL,
			APIPath("/batch"),
			FlushInterval(time.Millisecond),
			BatchSize(batchSize))

		for i := 0; i < totalEvents; i++ {
			ev := &UnomalyEvent{
				Message:   fmt.Sprintf("Hello, World %d!", i),
				Source:    "Test",
				Timestamp: time.Now(),
				Metadata:  nil,
			}
			err := ingest.Send(ev)
			assert.NoError(t, err)
		}
		err := ingest.Shutdown()
		s.Close()
		assert.NoError(t, err)

		assert.Equal(t, totalEvents, len(allEvents))

	})

	t.Run("Error in batch", func(t *testing.T) {

		totalEvents := 1001
		batchSize := 2
		allEvents := make([]*UnomalyEvent, 0)

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, _ := ioutil.ReadAll(r.Body)
			var batch []*UnomalyEvent
			err := json.Unmarshal(data, &batch)
			//fmt.Println(batch)
			allEvents = append(allEvents, batch...)
			assert.NoError(t, err)

			//fmt.Printf("%s\n", data)
			w.WriteHeader(400)
			w.Write([]byte("hello"))
		}))

		ingest := Init(
			s.URL,
			APIPath("/batch"),
			FlushInterval(time.Millisecond),
			BatchSize(batchSize))

		for i := 0; i < totalEvents; i++ {
			ev := &UnomalyEvent{
				Message:   fmt.Sprintf("Hello, World %d!", i),
				Source:    "Test",
				Timestamp: time.Now(),
				Metadata:  nil,
			}
			err := ingest.Send(ev)
			assert.NoError(t, err)
		}
		err := ingest.Shutdown()
		s.Close()
		assert.NoError(t, err)

		assert.Equal(t, totalEvents, len(allEvents))

	})

}

type TestRoundTripper struct {
	req     *http.Request
	reqBody string
	resp    *http.Response
	respErr error
}

func (t *TestRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	data, _ := ioutil.ReadAll(r.Body)

	fmt.Printf("%s\n", data)
	t.req = r
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	t.reqBody = string(bodyBytes)
	return t.resp, t.respErr
}
