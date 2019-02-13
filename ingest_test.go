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

func TestIngestBatch(t *testing.T) {

	t.Run("Nonimal operations", func(t *testing.T) {

		totalEvents := 1001
		batchSize := 2
		allEvents := make([]*Event, 0)

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, _ := ioutil.ReadAll(r.Body)
			var batch []*Event
			err := json.Unmarshal(data, &batch)
			//fmt.Println(batch)
			allEvents = append(allEvents, batch...)
			assert.NoError(t, err)

			//fmt.Printf("%s\n", data)
			w.WriteHeader(200)
			w.Write([]byte("hello"))
		}))

		ingest := Init(
			s.URL,
			APIPath("/batch"),
			FlushInterval(time.Millisecond),
			BatchSize(batchSize))

		for i := 0; i < totalEvents; i++ {
			ev := &Event{
				Message:   fmt.Sprintf("Hello, World %d!", i),
				Source:    "Test",
				Timestamp: time.Now(),
				Metadata:  nil,
			}
			ingest.Send(ev)
		}
		err := ingest.Close()
		s.Close()
		assert.NoError(t, err)

		assert.Equal(t, totalEvents, len(allEvents))

	})
}
