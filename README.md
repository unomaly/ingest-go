# Unomaly ingest-go
Go library to send your logs to Unomaly, https://unomaly.com

```go
 
// Initialize the library
ingest := ingest.Init("my.unomaly.host")

// Flush all data to Unomaly before exiting
defer ingest.Close()

// Add an event to the queue. The library will take care of batching and sending events to Unomaly
ingest.send(&ingest.Event{
    				Message:   "Hello, World!",
    				Source:    "My-system",
    				Timestamp: time.Now(),
    				Metadata:  nil,
})

```
