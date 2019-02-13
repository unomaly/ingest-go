# ingest-go
Go library to send your events to Unomaly, https://unomaly.com


The API is not stable yet, please contact our support before including it in your project.


```go


// Initialize the library
ingest := ingest.Init("https://my-host")

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
