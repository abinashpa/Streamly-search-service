package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"streamly/search-service/opensearch"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	osClient *opensearch.Client
}

type MediaUploadedEvent struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	MediaType    string    `json:"media_type"`
	Artist       string    `json:"artist"`
	Duration     float64   `json:"duration"`
	StreamURL    string    `json:"stream_url"`
	ThumbnailURL string    `json:"thumbnail_url"`
	UploaderID   string    `json:"uploader_id"`
	UploaderName string    `json:"uploader_name"`
	Tags         string    `json:"tags"`
	CreatedAt    time.Time `json:"created_at"`
}

func NewConsumer(brokers string, osClient *opensearch.Client) *Consumer {
	brokerList := strings.Split(brokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          "media.uploaded",
		GroupID:        "search-service-indexer",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	log.Printf("Kafka consumer initialized for topic 'media.uploaded' (group: search-service-indexer)")
	return &Consumer{reader: reader, osClient: osClient}
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Kafka consumer listening for media upload events to index into OpenSearch...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer stopped")
			return
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Retry on broker temporary unavailability during startup
				time.Sleep(2 * time.Second)
				continue
			}

			var event MediaUploadedEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal media.uploaded event: %v", err)
				continue
			}

			doc := opensearch.MediaDocument{
				ID:           event.ID,
				Title:        event.Title,
				Description:  event.Description,
				MediaType:    event.MediaType,
				Artist:       event.Artist,
				Duration:     event.Duration,
				StreamURL:    event.StreamURL,
				ThumbnailURL: event.ThumbnailURL,
				UploaderID:   event.UploaderID,
				UploaderName: event.UploaderName,
				Tags:         event.Tags,
				CreatedAt:    event.CreatedAt.Format(time.RFC3339),
			}

			if err := c.osClient.IndexDocument(ctx, doc); err != nil {
				log.Printf("Kafka consumer: failed to index document %s into OpenSearch: %v", event.ID, err)
			} else {
				log.Printf("Kafka consumer: successfully indexed media %s (%s) into OpenSearch", event.ID, event.Title)
			}
		}
	}
}

func (c *Consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
