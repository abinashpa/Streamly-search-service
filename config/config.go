package config

import (
	"os"
)

type Config struct {
	Port               string
	OpenSearchURL      string
	OpenSearchUser     string
	OpenSearchPass     string
	MediaServiceURL    string
	MediaServiceGRPCAddr string
	CatalogServiceURL  string
	IndexName            string
	KafkaBrokers         string
	GRPCPort             string
}

func GetEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

func LoadConfig() Config {
	return Config{
		Port:                 GetEnv("PORT", "8083"),
		OpenSearchURL:        GetEnv("OPENSEARCH_URL", "http://opensearch:9200"),
		OpenSearchUser:       GetEnv("OPENSEARCH_USER", "admin"),
		OpenSearchPass:       GetEnv("OPENSEARCH_PASSWORD", "admin"),
		MediaServiceURL:      GetEnv("MEDIA_SERVICE_URL", "http://media-service:8081"),
		MediaServiceGRPCAddr: GetEnv("MEDIA_SERVICE_GRPC_ADDR", "media-service:50051"),
		CatalogServiceURL:    GetEnv("CATALOG_SERVICE_URL", "http://catalog-service:8082"),
		IndexName:            GetEnv("INDEX_NAME", "streamly_media"),
		KafkaBrokers:         GetEnv("KAFKA_BROKERS", "kafka:9092"),
		GRPCPort:             GetEnv("GRPC_PORT", "50053"),
	}
}
