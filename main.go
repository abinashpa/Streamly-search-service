package main

import (
	"context"
	"log"

	"streamly/search-service/config"
	searchgrpc "streamly/search-service/grpc"
	searchkafka "streamly/search-service/kafka"
	"streamly/search-service/opensearch"
	"streamly/search-service/routes"
)

func main() {
	cfg := config.LoadConfig()
	osClient := opensearch.NewClient(cfg)

	// Start Kafka consumer in background
	consumer := searchkafka.NewConsumer(cfg.KafkaBrokers, osClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer consumer.Close()
	go consumer.Start(ctx)

	// Start gRPC Server in background
	grpcServer := searchgrpc.NewServer(osClient, cfg)
	go func() {
		if err := grpcServer.Start(cfg.GRPCPort); err != nil {
			log.Printf("search-service gRPC server error: %v", err)
		}
	}()

	// Start Gin HTTP Server
	r := routes.SetupRouter(osClient, cfg)

	log.Printf("search-service HTTP running on port %s, gRPC on port %s", cfg.Port, cfg.GRPCPort)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
