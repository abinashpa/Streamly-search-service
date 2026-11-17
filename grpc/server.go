package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"streamly/search-service/config"
	"streamly/search-service/opensearch"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	osClient *opensearch.Client
	cfg      config.Config
}

func NewServer(osClient *opensearch.Client, cfg config.Config) *Server {
	return &Server{osClient: osClient, cfg: cfg}
}

func (s *Server) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()

	serviceDesc := grpc.ServiceDesc{
		ServiceName: "search.SearchService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Search",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct {
						Query     string
						MediaType string
						Page      int
						Limit     int
					})
					if err := dec(in); err != nil {
						return nil, err
					}
					page := in.Page
					if page <= 0 {
						page = 1
					}
					limit := in.Limit
					if limit <= 0 || limit > 50 {
						limit = 20
					}

					res, err := s.osClient.Search(ctx, in.Query, in.MediaType, page, limit)
					if err != nil {
						return nil, err
					}

					items := make([]opensearch.MediaDocument, len(res.Hits.Hits))
					for i, hit := range res.Hits.Hits {
						items[i] = hit.Source
					}

					return map[string]interface{}{
						"total": res.Hits.Total.Value,
						"took":  res.Took,
						"page":  page,
						"limit": limit,
						"items": items,
					}, nil
				},
			},
			{
				MethodName: "Suggest",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct {
						Query string
						Limit int
					})
					if err := dec(in); err != nil {
						return nil, err
					}
					limit := in.Limit
					if limit <= 0 {
						limit = 6
					}
					suggestions, err := s.osClient.Suggest(ctx, in.Query, limit)
					if err != nil {
						return map[string]interface{}{"suggestions": []string{}}, nil
					}
					return map[string]interface{}{"suggestions": suggestions}, nil
				},
			},
			{
				MethodName: "IndexMedia",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct {
						Document opensearch.MediaDocument
					})
					if err := dec(in); err != nil {
						return nil, err
					}
					err := s.osClient.IndexDocument(ctx, in.Document)
					if err != nil {
						return nil, err
					}
					return map[string]interface{}{"success": true, "id": in.Document.ID}, nil
				},
			},
			{
				MethodName: "DeleteIndex",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct{ ID string })
					if err := dec(in); err != nil {
						return nil, err
					}
					err := s.osClient.DeleteDocument(ctx, in.ID)
					if err != nil {
						return nil, err
					}
					return map[string]interface{}{"success": true, "id": in.ID}, nil
				},
			},
		},
	}

	grpcServer.RegisterService(&serviceDesc, s)

	log.Printf("search-service gRPC server listening on port %s", port)
	return grpcServer.Serve(lis)
}

// FetchMediaListViaGRPC dials media-service on its gRPC port to pull all media
func FetchMediaListViaGRPC(mediaServiceAddr string) ([]opensearch.MediaDocument, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, mediaServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial media-service gRPC: %v", err)
	}
	defer conn.Close()

	var out struct {
		Items []opensearch.MediaDocument
		Count int
	}
	err = conn.Invoke(ctx, "/media.MediaService/ListMedia", map[string]interface{}{"limit": 100}, &out)
	if err != nil {
		return nil, err
	}
	return out.Items, nil
}
