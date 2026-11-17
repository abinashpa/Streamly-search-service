package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"streamly/search-service/config"
)

type Client struct {
	cfg        config.Config
	httpClient *http.Client
}

type MediaDocument struct {
	ID           string  `json:"id"`
	Title        string  `json:"title"`
	Description  string  `json:"description"`
	MediaType    string  `json:"media_type"`
	Artist       string  `json:"artist,omitempty"`
	Duration     float64 `json:"duration"`
	StreamURL    string  `json:"stream_url"`
	ThumbnailURL string  `json:"thumbnail_url"`
	UploaderID   string  `json:"uploader_id"`
	UploaderName string  `json:"uploader_name"`
	Tags         string  `json:"tags"`
	CreatedAt    string  `json:"created_at"`
}

func NewClient(cfg config.Config) *Client {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	cli := &Client{
		cfg:        cfg,
		httpClient: httpClient,
	}

	go cli.waitForReadyAndInitIndex()
	return cli
}

func (c *Client) sendRequest(ctx context.Context, method, endpoint string, body []byte) ([]byte, int, error) {
	url := fmt.Sprintf("%s%s", strings.TrimRight(c.cfg.OpenSearchURL, "/"), endpoint)
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	if c.cfg.OpenSearchUser != "" && c.cfg.OpenSearchPass != "" {
		req.SetBasicAuth(c.cfg.OpenSearchUser, c.cfg.OpenSearchPass)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, err
}

func (c *Client) waitForReadyAndInitIndex() {
	log.Printf("Connecting to OpenSearch at %s...", c.cfg.OpenSearchURL)
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, code, err := c.sendRequest(ctx, "GET", "/", nil)
		cancel()

		if err == nil && code == 200 {
			log.Println("OpenSearch cluster is ready!")
			_ = c.createIndexIfNotExists()
			return
		}
		time.Sleep(3 * time.Second)
	}
	log.Println("Warning: OpenSearch did not become ready within timeout. Will retry dynamically.")
}

func (c *Client) createIndexIfNotExists() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, code, _ := c.sendRequest(ctx, "HEAD", "/"+c.cfg.IndexName, nil)
	if code == 200 {
		log.Printf("OpenSearch index '%s' already exists", c.cfg.IndexName)
		return nil
	}

	// Create index with autocomplete analyzer
	indexConfig := map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"autocomplete_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 1,
						"max_gram": 20,
					},
				},
				"analyzer": map[string]interface{}{
					"autocomplete": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"filter": []string{
							"lowercase",
							"autocomplete_filter",
						},
					},
				},
			},
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id": map[string]interface{}{"type": "keyword"},
				"title": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"autocomplete": map[string]interface{}{
							"type":     "text",
							"analyzer": "autocomplete",
						},
						"keyword": map[string]interface{}{
							"type": "keyword",
						},
					},
				},
				"description": map[string]interface{}{"type": "text"},
				"media_type":  map[string]interface{}{"type": "keyword"},
				"artist": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"autocomplete": map[string]interface{}{
							"type":     "text",
							"analyzer": "autocomplete",
						},
						"keyword": map[string]interface{}{
							"type": "keyword",
						},
					},
				},
				"tags":          map[string]interface{}{"type": "text"},
				"uploader_id":   map[string]interface{}{"type": "keyword"},
				"uploader_name": map[string]interface{}{"type": "text"},
				"duration":      map[string]interface{}{"type": "float"},
				"stream_url":    map[string]interface{}{"type": "keyword"},
				"thumbnail_url": map[string]interface{}{"type": "keyword"},
				"created_at":    map[string]interface{}{"type": "date"},
			},
		},
	}

	body, err := json.Marshal(indexConfig)
	if err != nil {
		return err
	}

	_, statusCode, err := c.sendRequest(ctx, "PUT", "/"+c.cfg.IndexName, body)
	if err != nil || (statusCode != 200 && statusCode != 201) {
		log.Printf("Failed to create index: status %d, err %v", statusCode, err)
		return err
	}

	log.Printf("Successfully created OpenSearch index: %s", c.cfg.IndexName)
	return nil
}

func (c *Client) IndexDocument(ctx context.Context, doc MediaDocument) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("/%s/_doc/%s", c.cfg.IndexName, doc.ID)
	_, code, err := c.sendRequest(ctx, "PUT", endpoint, body)
	if err != nil {
		return err
	}
	if code != 200 && code != 201 {
		return fmt.Errorf("unexpected status code: %d", code)
	}
	return nil
}

func (c *Client) DeleteDocument(ctx context.Context, id string) error {
	endpoint := fmt.Sprintf("/%s/_doc/%s", c.cfg.IndexName, id)
	_, _, err := c.sendRequest(ctx, "DELETE", endpoint, nil)
	return err
}

type SearchResponse struct {
	Took int `json:"took"`
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			ID     string        `json:"_id"`
			Score  float64       `json:"_score"`
			Source MediaDocument `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (c *Client) Search(ctx context.Context, queryStr, mediaType string, page, limit int) (*SearchResponse, error) {
	from := (page - 1) * limit

	var mustClauses []map[string]interface{}
	var filterClauses []map[string]interface{}

	if strings.TrimSpace(queryStr) != "" {
		mustClauses = append(mustClauses, map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query": queryStr,
				"fields": []string{
					"title^3",
					"artist^2",
					"tags^2",
					"description",
					"uploader_name",
				},
				"fuzziness": "AUTO",
			},
		})
	} else {
		mustClauses = append(mustClauses, map[string]interface{}{
			"match_all": map[string]interface{}{},
		})
	}

	if mediaType != "" && mediaType != "all" {
		filterClauses = append(filterClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"media_type": mediaType,
			},
		})
	}

	searchQuery := map[string]interface{}{
		"from": from,
		"size": limit,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must":   mustClauses,
				"filter": filterClauses,
			},
		},
		"sort": []interface{}{
			map[string]interface{}{"_score": map[string]interface{}{"order": "desc"}},
			map[string]interface{}{"created_at": map[string]interface{}{"order": "desc"}},
		},
	}

	body, err := json.Marshal(searchQuery)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("/%s/_search", c.cfg.IndexName)
	respBody, code, err := c.sendRequest(ctx, "POST", endpoint, body)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("search error code: %d, body: %s", code, string(respBody))
	}

	var searchResp SearchResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return nil, err
	}

	return &searchResp, nil
}

func (c *Client) Suggest(ctx context.Context, prefix string, limit int) ([]string, error) {
	if strings.TrimSpace(prefix) == "" {
		return []string{}, nil
	}

	suggestQuery := map[string]interface{}{
		"size": limit,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"title.autocomplete": prefix,
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"artist.autocomplete": prefix,
						},
					},
				},
			},
		},
		"_source": []string{"title", "artist"},
	}

	body, err := json.Marshal(suggestQuery)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("/%s/_search", c.cfg.IndexName)
	respBody, code, err := c.sendRequest(ctx, "POST", endpoint, body)
	if err != nil || code != 200 {
		return []string{}, err
	}

	var searchResp SearchResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return []string{}, err
	}

	var suggestions []string
	seen := make(map[string]bool)
	for _, hit := range searchResp.Hits.Hits {
		if hit.Source.Title != "" && !seen[hit.Source.Title] {
			seen[hit.Source.Title] = true
			suggestions = append(suggestions, hit.Source.Title)
		}
		if hit.Source.Artist != "" && !seen[hit.Source.Artist] {
			seen[hit.Source.Artist] = true
			suggestions = append(suggestions, hit.Source.Artist)
		}
	}

	return suggestions, nil
}
