package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"streamly/search-service/config"
	"streamly/search-service/opensearch"

	"github.com/gin-gonic/gin"
)

type SearchController struct {
	OS  *opensearch.Client
	Cfg config.Config
}

func NewSearchController(osClient *opensearch.Client, cfg config.Config) *SearchController {
	return &SearchController{OS: osClient, Cfg: cfg}
}

func (sc *SearchController) Search(c *gin.Context) {
	query := c.Query("q")
	mediaType := c.Query("type")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit > 50 {
		limit = 50
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := sc.OS.Search(ctx, query, mediaType, page, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Search error: %v", err)})
		return
	}

	items := make([]opensearch.MediaDocument, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		items = append(items, hit.Source)
	}

	c.JSON(http.StatusOK, gin.H{
		"total": res.Hits.Total.Value,
		"took":  res.Took,
		"page":  page,
		"limit": limit,
		"items": items,
	})
}

func (sc *SearchController) Suggest(c *gin.Context) {
	prefix := c.Query("q")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "6"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	suggestions, err := sc.OS.Suggest(ctx, prefix, limit)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"suggestions": []string{}})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"suggestions": suggestions,
	})
}

func (sc *SearchController) IndexDocument(c *gin.Context) {
	var doc opensearch.MediaDocument
	if err := c.ShouldBindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if doc.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Document ID is required"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sc.OS.IndexDocument(ctx, doc); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to index: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Document indexed successfully", "id": doc.ID})
}

func (sc *SearchController) DeleteDocument(c *gin.Context) {
	id := c.Param("id")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sc.OS.DeleteDocument(ctx, id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete document: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Document deleted from index", "id": id})
}

func (sc *SearchController) Reindex(c *gin.Context) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/media", sc.Cfg.MediaServiceURL))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to reach media-service"})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var mediaResponse struct {
		Items []struct {
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
		} `json:"items"`
	}

	if err := json.Unmarshal(body, &mediaResponse); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse media response"})
		return
	}

	indexedCount := 0
	ctx := context.Background()
	for _, m := range mediaResponse.Items {
		doc := opensearch.MediaDocument{
			ID:           m.ID,
			Title:        m.Title,
			Description:  m.Description,
			MediaType:    m.MediaType,
			Artist:       m.Artist,
			Duration:     m.Duration,
			StreamURL:    m.StreamURL,
			ThumbnailURL: m.ThumbnailURL,
			UploaderID:   m.UploaderID,
			UploaderName: m.UploaderName,
			Tags:         m.Tags,
			CreatedAt:    m.CreatedAt.Format(time.RFC3339),
		}
		if err := sc.OS.IndexDocument(ctx, doc); err == nil {
			indexedCount++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Reindexing complete",
		"indexed": indexedCount,
	})
}
