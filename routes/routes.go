package routes

import (
	"streamly/search-service/config"
	"streamly/search-service/controllers"
	"streamly/search-service/opensearch"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func SetupRouter(osClient *opensearch.Client, cfg config.Config) *gin.Engine {
	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "search-service"})
	})

	searchCtrl := controllers.NewSearchController(osClient, cfg)

	api := r.Group("/api/v1/search")
	{
		api.GET("", searchCtrl.Search)
		api.GET("/suggest", searchCtrl.Suggest)
		api.POST("/index", searchCtrl.IndexDocument)
		api.DELETE("/index/:id", searchCtrl.DeleteDocument)
		api.POST("/reindex", searchCtrl.Reindex)
	}

	return r
}
