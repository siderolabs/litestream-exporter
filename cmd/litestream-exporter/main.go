package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Compaction level names for human-readable labels.
// Level 9 is the snapshot level in litestream.
var levelNames = map[int]string{
	0: "L0 (raw)",
	1: "L1 (30s)",
	2: "L2 (5min)",
	3: "L3 (hourly)",
	9: "L9 (snapshot)",
}

// LevelStats holds statistics for a single compaction level.
type LevelStats struct {
	FileCount  int
	TotalBytes int64
}

// LTXStats holds aggregated LTX file statistics.
type LTXStats struct {
	ByLevel    map[int]*LevelStats
	TotalBytes int64
	TotalFiles int
}

// NewLTXStats creates a new LTXStats instance.
func NewLTXStats() *LTXStats {
	return &LTXStats{
		ByLevel: make(map[int]*LevelStats),
	}
}

// LitestreamExporter collects and exposes Litestream metrics.
type LitestreamExporter struct {
	s3Client *s3.Client
	logger   *slog.Logger

	// Prometheus metrics - Local LTX
	localLTXFilesByLevel *prometheus.GaugeVec
	localLTXBytesByLevel *prometheus.GaugeVec

	// Prometheus metrics - Remote LTX (S3/R2)
	remoteLTXFilesByLevel *prometheus.GaugeVec
	remoteLTXBytesByLevel *prometheus.GaugeVec

	// Scrape metadata
	scrapeErrors *prometheus.GaugeVec

	localLTXFiles         prometheus.Gauge
	localLTXBytes         prometheus.Gauge
	remoteLTXFiles        prometheus.Gauge
	remoteLTXBytes        prometheus.Gauge
	lastScrapeTimestamp   prometheus.Gauge
	scrapeDurationSeconds prometheus.Gauge

	localLTXDir string
	s3Bucket    string
	s3Prefix    string
}

// NewLitestreamExporter creates a new LitestreamExporter instance.
func NewLitestreamExporter(
	localLTXDir string,
	s3Bucket string,
	s3Prefix string,
	s3EndpointURL string,
	awsAccessKeyID string,
	awsSecretAccessKey string,
	logger *slog.Logger,
) (*LitestreamExporter, error) {
	exporter := &LitestreamExporter{
		localLTXDir: localLTXDir,
		s3Bucket:    s3Bucket,
		s3Prefix:    strings.TrimSuffix(s3Prefix, "/"),
		logger:      logger,
	}

	// Initialize S3 client for R2/S3-compatible storage
	if s3Bucket != "" && awsAccessKeyID != "" && awsSecretAccessKey != "" {
		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")),
			config.WithRegion("auto"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}

		exporter.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			if s3EndpointURL != "" {
				o.BaseEndpoint = aws.String(s3EndpointURL)
			}
		})
	}

	// Initialize Prometheus metrics - Local LTX
	exporter.localLTXFiles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_local_ltx_files",
		Help: "Total number of LTX files in local metadata directory",
	})
	exporter.localLTXBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_local_ltx_bytes",
		Help: "Total size of LTX files in local metadata directory in bytes",
	})
	exporter.localLTXFilesByLevel = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ops_litestream_local_ltx_files_by_level",
		Help: "Number of LTX files by compaction level in local directory",
	}, []string{"level", "level_name"})
	exporter.localLTXBytesByLevel = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ops_litestream_local_ltx_bytes_by_level",
		Help: "Size of LTX files by compaction level in local directory in bytes",
	}, []string{"level", "level_name"})

	// Initialize Prometheus metrics - Remote LTX (S3/R2)
	exporter.remoteLTXFiles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_remote_ltx_files",
		Help: "Total number of LTX files in remote replica (S3/R2)",
	})
	exporter.remoteLTXBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_remote_ltx_bytes",
		Help: "Total size of LTX files in remote replica (S3/R2) in bytes",
	})
	exporter.remoteLTXFilesByLevel = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ops_litestream_remote_ltx_files_by_level",
		Help: "Number of LTX files by compaction level in remote replica",
	}, []string{"level", "level_name"})
	exporter.remoteLTXBytesByLevel = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ops_litestream_remote_ltx_bytes_by_level",
		Help: "Size of LTX files by compaction level in remote replica in bytes",
	}, []string{"level", "level_name"})

	// Scrape metadata
	exporter.lastScrapeTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_last_scrape_timestamp_seconds",
		Help: "Unix timestamp of the last successful metrics scrape",
	})
	exporter.scrapeDurationSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ops_litestream_scrape_duration_seconds",
		Help: "Duration of the last metrics scrape in seconds",
	})
	exporter.scrapeErrors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ops_litestream_scrape_errors",
		Help: "Total number of scrape errors",
	}, []string{"source"})

	// Register all metrics
	prometheus.MustRegister(
		exporter.localLTXFiles,
		exporter.localLTXBytes,
		exporter.localLTXFilesByLevel,
		exporter.localLTXBytesByLevel,
		exporter.remoteLTXFiles,
		exporter.remoteLTXBytes,
		exporter.remoteLTXFilesByLevel,
		exporter.remoteLTXBytesByLevel,
		exporter.lastScrapeTimestamp,
		exporter.scrapeDurationSeconds,
		exporter.scrapeErrors,
	)

	return exporter, nil
}

// parseLevelFromPath extracts compaction level from LTX file path.
// Local paths: ltx/0/..., ltx/1/..., ltx/9/...
// Remote paths: prefix/0000/..., prefix/0001/..., prefix/0009/...
// Returns -1 if level cannot be determined.
func parseLevelFromPath(path string) int {
	re := regexp.MustCompile(`/(\d{1,4})/`)

	matches := re.FindStringSubmatch(path)
	if len(matches) > 1 {
		level, err := strconv.Atoi(matches[1])
		if err == nil {
			return level
		}
	}

	return -1
}

// getLevelName returns human-readable name for compaction level.
func getLevelName(level int) string {
	if name, ok := levelNames[level]; ok {
		return name
	}

	return fmt.Sprintf("L%d (unknown)", level)
}

// collectLocalStats collects statistics from local LTX directory.
func (e *LitestreamExporter) collectLocalStats() *LTXStats {
	stats := NewLTXStats()

	if e.localLTXDir == "" {
		e.logger.Warn("Local LTX directory not configured")

		return stats
	}

	info, err := os.Stat(e.localLTXDir)
	if err != nil || !info.IsDir() {
		e.logger.Warn("Local LTX directory not found", "path", e.localLTXDir)

		return stats
	}

	err = filepath.Walk(e.localLTXDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			e.logger.Warn("Error accessing file", "path", path, "error", err)

			return nil
		}

		if info.IsDir() || !strings.HasSuffix(path, ".ltx") {
			return nil
		}

		fileSize := info.Size()
		level := parseLevelFromPath(path)

		stats.TotalFiles++
		stats.TotalBytes += fileSize

		if level >= 0 {
			if stats.ByLevel[level] == nil {
				stats.ByLevel[level] = &LevelStats{}
			}

			stats.ByLevel[level].FileCount++
			stats.ByLevel[level].TotalBytes += fileSize
		}

		return nil
	})
	if err != nil {
		e.logger.Error("Error walking local directory", "error", err)
	}

	return stats
}

// collectRemoteStats collects statistics from remote S3/R2 bucket.
func (e *LitestreamExporter) collectRemoteStats(ctx context.Context) (*LTXStats, error) {
	stats := NewLTXStats()

	if e.s3Client == nil {
		e.logger.Warn("S3 client not configured, skipping remote stats")

		return stats, nil
	}

	prefix := ""
	if e.s3Prefix != "" {
		prefix = e.s3Prefix + "/"
	}

	paginator := s3.NewListObjectsV2Paginator(e.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(e.s3Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return stats, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".ltx") {
				continue
			}

			fileSize := aws.ToInt64(obj.Size)
			level := parseLevelFromPath(key)

			stats.TotalFiles++
			stats.TotalBytes += fileSize

			if level >= 0 {
				if stats.ByLevel[level] == nil {
					stats.ByLevel[level] = &LevelStats{}
				}

				stats.ByLevel[level].FileCount++
				stats.ByLevel[level].TotalBytes += fileSize
			}
		}
	}

	return stats, nil
}

// UpdateMetrics collects stats and updates Prometheus metrics.
func (e *LitestreamExporter) UpdateMetrics(ctx context.Context) {
	startTime := time.Now()

	// Collect local stats
	localStats := e.collectLocalStats()
	e.localLTXFiles.Set(float64(localStats.TotalFiles))
	e.localLTXBytes.Set(float64(localStats.TotalBytes))

	// Reset level metrics before updating (to handle removed levels)
	for level := range levelNames {
		levelName := getLevelName(level)
		e.localLTXFilesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(0)
		e.localLTXBytesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(0)
	}

	for level, levelStats := range localStats.ByLevel {
		levelName := getLevelName(level)
		e.localLTXFilesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(float64(levelStats.FileCount))
		e.localLTXBytesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(float64(levelStats.TotalBytes))
	}

	e.logger.Info("Local LTX stats collected",
		"files", localStats.TotalFiles,
		"bytes_mb", fmt.Sprintf("%.2f", float64(localStats.TotalBytes)/1024/1024),
	)

	// Collect remote stats
	remoteStats, err := e.collectRemoteStats(ctx)
	if err != nil {
		e.logger.Error("Error collecting remote stats", "error", err)
		e.scrapeErrors.WithLabelValues("remote").Inc()
	} else {
		e.remoteLTXFiles.Set(float64(remoteStats.TotalFiles))
		e.remoteLTXBytes.Set(float64(remoteStats.TotalBytes))

		// Reset level metrics before updating
		for level := range levelNames {
			levelName := getLevelName(level)
			e.remoteLTXFilesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(0)
			e.remoteLTXBytesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(0)
		}

		for level, levelStats := range remoteStats.ByLevel {
			levelName := getLevelName(level)
			e.remoteLTXFilesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(float64(levelStats.FileCount))
			e.remoteLTXBytesByLevel.WithLabelValues(strconv.Itoa(level), levelName).Set(float64(levelStats.TotalBytes))
		}

		e.logger.Info("Remote LTX stats collected",
			"files", remoteStats.TotalFiles,
			"bytes_mb", fmt.Sprintf("%.2f", float64(remoteStats.TotalBytes)/1024/1024),
		)
	}

	duration := time.Since(startTime).Seconds()

	e.lastScrapeTimestamp.Set(float64(time.Now().Unix()))
	e.scrapeDurationSeconds.Set(duration)
	e.logger.Info("Metrics update completed", "duration_seconds", fmt.Sprintf("%.2f", duration))
}

// runCollector periodically updates metrics.
func runCollector(ctx context.Context, exporter *LitestreamExporter, interval time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			exporter.UpdateMetrics(ctx)
		}
	}
}

func getEnv(key string) string {
	return os.Getenv(key)
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}

	return defaultValue
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Configuration from environment variables
	localLTXDir := getEnv("LOCAL_LTX_DIR")
	s3Bucket := getEnv("S3_BUCKET")
	s3Prefix := getEnv("S3_PREFIX")
	s3EndpointURL := getEnv("S3_ENDPOINT_URL")
	awsAccessKeyID := getEnv("LITESTREAM_ACCESS_KEY_ID")
	awsSecretAccessKey := getEnv("LITESTREAM_SECRET_ACCESS_KEY")
	metricsPort := getEnvInt("METRICS_PORT", 9090)
	scrapeInterval := getEnvInt("SCRAPE_INTERVAL_SECONDS", 60)

	logger.Info("Starting Litestream Exporter")
	logger.Info("Configuration",
		"local_ltx_dir", orDefault(localLTXDir, "(not configured)"),
		"s3_bucket", orDefault(s3Bucket, "(not configured)"),
		"s3_prefix", orDefault(s3Prefix, "(none)"),
		"s3_endpoint", orDefault(s3EndpointURL, "(default AWS)"),
		"metrics_port", metricsPort,
		"scrape_interval", fmt.Sprintf("%ds", scrapeInterval),
	)

	exporter, err := NewLitestreamExporter(
		localLTXDir,
		s3Bucket,
		s3Prefix,
		s3EndpointURL,
		awsAccessKeyID,
		awsSecretAccessKey,
		logger,
	)
	if err != nil {
		logger.Error("Failed to create exporter", "error", err)
		os.Exit(1)
	}

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial metrics collection
	exporter.UpdateMetrics(ctx)

	// Start background collector
	var wg sync.WaitGroup

	wg.Add(1)

	go runCollector(ctx, exporter, time.Duration(scrapeInterval)*time.Second, &wg)

	// Start Prometheus HTTP server
	http.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", metricsPort),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Prometheus metrics available", "url", fmt.Sprintf("http://0.0.0.0:%d/metrics", metricsPort))

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down...")
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	wg.Wait()
	logger.Info("Shutdown complete")
}

func orDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}

	return value
}
