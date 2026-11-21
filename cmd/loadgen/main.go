package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type BenchSummary struct {
	Timestamp   string  `json:"timestamp"`
	Concurrency int     `json:"concurrency"`
	Duration    string  `json:"duration"`
	ReadRatio   float64 `json:"read_ratio"`
	TotalOps    int64   `json:"total_ops"`
	Errors      int64   `json:"errors"`
	Throughput  float64 `json:"throughput_ops_sec"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
}

func main() {
	coordAddr := flag.String("addr", "http://127.0.0.1:8080", "Coordinator HTTP address")
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	durationSec := flag.Int("duration", 10, "Test duration in seconds")
	readRatio := flag.Float64("read-ratio", 0.8, "Ratio of reads vs writes (0.0 - 1.0)")
	keySpace := flag.Int("key-space", 5000, "Number of unique keys in benchmark")
	valSize := flag.Int("value-size", 128, "Byte size of generated values")
	outputJSON := flag.String("output-json", "bench-results.json", "Path to save JSON summary")
	outputMD := flag.String("output-md", "bench-results.md", "Path to append Markdown table row")
	flag.Parse()

	fmt.Printf("Starting miniKV loadgen: %d workers, %ds duration, %.0f%% reads, target: %s\n",
		*concurrency, *durationSec, *readRatio*100, *coordAddr)

	payloadVal := string(bytes.Repeat([]byte("X"), *valSize))
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
		},
	}

	var totalOps atomic.Int64
	var errCount atomic.Int64
	latenciesMu := sync.Mutex{}
	latencies := make([]float64, 0, 100000)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	startTime := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-stop:
					return
				default:
				}

				key := fmt.Sprintf("bench:key:%d", r.Intn(*keySpace))
				isRead := r.Float64() < *readRatio

				opStart := time.Now()
				var err error

				if isRead {
					url := fmt.Sprintf("%s/api/v1/kv/%s", *coordAddr, key)
					resp, reqErr := client.Get(url)
					if reqErr != nil {
						err = reqErr
					} else {
						resp.Body.Close()
					}
				} else {
					url := fmt.Sprintf("%s/api/v1/kv/%s", *coordAddr, key)
					bodyBytes, _ := json.Marshal(map[string]interface{}{
						"value": payloadVal,
					})
					req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(bodyBytes))
					req.Header.Set("Content-Type", "application/json")
					resp, reqErr := client.Do(req)
					if reqErr != nil {
						err = reqErr
					} else {
						resp.Body.Close()
					}
				}

				durMs := float64(time.Since(opStart).Microseconds()) / 1000.0

				totalOps.Add(1)
				if err != nil {
					errCount.Add(1)
				}

				// Sample 1 out of 5 for latency collection to avoid lock overhead
				if r.Intn(5) == 0 {
					latenciesMu.Lock()
					latencies = append(latencies, durMs)
					latenciesMu.Unlock()
				}
			}
		}(w)
	}

	time.Sleep(time.Duration(*durationSec) * time.Second)
	close(stop)
	wg.Wait()
	elapsed := time.Since(startTime).Seconds()

	ops := totalOps.Load()
	errs := errCount.Load()
	throughput := float64(ops) / elapsed

	sort.Float64s(latencies)
	p50, p95, p99 := 0.0, 0.0, 0.0
	if len(latencies) > 0 {
		p50 = latencies[int(float64(len(latencies))*0.50)]
		p95 = latencies[int(float64(len(latencies))*0.95)]
		p99 = latencies[int(float64(len(latencies))*0.99)]
	}

	summary := BenchSummary{
		Timestamp:   time.Now().Format("2006-01-02 15:04:05"),
		Concurrency: *concurrency,
		Duration:    fmt.Sprintf("%ds", *durationSec),
		ReadRatio:   *readRatio,
		TotalOps:    ops,
		Errors:      errs,
		Throughput:  throughput,
		P50Ms:       p50,
		P95Ms:       p95,
		P99Ms:       p99,
	}

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Elapsed:     %.2fs\n", elapsed)
	fmt.Printf("Total Ops:   %d (Errors: %d)\n", ops, errs)
	fmt.Printf("Throughput:  %.2f ops/sec\n", throughput)
	fmt.Printf("Latency p50: %.3f ms\n", p50)
	fmt.Printf("Latency p95: %.3f ms\n", p95)
	fmt.Printf("Latency p99: %.3f ms\n", p99)

	// Save JSON
	if *outputJSON != "" {
		b, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(*outputJSON, b, 0644)
		fmt.Printf("Wrote JSON to %s\n", *outputJSON)
	}

	// Save Markdown
	if *outputMD != "" {
		row := fmt.Sprintf("| %s | %d | %.0f%% | %.1f | %.2f | %.2f | %.2f | %d |\n",
			summary.Duration, summary.Concurrency, summary.ReadRatio*100,
			summary.Throughput, summary.P50Ms, summary.P95Ms, summary.P99Ms, summary.Errors)
		f, err := os.OpenFile(*outputMD, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			f.WriteString(row)
			f.Close()
		}
	}
}
