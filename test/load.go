package main

import (
	"time"
	"sync"
	"math/rand"
	"net/http"
	"sync/atomic"
	"fmt"
	"log"
	"io/ioutil"
	"math"
	"gopkg.in/alecthomas/kingpin.v2"
	"encoding/json"
)


type loadResults struct {
	MinimumBatchTime float64
	MaximumBatchTime float64
	AverageBatchTime float64
	TotalTime        float64
	Batches          int
	BatchSize        int
	Concurrency      int
	Errors           int32
	Collisions       int32
}


func startLoadTesting(concurrency int, number int) (*loadResults, error) {
	results := &loadResults{}
	results.Concurrency = concurrency
	results.Batches = int(math.Ceil(float64(number) / float64(concurrency)))
	results.BatchSize = number / results.Batches

	err := load(results)
	return results, err
}

func load(results *loadResults) error {
	start := time.Now()
	for batch := 0; batch < results.Batches; batch++ {
		wg := sync.WaitGroup{}
		wg.Add(results.BatchSize)
		batchStart := time.Now()

		for batchItem := 0; batchItem < results.BatchSize; batchItem++ {
			go func() {
				defer wg.Done()

				key := randStringBytesMaskImprSrc(22)
				reqUri := fmt.Sprintf("%s?key=%s&duration=3600s", *uri, key)
				req, err := http.NewRequest("POST", reqUri, nil)
				if err != nil {
					log.Printf("Error from new request %v", err)
					return
				}
				req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					log.Printf("Error from request %v", err)
					atomic.AddInt32(&results.Errors, 1)
					return
				}
				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 403 {
					log.Printf("StatusCode from request %v", resp.StatusCode)
					atomic.AddInt32(&results.Collisions, 1)
				} else if resp.StatusCode >= 300 {
					log.Printf("StatusCode from request %v", resp.StatusCode)
					atomic.AddInt32(&results.Errors, 1)
				}
			}()
		}

		wg.Wait()
		batchDuration := time.Now().Sub(batchStart)
		if results.MinimumBatchTime == 0 || batchDuration.Seconds() < results.MinimumBatchTime {
			results.MinimumBatchTime = batchDuration.Seconds()
		}
		if results.MaximumBatchTime == 0 || batchDuration.Seconds() > results.MaximumBatchTime {
			results.MaximumBatchTime = batchDuration.Seconds()
		}
	}
	results.TotalTime = time.Now().Sub(start).Seconds()
	results.AverageBatchTime = results.TotalTime / float64(results.Batches)
	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var randSource = rand.NewSource(time.Now().UnixNano())
var randLocker = sync.RWMutex{}

func randStringBytesMaskImprSrc(n int) string {
	randLocker.Lock()
	defer randLocker.Unlock()
	b := make([]byte, n)
	for i, cache, remain := n-1, randSource.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = randSource.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

var (
	number = kingpin.Flag("n", "Number to insert").Default("10000").Int()
	concurrency = kingpin.Flag("c", "Concurrency").Default("50").Int()
	uri = kingpin.Flag("uri", "Uri to endpoint").Default("http://127.0.0.1:11111/cns").String()
)

func main() {
	kingpin.Parse()
	results, _ := startLoadTesting(*concurrency, *number)
	output, _ := json.MarshalIndent(results, "", "\t")
	fmt.Println(string(output))

}