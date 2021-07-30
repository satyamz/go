package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	albLogEntryCount               = 29
	albTargetStatusCodeRecordIndex = 9
	albRequestIndex                = 12
)

func isSuccesfulStatusCode(statusCode int) bool {
	return statusCode/100 == 2
}

func newALBLogEntryReader(input io.Reader) *csv.Reader {
	reader := csv.NewReader(input)
	reader.Comma = ' '
	reader.FieldsPerRecord = albLogEntryCount
	reader.ReuseRecord = true
	return reader
}

func getRequestURIFromLog(logReader *csv.Reader) (string, error) {
	records, err := logReader.Read()
	if err != nil {
		return "", err
	}

	statusCodeStr := records[albTargetStatusCodeRecordIndex]
	// discard requests with unknown status code
	if statusCodeStr == "-" {
		return "", nil
	}
	statusCode, err := strconv.Atoi(statusCodeStr)
	if err != nil {
		return "", fmt.Errorf("error parsing target status code %q: %v", statusCodeStr, err)
	}

	// discard unsuccesful requests
	if !isSuccesfulStatusCode(statusCode) {
		return "", nil
	}

	reqStr := records[albRequestIndex]
	reqFields := strings.Split(reqStr, " ")
	if len(reqFields) != 3 {
		return "", fmt.Errorf("error parsing request %q: 3 fields exepcted, found %d", reqStr, len(reqFields))
	}
	method := reqFields[0]

	// discard non-get requests
	if method != http.MethodGet {
		return "", nil
	}

	urlStr := reqFields[1]
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("error parsing url %q: %v", urlStr, err)
	}

	return parsed.RequestURI(), nil
}

func main() {
	workers := flag.Int("workers", 1, "How many parallel workers to use")
	flag.Parse()
	if *workers < 1 {
		log.Fatal("--workers parameter must be > 0")
	}
	if flag.NArg() != 2 {
		log.Fatalf("usage: %s <aws_log_file> <target_host_base_url>", os.Args[0])
	}

	file, err := os.Open(flag.Args()[0])
	if err != nil {
		log.Fatalf("error opening file %q: %v", os.Args[1], err)
	}
	baseURL := flag.Args()[1]
	logReader := newALBLogEntryReader(file)
	urlChan := make(chan string, *workers)
	stop := make(chan struct{})

	group := errgroup.Group{}

	// spawn workers
	for i := 0; i < *workers; i++ {
		group.Go(func() error {
			client := http.Client{}
			for {
				select {
				case <-stop:
					return nil
				case fullURL := <-urlChan:
					start := time.Now()
					resp, err := client.Get(fullURL)
					if err != nil {
						log.Printf("unexpected request error: %v %q", err, fullURL)
					}
					resp.Body.Close()
					if !isSuccesfulStatusCode(resp.StatusCode) {
						log.Printf("unexpected status code: %d %q", resp.StatusCode, fullURL)
					}
					log.Printf("%s %s", time.Now().Sub(start), fullURL)
				}
			}
		})
	}

	// parse URLs
	group.Go(func() error {
		for {
			uri, err := getRequestURIFromLog(logReader)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err.Error())
			}
			if uri == "" {
				continue
			}
			select {
			case <-stop:
				return nil
			case urlChan <- baseURL + uri:
			}
		}
		return nil
	})

	// setup interrupt cleanup code
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	waitOnce := sync.Once{}
	wait := func() {
		waitOnce.Do(func() {
			if err := group.Wait(); err != nil {
				log.Fatal(err)
			}
		})
	}
	go func() {
		<-c
		close(stop)
		wait()
	}()

	// just wait for the magic to happen
	wait()
}
