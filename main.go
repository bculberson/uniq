package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/bculberson/uniq/httpd"
	"github.com/bculberson/uniq/store"
	"time"
)

var (
	joinAddress = kingpin.Flag("join", "Address to join ex: `127.0.0.1:11110`").String()
	singleMode  = kingpin.Flag("singlemode", "Single mode").Short('s').Bool()
	storagePath = kingpin.Flag("storagepath", "Storage path").Default("/tmp").String()
	httpAddress = kingpin.Flag("haddr", "Address for HTTP binding").Default("127.0.0.1:11111").String()
	raftAddress = kingpin.Flag("raddr", "Address for RAFT binding").Default("127.0.0.1:11112").String()
	logger      = log.New(os.Stderr, "[main] ", log.LstdFlags)
)

func main() {
	kingpin.Parse()
	if *joinAddress == "" && !*singleMode {
		logger.Println("Must supply join address when not standalone")
		os.Exit(1)
	}

	s := store.New(*storagePath, *raftAddress, *singleMode)
	if err := s.Open(); err != nil {
		logger.Fatalf("failed to open store: %s", err.Error())
	}

	if *joinAddress != "" {
		if err := join(*joinAddress, *raftAddress); err != nil {
			log.Fatalf("failed to join node at %s: %s", *joinAddress, err.Error())
		}
	}

	h := httpd.New(*httpAddress, s)
	if err := h.Start(); err != nil {
		logger.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	logger.Println("exiting")
}

func join(joinAddr, raftAddr string) error {
	data := url.Values{}
	data.Set("addr", raftAddr)

	for {
		resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
		if err != nil {
			log.Printf("Err joining cluster %v", err)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		resp.Body.Close()
		break
	}

	return nil
}
