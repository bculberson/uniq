package httpd

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var logger = log.New(os.Stderr, "[httpd] ", log.LstdFlags)

type Store interface {
	CheckNSet(key string, value string, expiration time.Time) (bool, string, error)
	Join(addr string) error
	IsLeader() bool
}

type Service struct {
	addr  string
	ln    net.Listener
	store Store
}

func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			logger.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

func (s *Service) Close() {
	s.ln.Close()
	return
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/check") {
		if s.store.IsLeader() {
			w.WriteHeader(http.StatusOK)
			return
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else if strings.HasPrefix(r.URL.Path, "/join") {
		r.ParseForm()
		addrParam := r.Form.Get("addr")
		err := s.store.Join(addrParam)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		} else {
			w.WriteHeader(http.StatusOK)
		}
	} else if strings.HasPrefix(r.URL.Path, "/cns") {
		r.ParseForm()
		keyParam := r.Form.Get("key")
		if keyParam == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("key id required\n"))
			return
		}
		durationParam := r.Form.Get("duration")
		if durationParam == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("duration id required\n"))
			return
		}
		duration, err := time.ParseDuration(durationParam)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		valueParam := r.Form.Get("value")
		exists, value, err := s.store.CheckNSet(keyParam, valueParam, time.Now().Add(duration))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		} else if exists {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(value))
			w.Write([]byte("\n"))
			return
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(value))
			w.Write([]byte("\n"))
			return
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}
