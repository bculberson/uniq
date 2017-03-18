package httpd

import (
	"log"
	"net"
	"net/http"
	"os"
	"time"
	_ "net/http/pprof"
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
	go func() {
		http.HandleFunc("/check", s.checkHandler)
		http.HandleFunc("/join", s.joinHandler)
		http.HandleFunc("/cns", s.cnsHandler)
		err := http.ListenAndServe(s.addr, nil)
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

func (s *Service) checkHandler(w http.ResponseWriter, r *http.Request) {
	if s.store.IsLeader() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *Service) joinHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	addrParam := r.Form.Get("addr")
	err := s.store.Join(addrParam)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Service) cnsHandler(w http.ResponseWriter, r *http.Request) {
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
	} else if exists {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(value))
		w.Write([]byte("\n"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
		w.Write([]byte("\n"))
	}
}

