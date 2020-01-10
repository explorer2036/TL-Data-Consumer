package server

import (
	"TL-Data-Consumer/config"
	"context"
	"net/http"
	"net/http/pprof"
	"sync"

	"github.com/emicklei/go-restful"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server defines the http server
type Server struct {
	server *http.Server
}

// NewServer returns a new server
func NewServer(settings *config.Config) *Server {
	s := &Server{}
	s.server = &http.Server{
		Addr:    settings.Server.AdminAddr,
		Handler: s.newContainer(),
	}
	return s
}

// GetMetrics returns the monitor metrics
func (s *Server) GetMetrics(r *restful.Request, w *restful.Response) {
	promhttp.Handler().ServeHTTP(w.ResponseWriter, r.Request)
}

// newContainer returns a restful container with routes
func (s *Server) newContainer() *restful.Container {
	container := restful.NewContainer()

	// pprof routes
	container.ServeMux.HandleFunc("/debug/pprof/", pprof.Index)
	container.ServeMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	container.ServeMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	container.ServeMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	container.ServeMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ws := new(restful.WebService)
	ws.Path("/").Doc("root").
		Consumes(restful.MIME_XML, restful.MIME_JSON).
		Produces(restful.MIME_JSON, restful.MIME_XML)

	ws.Route(ws.GET("/metrics").To(s.GetMetrics)) // metrics routes

	container.Add(ws)

	return container
}

// Start the server
func (s *Server) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.server.ListenAndServe(); err != nil {
			return
		}
	}()
}

// Stop shutdown the server
func (s *Server) Stop() {
	s.server.Shutdown(context.Background())
}
