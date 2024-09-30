package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paulmach/orb/geojson"
)

type Router struct {
}

func NewRouter(r *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	r.Handle("/", http.FileServer(http.Dir("../front/dist")))

	for _, el := range nodes {
		for _, node := range el {
			r.Handle("/insert", http.RedirectHandler("/"+node+"/insert", http.StatusTemporaryRedirect))
			r.Handle("/select", http.RedirectHandler("/"+node+"/select", http.StatusTemporaryRedirect))
			r.Handle("/delete", http.RedirectHandler("/"+node+"/delete", http.StatusTemporaryRedirect))
			r.Handle("/replace", http.RedirectHandler("/"+node+"/replace", http.StatusTemporaryRedirect))
		}
	}
	return &result
}

func (r *Router) Run() {

}

func (r *Router) Stop() {

}

type Storage struct {
	name string
}

func WriteError(w http.ResponseWriter, err error) {
	slog.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

type DeleteRequest struct {
	Id string
}

func saveFeature(s *Storage, r *io.ReadCloser, w *http.ResponseWriter, replace bool) {
	body, err := io.ReadAll(*r)
	if err != nil {
		WriteError(*w, err)
		return
	}

	feat, err := geojson.UnmarshalFeature(body)
	if err != nil {
		WriteError(*w, err)
		return
	}

	id, ok := feat.ID.(string)
	if !ok {
		slog.Error("ID is not string")
		(*w).WriteHeader(http.StatusInternalServerError)
		(*w).Write([]byte("ID is not string"))
		return
	}

	slog.Info(id)

	json, err := feat.MarshalJSON()
	if err != nil {
		WriteError(*w, err)
		return
	}

	path := s.getDir() + "/" + id + ".json"
	if replace {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			WriteError(*w, err)
			return
		}
	} else if _, err := os.Stat(path); err == nil {
		(*w).WriteHeader(http.StatusInternalServerError)
		(*w).Write([]byte("Feature with id '" + feat.ID.(string) + "' already exists"))
		slog.Error("Feature with id '" + feat.ID.(string) + "' already exists")
		return
	}

	err = os.WriteFile(path, json, 0666)
	if err != nil {
		WriteError(*w, err)
		return
	}

	slog.Info("Successfully")
	if err != nil {
		WriteError(*w, err)
		return
	}
}

func (s *Storage) getDir() string {
	wd, err := os.Getwd()

	if err != nil {
		panic("Can't get os.Getwd")
	}
	slog.Warn(wd + "/" + s.name)
	return wd + "/" + s.name
}

func NewStorage(r *http.ServeMux, name string) *Storage {
	result := Storage{
		name: name,
	}
	dir := result.getDir()
	os.MkdirAll(dir, 0777)
	slog.Info("Working directory for '" + name + "' storage is " + dir)

	r.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Insert")

		saveFeature(&result, &r.Body, &w, false)
	})

	r.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Select")

		elems, err := os.ReadDir(dir)
		if err != nil {
			WriteError(w, err)
			return
		}

		collection := geojson.NewFeatureCollection()

		for _, el := range elems {
			if el.IsDir() {
				slog.Warn("Unsupported internal dirs in Storage '" + name + "'")
				continue
			}
			row, err := os.ReadFile(dir + "/" + el.Name())
			if err != nil {
				WriteError(w, err)
				return
			}

			feature, err := geojson.UnmarshalFeature(row)
			if err != nil {
				WriteError(w, err)
				return
			}

			collection.Append(feature)
		}

		b, err := collection.MarshalJSON()
		if err != nil {
			WriteError(w, err)
			return
		}

		_, err = w.Write(b)
		if err != nil {
			WriteError(w, err)
			return
		}
	})

	r.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Replace")
		saveFeature(&result, &r.Body, &w, true)
	})

	r.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Delete")

		b, err := io.ReadAll(r.Body)
		if err != nil {
			WriteError(w, err)
			return
		}

		delRequest := DeleteRequest{}
		err = json.Unmarshal(b, &delRequest)
		if err != nil {
			WriteError(w, err)
			return
		}

		err = os.Remove(dir + "/" + delRequest.Id + ".json")
		if err != nil {
			WriteError(w, err)
			return
		}
	})

	return &result
}

func (r *Storage) Run() {

}

func (r *Storage) Stop() {

}

func main() {
	r := http.ServeMux{}

	router := NewRouter(&r, [][]string{{"storage"}})
	router.Run()

	storage := NewStorage(&r, "storage")
	storage.Run()

	l := http.Server{}
	l.Addr = "127.0.0.1:8080"
	l.Handler = &r

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		for _ = range sigs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l.Shutdown(ctx)
		}
	}()

	defer slog.Info("we are going down")
	slog.Info("listen http://" + l.Addr)
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}
	router.Stop()
	storage.Stop()
}
