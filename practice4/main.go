package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Shard struct {
	leader   string
	replicas []string
}

type Router struct {
	serverMux *http.ServeMux
	index     rtree.RTree
}

func NewRouter(r *http.ServeMux, min float64, max float64, steps int, shards []Shard) *Router {
	result := Router{
		serverMux: r,
	}

	var num = 0
	result.index.Insert([2]float64{-math.Inf(1), -math.Inf(1)}, [2]float64{min, min}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{-math.Inf(1), min}, [2]float64{min, max}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{-math.Inf(1), max}, [2]float64{min, math.Inf(1)}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{max, -math.Inf(1)}, [2]float64{math.Inf(1), min}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{max, min}, [2]float64{math.Inf(1), max}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{max, max}, [2]float64{math.Inf(1), math.Inf(1)}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{min, -math.Inf(1)}, [2]float64{max, min}, shards[num])
	num++
	num %= len(shards)
	result.index.Insert([2]float64{min, max}, [2]float64{max, math.Inf(1)}, shards[num])
	num++
	num %= len(shards)

	var d = (max - min) / float64(steps)
	for i := min; i < max; i += d {
		for j := min; j < max; j += d {
			result.index.Insert([2]float64{i, j}, [2]float64{i + d, j + d}, shards[num])
			num++
			num %= len(shards)
		}
	}

	return &result
}

func (router *Router) handleToSingleLeader(w http.ResponseWriter, r *http.Request, actionPath string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	feat, err := geojson.UnmarshalFeature(body)
	if err != nil {
		writeError(w, err)
		return
	}
	was := false
	router.index.Search(feat.Geometry.Bound().Min, feat.Geometry.Bound().Max, func(min, max [2]float64, data interface{}) bool {
		shard, ok := data.(Shard)
		if !ok {
			slog.Error("Not a shard in index")
			panic("Not a shard in index")
		}
		http.Redirect(w, r, "/"+shard.leader+"/"+actionPath, http.StatusTemporaryRedirect)
		was = true
		return true
	})

	if !was {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (r *Router) Run() {
	r.serverMux.Handle("/", http.FileServer(http.Dir("../front/dist")))

	r.serverMux.HandleFunc("/insert", func(w http.ResponseWriter, req *http.Request) {
		r.handleToSingleLeader(w, req, "insert")
	})
	r.serverMux.HandleFunc("/select", func(w http.ResponseWriter, req *http.Request) {
		spl := req.URL.Query()["rect"]

		var rect *Rect = &Rect{
			Min: [2]float64{-math.Inf(1), -math.Inf(1)},
			Max: [2]float64{math.Inf(1), math.Inf(1)},
		}
		if len(spl) >= 4 {
			slog.Warn("SPL len", "len", len(spl))
			minx, err := strconv.ParseFloat(spl[0], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			miny, err := strconv.ParseFloat(spl[1], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			maxx, err := strconv.ParseFloat(spl[2], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			maxy, err := strconv.ParseFloat(spl[3], 64)
			if err != nil {
				writeError(w, err)
				return
			}

			rect = &Rect{
				Min: [2]float64{minx, miny},
				Max: [2]float64{maxx, maxy},
			}
		}

		result := geojson.NewFeatureCollection()
		resultIds := make(map[string]bool)

		r.index.Search(rect.Min, rect.Max, func(min, max [2]float64, data interface{}) bool {
			shard := data.(Shard)
			path := shard.leader
			if len(shard.replicas) > 0 {
				replicaIndex := rand.Intn(len(shard.replicas))
				path = shard.replicas[replicaIndex]
			}
			reqNew, err := http.NewRequest("GET", "/"+path+"/select?"+req.URL.RawQuery, bytes.NewReader(nil))
			resp := httptest.NewRecorder()
			r.serverMux.ServeHTTP(resp, reqNew)
			// res, err := http.Get(req.Host + ":" + req.URL.Port() + "/" + path + "/select?" + req.URL.RawQuery)
			if err != nil {
				slog.Error(err.Error())
				return false
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				slog.Error(err.Error())
				return false
			}
			current := geojson.NewFeatureCollection()
			current.UnmarshalJSON(body)
			for _, f := range current.Features {
				if v, ok := resultIds[f.ID.(string)]; !ok || !v {
					result.Append(f)
					resultIds[f.ID.(string)] = true
				}
			}
			return true
		})

		response, err := result.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}
		w.Write(response)
	})
	r.serverMux.HandleFunc("/delete", func(w http.ResponseWriter, req *http.Request) {
		r.handleToSingleLeader(w, req, "delete")
	})
	r.serverMux.HandleFunc("/replace", func(w http.ResponseWriter, req *http.Request) {
		r.handleToSingleLeader(w, req, "replace")
	})
}

func (r *Router) Stop() {
}

type Transaction struct {
	Action  string
	Name    string
	Lsn     uint64
	Feature *geojson.Feature
}

const insertAction = "insert"
const deleteAction = "delete"
const replaceAction = "replace"
const selectAction = "select"
const checkpointAction = "checkpoint"

type Message struct {
	action string
	data   any
	result chan any
}

func transaction2Message(t Transaction) Message {
	data := t.Feature.ID
	if t.Action != deleteAction {
		data = t.Feature
	}
	return Message{
		action: t.Action,
		data:   data,
		result: make(chan any),
	}
}

type Storage struct {
	name     string
	features map[string]*geojson.Feature
	rtree    rtree.RTree
	lsn      uint64
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan Message
	replicas map[string]*websocket.Conn
	leader   bool
}

func writeError(w http.ResponseWriter, err error) {
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
		writeError(*w, err)
		return
	}

	feat, err := geojson.UnmarshalFeature(body)
	if err != nil {
		writeError(*w, err)
		return
	}

	id, ok := feat.ID.(string)
	if !ok {
		slog.Error("ID is not string")
		(*w).WriteHeader(http.StatusInternalServerError)
		(*w).Write([]byte("ID is not string"))
		return
	}

	ansChan := make(chan any)
	action := insertAction
	if replace {
		action = replaceAction
	}
	s.queue <- Message{
		action: action,
		data:   feat,
		result: ansChan,
	}

	slog.Info(id)
	ans := <-ansChan
	if ans != nil {
		err, ok := ans.(error)
		if !ok {
			writeError(*w, errors.New("Internal error"))
		}
		writeError(*w, err)
	}

	slog.Info("Successfully")
}

func (s *Storage) getDir() string {
	wd, err := os.Getwd()

	if err != nil {
		panic("Can't get os.Getwd")
	}
	return wd + "/" + s.name
}

func (s *Storage) getCkpPath() string {
	return s.getDir() + "/" + s.name + ".ckp"
}

func (s *Storage) getWalPath() string {
	return s.getDir() + "/" + s.name + ".wal"
}

func (s *Storage) writeCheckpoint() error {
	features := geojson.NewFeatureCollection()

	for _, feature := range s.features {
		features.Append(feature)
	}

	marshalled, err := features.MarshalJSON()
	if err != nil {
		return err
	}

	err = os.WriteFile(s.getCkpPath(), marshalled, 0666)
	os.Remove(s.getWalPath())
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) readCheckpoint() error {
	f, err := os.ReadFile(s.getCkpPath())
	if err != nil {
		return err
	}

	features, err := geojson.UnmarshalFeatureCollection(f)
	if err != nil {
		return err
	}

	s.features = make(map[string]*geojson.Feature)
	for _, feat := range features.Features {
		s.features[feat.ID.(string)] = feat
		s.rtree.Insert(feat.Geometry.Bound().Min, feat.Geometry.Bound().Max, feat)
	}
	return nil
}

func (s *Storage) readWal() error {
	f, err := os.ReadFile(s.getWalPath())
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(f))
	for decoder.More() {
		transaction := Transaction{}
		err := decoder.Decode(&transaction)
		if err != nil {
			return err
		}
		s.applyTransaction(&transaction)
	}
	return nil
}

type Rect struct {
	Min [2]float64
	Max [2]float64
}

func NewStorage(r *http.ServeMux, name string, leader bool, replicas []string) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	result := Storage{
		name:     name,
		features: make(map[string]*geojson.Feature),
		rtree:    rtree.RTree{},
		lsn:      0,
		ctx:      ctx,
		cancel:   cancel,
		queue:    make(chan Message),
		replicas: make(map[string]*websocket.Conn),
		leader:   leader,
	}
	dir := result.getDir()
	os.MkdirAll(dir, 0777)
	slog.Info("Working directory for '" + name + "' storage is " + dir)

	if leader {
		for _, replica := range replicas {
			conn, _, err := websocket.DefaultDialer.Dial(replica+"/replication", nil)
			if err != nil {
				panic(err.Error())
			}

			slog.Info("Connection OK for replica: " + replica)

			result.replicas[replica] = conn
		}
	}

	r.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("Select")

		spl := r.URL.Query()["rect"]

		var rect *Rect = nil
		if len(spl) >= 4 {
			slog.Warn("SPL len", "len", len(spl))
			minx, err := strconv.ParseFloat(spl[0], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			miny, err := strconv.ParseFloat(spl[1], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			maxx, err := strconv.ParseFloat(spl[2], 64)
			if err != nil {
				writeError(w, err)
				return
			}
			maxy, err := strconv.ParseFloat(spl[3], 64)
			if err != nil {
				writeError(w, err)
				return
			}

			rect = &Rect{
				Min: [2]float64{minx, miny},
				Max: [2]float64{maxx, maxy},
			}
		}
		ansChan := make(chan any)
		result.queue <- Message{
			action: selectAction,
			data:   rect,
			result: ansChan,
		}

		ans := <-ansChan
		collection, ok := ans.(*geojson.FeatureCollection)
		if !ok {
			err, ok := ans.(error)
			if !ok {
				err = errors.New("Internal error")
			}
			writeError(w, err)
			return
		}

		b, err := collection.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}

		_, err = w.Write(b)
		if err != nil {
			writeError(w, err)
			return
		}
	})

	if leader {

		r.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Insert")

			saveFeature(&result, &r.Body, &w, false)
		})

		r.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Replace")
			saveFeature(&result, &r.Body, &w, true)
		})

		r.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Delete")

			b, err := io.ReadAll(r.Body)
			if err != nil {
				writeError(w, err)
				return
			}

			delRequest := DeleteRequest{}
			err = json.Unmarshal(b, &delRequest)
			if err != nil {
				writeError(w, err)
				return
			}

			ansChan := make(chan any)
			result.queue <- Message{
				action: deleteAction,
				data:   delRequest.Id,
				result: ansChan,
			}

			ans := <-ansChan
			if ans != nil {
				err, ok := ans.(error)
				if !ok {
					writeError(w, errors.New("Internal error"))
					return
				}
				writeError(w, err)
				return
			}
			slog.Info("Successfully")
		})

		r.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Checkpoint")

			ansChan := make(chan any)
			result.queue <- Message{
				action: checkpointAction,
				data:   nil,
				result: ansChan,
			}

			ans := <-ansChan
			if ans != nil {
				err, ok := ans.(error)
				if !ok {
					writeError(w, errors.New("Internal error"))
					return
				}
				writeError(w, err)
				return
			}
			slog.Info("Successfully")
		})
	} else {
		r.HandleFunc("/"+name+"/replication", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Try to receive connection from leader for " + result.name)
			upgrader := websocket.Upgrader{
				ReadBufferSize:  1024,
				WriteBufferSize: 1024,
			}
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				writeError(w, err)
				return
			}
			defer ws.Close()
			for {
				transaction := Transaction{}
				err := ws.ReadJSON(&transaction)
				// _, bytes, err := ws.ReadMessage()
				if err != nil {
					if _, ok := err.(*websocket.CloseError); ok {
						slog.Info("Closed WS")
						return
					}
					ws.WriteMessage(websocket.TextMessage, []byte("Error during reading message from WS"+err.Error()))
					slog.Error("Error during reading message from WS: " + err.Error())
					continue
				}

				// err = json.Unmarshal(bytes, transaction)
				// if err != nil {
				// 	ws.WriteMessage(websocket.TextMessage, []byte("Error during Unmarshaling transaction from WS"))
				// 	slog.Error("Error during Unmarshaling transaction from WS")
				// 	continue
				// }

				message := transaction2Message(transaction)
				result.queue <- message
				ans := <-message.result
				if ans != nil {
					err, ok := ans.(error)
					if !ok {
						ws.WriteMessage(websocket.TextMessage, []byte("Error during processing transaction from WS"))
						slog.Error("Error during processing transaction from WS")
						continue
					}
					ws.WriteMessage(websocket.TextMessage, []byte(err.Error()))
					slog.Error(err.Error())
					continue
				}
				ws.WriteMessage(websocket.TextMessage, []byte("OK"))
				slog.Info("Successfully")
			}
		})
	}

	return &result
}

func (s *Storage) writeTransaction(transaction *Transaction) error {
	j, err := json.Marshal(*transaction)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(s.getWalPath(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer f.Close()
	_, err = f.Write(j)
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) applyTransaction(transaction *Transaction) error {
	switch transaction.Action {
	case insertAction:
		if _, ok := s.features[transaction.Feature.ID.(string)]; ok {
			return errors.New("Feature with ID=" + transaction.Feature.ID.(string) + " already exists")
		}

		s.features[transaction.Feature.ID.(string)] = transaction.Feature
		s.rtree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case replaceAction:
		old, ok := s.features[transaction.Feature.ID.(string)]
		if !ok {
			return errors.New("Feature with ID=" + transaction.Feature.ID.(string) + " is not exists")
		}

		s.features[transaction.Feature.ID.(string)] = transaction.Feature
		s.rtree.Replace(
			old.Geometry.Bound().Min, old.Geometry.Bound().Max, old,
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature,
		)
	case deleteAction:
		if transaction.Feature == nil {
			return errors.New("Feature is not exists")
		}
		old, ok := s.features[transaction.Feature.ID.(string)]
		if !ok {
			return errors.New("Feature with ID=" + transaction.Feature.ID.(string) + " is not exists")
		}

		delete(s.features, transaction.Feature.ID.(string))
		s.rtree.Delete(old.Geometry.Bound().Min, old.Geometry.Bound().Max, old)
	}

	return nil
}

func (s *Storage) sendToReplicas(transaction *Transaction) {
	for _, replica := range s.replicas {
		replica.WriteJSON(transaction)
		_, bytes, _ := replica.ReadMessage()
		str := string(bytes)
		if str != "OK" {
			slog.Error(str)
		} else {
			slog.Info("Replication OK")
		}
	}
}

func engine(s *Storage) {
	err := s.readCheckpoint()
	if err != nil {
		slog.Warn(err.Error())
	}
	err = s.readWal()
	if err != nil {
		slog.Warn(err.Error())
	}
	slog.Info("Size: %d", "size", len(s.features))
	vclock := make(map[string]uint64)
	vclock[s.name] = s.lsn

	for {
		select {
		case <-s.ctx.Done():
			return
		case message := <-s.queue:
			switch message.action {
			case insertAction:
				feat, ok := message.data.(*geojson.Feature)
				if !ok {
					message.result <- errors.New("Internal error")
					break
				}

				transaction := Transaction{
					Action:  insertAction,
					Name:    s.name,
					Lsn:     s.lsn,
					Feature: feat,
				}
				s.lsn++
				if s.lsn <= vclock[s.name] {
					break
				}
				vclock[s.name] = s.lsn

				err := s.writeTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				err = s.applyTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				s.sendToReplicas(&transaction)
				message.result <- nil
			case replaceAction:
				feat, ok := message.data.(*geojson.Feature)
				if !ok {
					message.result <- errors.New("Internal error")
					break
				}

				transaction := Transaction{
					Action:  replaceAction,
					Name:    s.name,
					Lsn:     s.lsn,
					Feature: feat,
				}
				s.lsn++
				err := s.writeTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				err = s.applyTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				s.sendToReplicas(&transaction)
				message.result <- nil
			case deleteAction:
				id, ok := message.data.(string)
				if !ok {
					message.result <- errors.New("Internal error")
					break
				}

				transaction := Transaction{
					Action:  deleteAction,
					Name:    s.name,
					Lsn:     s.lsn,
					Feature: s.features[id],
				}
				s.lsn++
				err := s.writeTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				err = s.applyTransaction(&transaction)
				if err != nil {
					message.result <- err
					break
				}
				s.sendToReplicas(&transaction)
				message.result <- nil
			case selectAction:
				rect, _ := message.data.(*Rect)
				collection := geojson.NewFeatureCollection()
				if rect == nil {
					for _, feat := range s.features {
						collection.Append(feat)
					}
				} else {
					s.rtree.Search(
						rect.Min, rect.Max,
						func(min [2]float64, max [2]float64, data interface{}) bool {
							collection.Append(data.(*geojson.Feature))
							return true
						},
					)
				}
				message.result <- collection
			case checkpointAction:
				slog.Info("Checkpoint")

				err := s.writeCheckpoint()
				if err != nil {
					message.result <- err
					break
				}

				message.result <- nil
			}
		}
	}
}

func (s *Storage) Run() {
	go engine(s)
}

func (s *Storage) Stop() {
	if s.leader {
		for _, replica := range s.replicas {
			replica.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			replica.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, _, err := replica.ReadMessage()
			if err != nil {
				if err, ok := err.(*websocket.CloseError); !ok {
					panic(err.Error())
				}
			}
			replica.Close()
		}
	}
	s.cancel()
}

func main() {
	mux := http.NewServeMux()
	l := http.Server{}
	l.Addr = "127.0.0.1:8080"
	l.Handler = mux

	s1 := NewStorage(mux, "shard1", true, make([]string, 0))
	s1.Run()
	s2 := NewStorage(mux, "shard2", true, make([]string, 0))
	s2.Run()
	s3 := NewStorage(mux, "shard3", true, make([]string, 0))
	s3.Run()
	r1 := NewRouter(mux, -10, 10, 5, []Shard{
		{"shard1", []string{}},
		{"shard2", []string{}},
		{"shard3", []string{}},
	})
	r1.Run()

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
	slog.Info("listen leader http://" + l.Addr)
	slog.Info("listen replica http://" + l.Addr)
	l.ListenAndServe()

	s1.Stop()
	s2.Stop()
	s3.Stop()
	r1.Stop()
}
