package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func checkInsert(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	b, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("POST", "/insert", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("storage returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
}

func checkSelect(t *testing.T, mux *http.ServeMux, features []geojson.Feature) {
	req, err := http.NewRequest("GET", "/select", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("Get", rr.Header().Get("location"), bytes.NewReader(nil))
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("storage returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}

		b, err := io.ReadAll(rr.Body)
		if err != nil {
			t.Fatal(err)
		}

		fs, err := geojson.UnmarshalFeatureCollection(b)
		if err != nil {
			t.Fatal(err)
		}

		if len(fs.Features) != len(features) {
			t.Errorf("wrong size of returned collection: got %d want %d", len(fs.Features), len(features))
		}
		sort.Slice(fs.Features, func(i, j int) bool {
			return fs.Features[i].ID.(string) < fs.Features[j].ID.(string)
		})
		sort.Slice(features, func(i, j int) bool {
			return features[i].ID.(string) < features[j].ID.(string)
		})
		for i := range features {
			if features[i].ID != fs.Features[i].ID {
				t.Errorf("invalid ids in returned collection")
			}
			if features[i].Geometry != fs.Features[i].Geometry {
				t.Errorf("invalid geometry in returned collection")
			}
		}
	} else {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
}

func checkReplace(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	b, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", "/replace", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("storage returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
}

func checkDelete(t *testing.T, mux *http.ServeMux, id string) {
	deleteReq := DeleteRequest{
		Id: id,
	}
	b, err := json.Marshal(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", "/delete", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("storage returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
}

func TestInsertAndSelect(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(wd + "/test")

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	go func() { s.Run() }()
	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()

	ids := []string{"first", "second", "third"}
	features := make([]geojson.Feature, 0)
	for _, id := range ids {
		feat := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
		feat.ID = id
		features = append(features, *feat)
	}

	for _, feat := range features {
		checkInsert(t, mux, &feat)
	}

	checkSelect(t, mux, features)

	r.Stop()
	s.Stop()
}

func TestReplaceAndDelete(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(wd + "/test")

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	go func() { s.Run() }()
	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"

	checkInsert(t, mux, feature)

	featureNew := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureNew.ID = "t"

	checkReplace(t, mux, featureNew)

	checkSelect(t, mux, []geojson.Feature{*featureNew})

	checkDelete(t, mux, "t")

	checkSelect(t, mux, []geojson.Feature{})

	r.Stop()
	s.Stop()
}

func checkError(t *testing.T, mux *http.ServeMux, method string, path string, b *bytes.Reader) {
	req, err := http.NewRequest(method, path, b)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest(method, rr.Header().Get("location"), b)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusInternalServerError {
			t.Errorf("insert was successfully called when feature exists")
		}
	} else {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}
}

func TestUncorrectCalls(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(wd + "/test")

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	go func() { s.Run() }()
	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"
	b, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	checkError(t, mux, "POST", "/replace", bytes.NewReader(b))

	checkInsert(t, mux, feature)

	checkError(t, mux, "POST", "/insert", bytes.NewReader(b))

	b, err = json.Marshal(DeleteRequest{
		Id: "unknown",
	})
	if err != nil {
		t.Fatal(err)
	}
	checkError(t, mux, "POST", "/delete", bytes.NewReader(b))

	r.Stop()
	s.Stop()
}
