package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
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

func checkSelect(t *testing.T, mux *http.ServeMux, rect []float64, features []geojson.Feature) {
	baseUrl, err := url.Parse("/select")
	if err != nil {
		t.Fatal(err.Error())
	}
	params := url.Values{}
	for _, r := range rect {
		params.Add("rect", strconv.FormatFloat(r, 'f', -1, 64))
	}
	baseUrl.RawQuery = params.Encode()
	finalUrl := baseUrl.String()

	req, err := http.NewRequest("GET", finalUrl, bytes.NewReader(nil))
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
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	s.Run()
	r := NewRouter(mux, [][]string{{"test"}})
	r.Run()

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

	checkSelect(t, mux, make([]float64, 0), features)

	r.Stop()
	s.Stop()
}

func TestReplaceAndDelete(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	s.Run()
	r := NewRouter(mux, [][]string{{"test"}})
	r.Run()

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"

	checkInsert(t, mux, feature)

	featureNew := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureNew.ID = "t"

	checkReplace(t, mux, featureNew)

	checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*featureNew})

	checkDelete(t, mux, "t")

	checkSelect(t, mux, make([]float64, 0), []geojson.Feature{})

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
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	s.Run()
	r := NewRouter(mux, [][]string{{"test"}})
	r.Run()

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

func TestRestartWithoutCheckpoint(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"

	featureNew := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureNew.ID = "t"

	t.Run("1", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkInsert(t, mux, feature)
	})
	t.Run("2", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*feature})

		checkReplace(t, mux, featureNew)
	})

	t.Run("3", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*featureNew})

		checkDelete(t, mux, "t")
	})

	t.Run("4", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{})
	})
}

func checkCheckpoint(t *testing.T, mux *http.ServeMux) {
	req, err := http.NewRequest("POST", "/checkpoint", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(nil))
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

func TestRestartWithCheckpoint(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"

	featureNew := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureNew.ID = "t"

	t.Run("1", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkInsert(t, mux, feature)
		checkCheckpoint(t, mux)
		os.Remove(s.getWalPath())
	})
	t.Run("2", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*feature})

		checkReplace(t, mux, featureNew)
		checkCheckpoint(t, mux)
		os.Remove(s.getWalPath())
	})

	t.Run("3", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*featureNew})

		checkDelete(t, mux, "t")
		checkCheckpoint(t, mux)
		os.Remove(s.getWalPath())
	})

	t.Run("4", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test")
		s.Run()
		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		checkSelect(t, mux, make([]float64, 0), []geojson.Feature{})
	})
}

func TestSelectRect(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	mux := http.NewServeMux()
	s := NewStorage(mux, "test")
	s.Run()
	r := NewRouter(mux, [][]string{{"test"}})
	r.Run()

	feature1 := geojson.NewFeature(orb.Point{0, 0})
	feature1.ID = "1"
	feature2 := geojson.NewFeature(orb.Point{2, 2})
	feature2.ID = "2"
	feature3 := geojson.NewFeature(orb.Point{-2, 3})
	feature3.ID = "3"
	feature4 := geojson.NewFeature(orb.Point{-1, -1})
	feature4.ID = "4"

	checkInsert(t, mux, feature1)
	checkInsert(t, mux, feature2)
	checkInsert(t, mux, feature3)
	checkInsert(t, mux, feature4)

	checkSelect(t, mux, []float64{-10, -10, 10, 10}, []geojson.Feature{*feature1, *feature2, *feature3, *feature4})
	checkSelect(t, mux, []float64{20, 20, 30, 30}, []geojson.Feature{})
	checkSelect(t, mux, []float64{-3, 1, 3, 4}, []geojson.Feature{*feature2, *feature3})
	checkSelect(t, mux, []float64{-1.5, -1, 5, 0.5, 0.5}, []geojson.Feature{*feature1, *feature4})
}
