package main

import (
	"bytes"
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
	if rr.Code != http.StatusOK {
		t.Errorf("router returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
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

func checkDelete(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	b, err := feature.MarshalJSON()
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
	s := NewStorage(mux, "test", true, make([]string, 0))
	s.Run()
	r := NewRouter(mux, 0, 10, 5, []Shard{{"test", []string{}}})
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
	s := NewStorage(mux, "test", true, make([]string, 0))
	s.Run()
	r := NewRouter(mux, 0, 10, 5, []Shard{{"test", []string{}}})
	r.Run()

	feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	feature.ID = "t"

	checkInsert(t, mux, feature)

	featureNew := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureNew.ID = "t"

	checkReplace(t, mux, featureNew)

	checkSelect(t, mux, make([]float64, 0), []geojson.Feature{*featureNew})

	checkDelete(t, mux, featureNew)

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
		if rr.Code != http.StatusInternalServerError && rr.Code != http.StatusNotFound {
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
	s := NewStorage(mux, "test", true, make([]string, 0))
	s.Run()
	r := NewRouter(mux, 0, 10, 5, []Shard{{"test", []string{}}})
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

	featureUnknown := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	featureUnknown.ID = "Unknown"
	b, err = featureUnknown.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	checkError(t, mux, "POST", "/delete", bytes.NewReader(b))

	r.Stop()
	s.Stop()
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
	s := NewStorage(mux, "test", true, make([]string, 0))
	s.Run()
	r := NewRouter(mux, 0, 10, 5, []Shard{{"test", []string{}}})
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

func TestSelectRectWithShards(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(wd + "/test"); err != nil {
		slog.Warn(err.Error())
	}

	mux := http.NewServeMux()
	s1 := NewStorage(mux, "test1", true, make([]string, 0))
	s1.Run()
	s2 := NewStorage(mux, "test2", true, make([]string, 0))
	s2.Run()
	r := NewRouter(mux, 0, 10, 5, []Shard{{"test1", []string{}}, {"test2", []string{}}})
	r.Run()

	feature1 := geojson.NewFeature(orb.Point{2, 2})
	feature1.ID = "1"
	feature2 := geojson.NewFeature(orb.Point{2, 7})
	feature2.ID = "2"
	feature3 := geojson.NewFeature(orb.Point{7, 2})
	feature3.ID = "3"
	feature4 := geojson.NewFeature(orb.Point{7, 7})
	feature4.ID = "4"

	checkInsert(t, mux, feature1)
	checkInsert(t, mux, feature2)
	checkInsert(t, mux, feature3)
	checkInsert(t, mux, feature4)

	checkSelect(t, mux, []float64{-10, -10, 10, 10}, []geojson.Feature{*feature1, *feature2, *feature3, *feature4})
	checkSelect(t, mux, []float64{20, 20, 30, 30}, []geojson.Feature{})
	checkSelect(t, mux, []float64{1, 1, 4, 4}, []geojson.Feature{*feature1})
	checkSelect(t, mux, []float64{1, 1, 4, 9}, []geojson.Feature{*feature1, *feature2})
}
