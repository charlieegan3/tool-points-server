package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"

	"github.com/charlieegan3/tool-points-server/pkg/apis"
	bq "github.com/charlieegan3/tool-points-server/pkg/bigquery"
)

func BuildPointCreateHandler(googleServiceAccountJSON, googleProject, bqDataset, bqTable string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		bqClient, err := bigquery.NewClient(
			r.Context(),
			googleProject,
			option.WithCredentialsJSON([]byte(googleServiceAccountJSON)),
		)

		w.Header().Set("Content-Type", "text/html; charset=UTF-a")

		if val, ok := r.Header["Content-Type"]; !ok || val[0] != "application/json" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Content-Type must be 'multipart/form-data'"))
			return
		}

		upload := struct {
			Latitude         float64 `json:"lat"`
			Longitude        float64 `json:"lon"`
			Accuracy         float64 `json:"acc"`
			VerticalAccuracy float64 `json:"vac"`
			Velocity         float64 `json:"vel"`
			Altitude         float64 `json:"alt"`
			Connection       string  `json:"conn"`
			Topic            string  `json:"topic"`
			Time             int64   `json:"tst"`
		}{}

		bytes, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		err = json.Unmarshal(bytes, &upload)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		points := []apis.Point{
			{
				Latitude:         upload.Latitude,
				Longitude:        upload.Longitude,
				Velocity:         upload.VerticalAccuracy,
				Altitude:         upload.Altitude,
				Accuracy:         upload.Accuracy,
				VerticalAccuracy: upload.VerticalAccuracy,
				WasOffline:       upload.Connection == "o",
				CreatedAt:        time.Unix(upload.Time, 0),
			},
		}

		topicParts := strings.Split(upload.Topic, "/")
		if len(topicParts) != 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected topic format"))
			return
		}

		err = bq.InsertPoints(r.Context(), bqClient, points, bqDataset, bqTable)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	}
}
