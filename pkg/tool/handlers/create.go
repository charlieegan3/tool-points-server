package handlers

import (
	"crypto/sha256"
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

func BuildPointCreateHandler(
	googleServiceAccountJSON,
	googleProject,
	bqDataset,
	bqTable,
	username,
	password string,
	callers map[string]int64,

) func(http.ResponseWriter, *http.Request) {

	usernameHash := sha256.Sum256([]byte(username))
	passwordHash := sha256.Sum256([]byte(password))

	return func(w http.ResponseWriter, r *http.Request) {

		u, p, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("missing credentials"))
			return
		}

		uHash := sha256.Sum256([]byte(u))
		pHash := sha256.Sum256([]byte(p))
		if usernameHash != uHash || passwordHash != pHash {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("invalid credentials"))
			return
		}

		bqClient, err := bigquery.NewClient(
			r.Context(),
			googleProject,
			option.WithCredentialsJSON([]byte(googleServiceAccountJSON)),
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		bytes, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
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

		err = json.Unmarshal(bytes, &upload)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		topicParts := strings.Split(upload.Topic, "/")
		if len(topicParts) != 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected topic format"))
			return
		}

		var callerID int64
		val, ok := callers[topicParts[2]]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("unknown caller"))
			return
		}
		callerID = val

		points := []apis.Point{
			{
				Latitude:         upload.Latitude,
				Longitude:        upload.Longitude,
				Velocity:         upload.VerticalAccuracy,
				Altitude:         upload.Altitude,
				Accuracy:         upload.Accuracy,
				VerticalAccuracy: upload.VerticalAccuracy,
				WasOffline:       upload.Connection == "o",
				// this is the ID of the owntracks importer
				ImporterID: 1,
				// this the id of the reason this endpoint
				ReasonID:  1,
				CallerID:  callerID,
				CreatedAt: time.Unix(upload.Time, 0),
			},
		}

		err = bq.InsertPoints(r.Context(), bqClient, points, bqDataset, bqTable)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	}
}
