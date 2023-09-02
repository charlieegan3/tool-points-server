package handlers

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/tkrajina/gpxgo/gpx"
	"google.golang.org/api/option"

	bq "github.com/charlieegan3/tool-points-server/pkg/bigquery"
)

//go:embed templates/periodIndex.html
var periodIndexTemplate string

func BuildPeriodIndexHandler(username, password string) func(http.ResponseWriter, *http.Request) {
	usernameHash := sha256.Sum256([]byte(username))
	passwordHash := sha256.Sum256([]byte(password))

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-a")

		u, p, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		uHash := sha256.Sum256([]byte(u))
		pHash := sha256.Sum256([]byte(p))
		if usernameHash != uHash || passwordHash != pHash {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		w.Write([]byte(periodIndexTemplate))
	}
}

func BuildPeriodGPXHandler(
	googleServiceAccountJSON,
	googleProject,
	bqDataset,
	bqTable,
	username,
	password string,
) func(http.ResponseWriter, *http.Request) {

	usernameHash := sha256.Sum256([]byte(username))
	passwordHash := sha256.Sum256([]byte(password))

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-a")

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

		fromValues, ok := r.URL.Query()["from"]
		if !ok || len(fromValues) != 1 {
			w.Header().Set("Content-Type", "application/text")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("from param required"))
			return
		}

		fromTime, err := time.Parse("2006-01-02", fromValues[0])
		if err != nil {
			w.Header().Set("Content-Type", "application/text")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid from date format"))
			return
		}

		toValues, ok := r.URL.Query()["to"]
		if !ok || len(toValues) != 1 {
			w.Header().Set("Content-Type", "application/text")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("to param required"))
			return
		}

		toTime, err := time.Parse("2006-01-02", toValues[0])
		if err != nil {
			w.Header().Set("Content-Type", "application/text")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid to date format"))
			return
		}
		// make it to the end of the day
		toTime = toTime.Add(24 * time.Hour).Add(-time.Second)

		points, err := bq.PointsInRange(
			r.Context(),
			bqClient,
			bqDataset,
			bqTable,
			fromTime,
			toTime,
		)
		if err != nil {
			w.Header().Set("Content-Type", "application/text")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		segment := gpx.GPXTrackSegment{}

		for _, point := range points {
			alt := gpx.NewNullableFloat64(point.Altitude)
			gpxPoint := gpx.GPXPoint{
				Point: gpx.Point{
					Latitude:  point.Latitude,
					Longitude: point.Longitude,
					Elevation: *alt,
				},
				Timestamp: point.CreatedAt,
			}
			segment.Points = append(segment.Points, gpxPoint)
		}

		g := &gpx.GPX{
			Version: "1.0",
			Creator: "photos.charlieegan3.com",
			Tracks: []gpx.GPXTrack{
				{
					Name:     fmt.Sprintf("%s to %s", fromValues[0], toValues[0]),
					Segments: []gpx.GPXTrackSegment{segment},
				},
			},
		}

		bytes, err := g.ToXml(gpx.ToXmlParams{
			Version: "1.0",
			Indent:  true,
		})

		w.Header().Set("Content-Type", "application/gpx+xml")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-to-%s.gpx", fromValues[0], toValues[0]))

		w.Write(bytes)
	}
}
