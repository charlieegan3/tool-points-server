package tool

import (
	"database/sql"
	"embed"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/Jeffail/gabs/v2"
	"github.com/charlieegan3/toolbelt/pkg/apis"
	"github.com/gorilla/mux"

	"github.com/charlieegan3/tool-points-server/pkg/tool/handlers"
)

// PointsServer receives requests from OwnTracks and stores them in bigquery
type PointsServer struct {
	config *gabs.Container

	bqTable, bqDataset, googleServiceAccountKey, googleProject string

	bqClient *bigquery.Client
}

func (d *PointsServer) Name() string {
	return "points-server"
}

func (d *PointsServer) FeatureSet() apis.FeatureSet {
	return apis.FeatureSet{
		Config: true,
		HTTP:   true,
	}
}

func (d *PointsServer) SetConfig(config map[string]any) error {
	var path string
	var ok bool

	d.config = gabs.Wrap(config)

	path = "bigquery.dataset"
	d.bqDataset, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	path = "bigquery.table"
	d.bqTable, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	path = "google.service_account_key"
	d.googleServiceAccountKey, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	path = "google.project"
	d.googleProject, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	return nil
}
func (d *PointsServer) HTTPPath() string {
	return "/points-server"
}
func (d *PointsServer) HTTPAttach(router *mux.Router) error {

	router.HandleFunc(
		"/points",
		handlers.BuildPointCreateHandler(
			d.googleProject,
			d.googleServiceAccountKey,
			d.bqDataset,
			d.bqTable,
		),
	).Methods("POST")

	return nil
}

func (d *PointsServer) Jobs() ([]apis.Job, error)                              { return []apis.Job{}, nil }
func (d *PointsServer) ExternalJobsFuncSet(f func(job apis.ExternalJob) error) {}
func (d *PointsServer) HTTPHost() string                                       { return "" }
func (d *PointsServer) DatabaseMigrations() (*embed.FS, string, error)         { return nil, "", nil }
func (d *PointsServer) DatabaseSet(db *sql.DB)                                 {}
