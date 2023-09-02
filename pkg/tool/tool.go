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

	username, password string

	callers map[string]int64

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

	path = "auth.username"
	d.username, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	path = "auth.password"
	d.password, ok = d.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("config path %s is not a string", path)
	}

	path = "callers"
	callers, ok := d.config.Path(path).Data().(map[string]interface{})
	if !ok {
		return fmt.Errorf("config path %s is not a map", path)
	}

	d.callers = make(map[string]int64)
	for caller, id := range callers {
		intID, ok := id.(int)
		if !ok {
			return fmt.Errorf("caller %s id is not an int", caller)
		}
		d.callers[caller] = int64(intID)
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
			d.googleServiceAccountKey,
			d.googleProject,
			d.bqDataset,
			d.bqTable,
			d.username,
			d.password,
			d.callers,
		),
	).Methods("POST")

	return nil
}

func (d *PointsServer) Jobs() ([]apis.Job, error)                              { return []apis.Job{}, nil }
func (d *PointsServer) ExternalJobsFuncSet(f func(job apis.ExternalJob) error) {}
func (d *PointsServer) HTTPHost() string                                       { return "" }
func (d *PointsServer) DatabaseMigrations() (*embed.FS, string, error)         { return nil, "", nil }
func (d *PointsServer) DatabaseSet(db *sql.DB)                                 {}
