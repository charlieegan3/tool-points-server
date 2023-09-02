package apis

type OwnTracksUpload struct {
	Latitude         float64 `json:"lat"`
	Longitude        float64 `json:"lon"`
	Accuracy         float64 `json:"acc"`
	VerticalAccuracy float64 `json:"vac"`
	Velocity         float64 `json:"vel"`
	Altitude         float64 `json:"alt"`
	Connection       string  `json:"conn"`
	Topic            string  `json:"topic"`
	Time             int64   `json:"tst"`
}
