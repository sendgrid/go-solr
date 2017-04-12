package solr

type SolrResponse struct {
	Status int `json:"status"`
	QTime  int `json:"qtime"`
	Params struct {
		Query  string `json:"q"`
		Indent string `json:"indent"`
		Wt     string `json:"wt"`
	} `json:"params"`
	Response struct {
		NumFound uint32 `json:"numFound"`
		Start    int    `json:"start"`
		Docs     []Doc  `json:"docs"`
	} `json:"response"`
	NextCursorMark string `json:"nextCursorMark"`
	Adds           Adds   `json:"adds"`
}

type Doc struct {
	ID      string `json:"id"`
	Version int    `json:"_version_"`
	Email   string `json:"email_sort"`
}

type Adds map[string]int

type updateResponse struct {
	Response struct {
		Status int `json:"status"`
		QTime  int `json:"QTime"`
		RF     int `json:"rf"`
		MinRF  int `json:"min_rf"`
	} `json:"responseHeader"`
	Error struct {
		Metadata []string `json:"metadata"`
		Msg      string   `json:"msg"`
		Code     int      `json:"code"`
	}
}

type deleteRequest struct {
	Delete []string `json:"delete"`
}
