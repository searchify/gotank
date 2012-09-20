package indextank

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"time"
)

// Provides access to a hosted search index for adding, updating, deleting, and searching documents.
type Index interface {
	// Exists returns whether this search index exists on the server.
	Exists() bool
	// HasStarted returns whether this search index is ready to receive requests
	HasStarted() bool
	// Status returns the running status of this search index
	Status() string
	// GetCode returns an internal identifier for this search index
	GetCode() string
	// GetSize returns the number of documents in this search index
	GetSize() int
	// GetCreationTime returns the time this search index was created
	GetCreationTime() *time.Time
	// IsPublicSearchEnabled returns whether public search is enabled
	IsPublicSearchEnabled() bool
	// CreateIndex creates a new search index on the server
	CreateIndex() error
	// CreateIndexWithOptions creates a new search index on the server with index options
	CreateIndexWithOptions(options map[string]interface{}) error
	// UpdateIndex updates the options for this search index
	UpdateIndex(options map[string]interface{}) error
	// DeleteIndex deletes this search index
	DeleteIndex() error
	// AddDocument adds a document to the search index. The variables and categories parameters can be nil.
	AddDocument(docid string, fields map[string]string, variables map[int]float32, categories map[string]string) error
	//AddDocumentWithCategories(docid string, fields map[string]string, variables map[int]float32, categories map[string]string) error
	// AddDocuments adds a batch of document to the search index.
	AddDocuments(documents []Document) (BatchResults, error)
	// UpdateVariables updates document variables for a given document, without affecting its text fields.
	UpdateVariables(documentId string, variables map[int]float32) error
	// UpdateCategories updates the categories for a given document.
	UpdateCategories(documentId string, categories map[string]string) error
	// DeleteDocument deletes a document from the search index.
	DeleteDocument(string) error
	// DeleteDocuments deletes a batch of documents from the search index. Check BulkDeleteResults for status.
	DeleteDocuments([]string) (BulkDeleteResults, error)
	// AddFunction sets a custom scoring function for a search index.
	AddFunction(functionIndex int, definition string) error
	// DeleteFunction removes a custom scoring function for a search index.
	DeleteFunction(functionIndex int) error
	// ListFunctions lists all scoring functions for this search index.
	ListFunctions() (map[string]string, error)
	// Search performs a search for a simple query string.
	Search(queryString string) (map[string]interface{}, error)
	// SearchWithQuery performs a search for an indextank.Query object.
	SearchWithQuery(query Query) (SearchResults, error)
	//DeleteBySearch()
	// GetMetadata returns metadata for a search index.
	GetMetadata() (map[string]interface{}, error)
}

type IndexClient struct {
	url      string
	metadata map[string]interface{}
}

func (client *IndexClient) CreateIndex() error {
	return client.CreateIndexWithOptions(nil)
}

func (client *IndexClient) CreateIndexWithOptions(options map[string]interface{}) error {
	if options == nil {
		options = make(map[string]interface{})
	}
	// error: io error, index already exists, maximum indexes exceeded
	// PUT to index url, body can contain JSON with "public_search":boolean
	// returns 201 if created,
	//         204 if already existed,
	//         409 if too many indexes

	resp, err := request("PUT", client.url, options)
	if err != nil {
		return err
	}
	if resp != nil {
		defer resp.Body.Close()
	}
	switch resp.StatusCode {
	case 201:
		client.GetMetadata()
		return nil
	case 204:
		return errors.New("Index already exists")
	case 409:
		return errors.New("Maximum indexes limit reached for this account")
	}
	return fmt.Errorf("Unexpected error, HTTP status %d: %s", resp.StatusCode, resp.Status)
}

func (client *IndexClient) UpdateIndex(options map[string]interface{}) error {
	resp, err := request("PUT", client.url, options)
	if resp != nil {
		defer resp.Body.Close()
	}
	if isOk(resp.StatusCode) {
		client.metadata, err = client.refreshMetadata()
		return nil
	}
	if resp.StatusCode == 404 {
		return errors.New("Index does not exist")
	}
	return err
}

func (client *IndexClient) DeleteIndex() error {
	// error: index does not exist, io error
	// returns 200 if OK, or 204 if no index existed
	_, err := request("DELETE", client.url, nil)
	return err
}

func (client *IndexClient) Exists() bool {
	_, err := client.refreshMetadata()
	return err == nil
}

func (client *IndexClient) HasStarted() bool {
	client.metadata, _ = client.refreshMetadata()
	return client.metadata["started"] == true
}

func (client *IndexClient) Status() string {
	if status, ok := client.metadata["status"]; ok {
		s := status.(string)
		return s
	}
	return ""
}

func (client *IndexClient) GetCode() string {
	if code, ok := client.metadata["code"]; ok {
		s := code.(string)
		return s
	}
	return ""
}

func (client *IndexClient) GetSize() int {
	if size, ok := client.metadata["size"]; ok {
		// json decodes it as a float64
		floatVal := size.(float64)
		return int(floatVal)
	}
	return -1
}

func (client *IndexClient) GetCreationTime() *time.Time {
	if creationTime, ok := client.metadata["creation_time"]; ok {
		t, err := parseTime(creationTime.(string))
		if err == nil {
			return &t
		}
	}
	return nil
}

func (client *IndexClient) IsPublicSearchEnabled() bool {
	return client.metadata["public_search"] == true
}

func (client *IndexClient) getMetadata(s string) (interface{}, error) {
	metadata, err := client.GetMetadata()
	if err != nil {
		return nil, err
	}
	val := metadata[s]
	if val == nil {
		return nil, errors.New("No such metadata item")
	}
	return val, nil
}

func (client *IndexClient) GetMetadata() (map[string]interface{}, error) {
	var err error
	if client.metadata == nil {
		client.metadata, err = client.refreshMetadata()
	}
	return client.metadata, err
}

func (client *IndexClient) refreshMetadata() (map[string]interface{}, error) {
	uri := client.url
	return doRequest("GET", uri, nil)
}

func (client *IndexClient) ListFunctions() (map[string]string, error) {
	functions_url := client.url + "/functions"
	resp, err := request("GET", functions_url, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if len(body) == 0 {
		return nil, err
	}

	m := map[string]string{}
	err = json.Unmarshal(body, &m)
	return m, err
}

func (client *IndexClient) AddFunction(functionIndex int, definition string) error {
	functions_url := client.url + "/functions/" + strconv.Itoa(functionIndex)

	data := map[string]string{"definition": definition}
	resp, err := request("PUT", functions_url, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	//fmt.Printf("request() resp status = %d\n", resp.StatusCode)
	if isOk(resp.StatusCode) {
		return nil
	}
	if resp.StatusCode == 400 {
		//body, _ := ioutil.ReadAll(resp.Body)
		body, _ := readResponseBody(resp)
		if len(body) > 0 {
			return errors.New(string(body))
		}
		//return errors.New(resp.Status)
	}

	// other errors:
	// IndexDoesNotExist
	// UnexpectedError

	return fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, resp.Status)
}

func (client *IndexClient) DeleteFunction(functionIndex int) error {
	functions_url := fmt.Sprintf("%s/functions/%d", client.url, functionIndex)
	resp, err := request("DELETE", functions_url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if isOk(resp.StatusCode) {
		return nil
	}
	if resp.StatusCode == 400 {
		// todo read resp body
		return errors.New(resp.Status)
	}
	return fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, resp.Status)
}

func (client *IndexClient) AddDocument(documentId string, fields map[string]string, variables map[int]float32,
	categories map[string]string) error {
	addUrl := client.url + "/docs"
	// todo - validate len(utf8(docId)) <= 1024
	data := map[string]interface{}{"docid": documentId, "fields": fields}
	if variables != nil {
		// convert int keys to strings because the json encoder only supports string keys
		vars := map[string]float32{}
		for k, v := range variables {
			vars[strconv.Itoa(k)] = v
		}
		data["variables"] = vars
	}
	if categories != nil {
		data["categories"] = categories
	}
	//fmt.Printf("AddDocument data: %v\n", data)
	resp, err := request("PUT", addUrl, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if isOk(resp.StatusCode) {
		return nil
	}
	if resp.StatusCode == 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		if len(body) > 0 {
			return errors.New(string(body))
		}
	}

	return errors.New("Unexpected error adding document")
}

type Document struct {
	Id         string             `json:"docid"`
	Fields     map[string]string  `json:"fields"`
	Variables  map[string]float32 `json:"variables,omitempty"`
	Categories map[string]string  `json:"categories,omitempty"`
}

//type Doc interface{}

func NewDocument(docid string, fields map[string]string, variables map[int]float32, categories map[string]string) (Document, error) {
	// convert int keys to strings because the json encoder only supports string keys
	vars := map[string]float32{}
	for k, v := range variables {
		vars[strconv.Itoa(k)] = v
	}
	// todo - validate len(utf8(docId)) <= 1024
	doc := Document{
		Id:         docid,
		Fields:     fields,
		Variables:  vars,
		Categories: categories,
	}
	return doc, nil
}

type addResult struct {
	Added bool   `json:"added"`
	Error string `json:"error"`
}

func (client *IndexClient) AddDocuments(documents []Document) (BatchResults, error) {
	addUrl := client.url + "/docs"

	// request body is a JSON list of documents, e.g.:
	// [ { "docid":"123", "fields": {"text","testing","title":"heya"}, "variables":{0:1}, "categories":{"type":"val"} } ]

	// todo - validate len(utf8(docId)) <= 1024

	//fmt.Printf("AddDocuments data: %v\n", documents)
	resp, err := request("PUT", addUrl, documents)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if isOk(resp.StatusCode) {
		// response body will be a JSON list e.g.
		// [ {"added":true }, {"added":false, "error":"something"} ]
		body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Printf("AddDocuments response: %s\n", string(body))
		// FAKE the body for testing
		//body = []byte(`[{"added":true}, {"added":false, "error":"Fake add error"}]`)
		r := make([]addResult, 0)
		err := json.Unmarshal(body, &r)
		if err != nil {
			//fmt.Printf("Error unmarshalling bulk add: %v\n", err)
			return nil, err
		}
		if len(documents) != len(r) {
			// something went wonky
			return nil, fmt.Errorf("Something is wrong, we have %d docs and %d results\n", len(documents), len(r))
		}
		//fmt.Printf("Bulk add unmarshalled results: %v\n", r)
		bd := newBatchResults(documents, r)
		//fmt.Printf("Failed docids: %v\n", bd.GetFailedDocuments())
		return bd, nil
	}

	if resp.StatusCode == 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		if len(body) > 0 {
			return nil, errors.New(string(body))
		}
	}

	return nil, errors.New("Unexpected error adding documents batch")
}

func (c *IndexClient) UpdateVariables(documentId string, variables map[int]float32) error {
	updateUrl := c.url + "/docs/variables"

	// convert int keys to strings because the json encoder only supports string keys
	vars := map[string]float32{}
	for k, v := range variables {
		vars[strconv.Itoa(k)] = v
	}
	data := map[string]interface{}{"docid": documentId, "variables": vars}
	//fmt.Printf("UpdateVariables data: %v\n", data)
	resp, err := request("PUT", updateUrl, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if isOk(resp.StatusCode) {
		return nil
	}
	if resp.StatusCode == 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		// todo - do this anytime we have an error (read the body)
		if len(body) > 0 {
			return errors.New(string(body))
		}
	}

	return errors.New("Unexpected error updating variables")
}

func (c *IndexClient) UpdateCategories(documentId string, categories map[string]string) error {
	//categoriesUrl := c.url + "/docs/categories"
	return errors.New("UpdateCategories not yet implemented")
}

func (client *IndexClient) DeleteDocument(documentId string) error {
	docs_url := client.url + "/docs?docid=" + url.QueryEscape(documentId)
	resp, err := request("DELETE", docs_url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if isOk(resp.StatusCode) {
		return nil
	}
	if resp.StatusCode == 404 {
		return errors.New("Index does not exist")
	}
	return fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, resp.Status)
}

// used in DeleteDocuments
type docPair struct {
	DocId string `json:"docid"`
}

type deleteResult struct {
	Deleted bool   `json:"deleted"`
	Error   string `json:"error"`
}

func (client *IndexClient) DeleteDocuments(documentIds []string) (BulkDeleteResults, error) {
	// request body should be JSON list like:
	// [ {"docid":"123"}, {"docid":"234"} ]

	docs := make([]docPair, 0, len(documentIds))
	for _, v := range documentIds {
		dp := docPair{DocId: v}
		docs = append(docs, dp)
	}

	docs_url := client.url + "/docs"
	resp, err := request("DELETE", docs_url, docs)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		// response body will be a JSON list e.g.
		// [ {"deleted":true }, {"deleted":false, "error":"something"} ]
		body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Printf("DeleteDocuments response: %s\n", string(body))
		// FAKE the body for testing
		//body = []byte(`[{"deleted":true}, {"deleted":false, "error":"Fake error"}]`)
		r := make([]deleteResult, 0)
		err := json.Unmarshal(body, &r)
		if err != nil {
			return nil, err
		}
		bd := newBulkResults(documentIds, r)
		//fmt.Printf("Failed docids: %v\n", bd.GetFailedDocids())
		return bd, nil
	}
	if resp.StatusCode == 404 {
		return nil, errors.New("Index does not exist")
	}
	return nil, fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, resp.Status)
}

type searchResults struct {
	// e.g. {"matches": 0, "facets": {}, "results": [], "didyoumean": null, "query": "cats OR dogs", "search_time": "0.004"}
	Matches    int64                     `json:"matches,omitempty"`
	Query      string                    `json:"query,omitempty"`
	SearchTime string                    `json:"search_time"`
	DidYouMean *string                   `json:"didyoumean,omitempty"`
	Results    []map[string]interface{}  `json:"results,omitempty"`
	Facets     map[string]map[string]int `json:"facets,omitempty"`
}

type SearchResults interface {
	GetResults() []map[string]interface{}
	GetMatches() int64
	GetQuery() string
	GetFacets() map[string]map[string]int
	GetSearchTime() float32
	GetDidYouMean() string
}

func (r *searchResults) GetMatches() int64 {
	return r.Matches
}

func (r *searchResults) GetQuery() string {
	return r.Query
}
func (r *searchResults) GetSearchTime() float32 {
	f, _ := strconv.ParseFloat(r.SearchTime, 32)
	return float32(f)
}
func (r *searchResults) GetDidYouMean() string {
	return *r.DidYouMean
}
func (r *searchResults) GetResults() []map[string]interface{} {
	return r.Results
}
func (r *searchResults) GetFacets() map[string]map[string]int {
	return r.Facets
}

//func (client *IndexClient) SearchWithQuery(query Query) (map[string]interface{}, error) {
func (client *IndexClient) SearchWithQuery(query Query) (SearchResults, error) {
	searchUrl := client.url + "/search"
	params := query.ToQueryParams()
	searchUrl += "?" + params
	//fmt.Printf(" search URL: %s\n", searchUrl)
	resp, err := request("GET", searchUrl, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if isOk(resp.StatusCode) {
		body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Printf("**BODY: %v\n", string(body))
		/*
			var r map[string]interface{}
			err := json.Unmarshal(body, &r)
			if err != nil {
				//fmt.Printf("Error unmarshalling searchResults: %v\n", err)
				return nil, err
			} */
		sr := new(searchResults)
		err = json.Unmarshal(body, sr)
		if err != nil {
			//fmt.Printf("Error unmarshalling searchResults: %v\n", err)
			return nil, err
		}
		//fmt.Printf("SearchResults object: %v\n", sr)
		if sr.DidYouMean == nil {
			empty := ""
			sr.DidYouMean = &empty
		}
		return sr, nil
	}
	// todo handle other HTTP statuses
	if resp.StatusCode == 404 {
		return nil, errors.New("Index does not exist")
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, body)
	}
	return nil, fmt.Errorf("Unexpected %d error: %s", resp.StatusCode, resp.Status)
}

func (client *IndexClient) Search(queryString string) (map[string]interface{}, error) {
	// search(self, query, start=None, length=None, scoring_function=None, snippet_fields=None,
	// fetch_fields=None, category_filters=None, variables=None, docvar_filters=None, function_filters=None,
	// fetch_variables=None, fetch_categories=None):
	searchUrl := client.url + "/search"
	//fmt.Printf(" search URL: %s\n", searchUrl)
	params := map[string]string{"q": queryString}
	return doRequest("GET", searchUrl, params)
}

const iSO8601Format = "2006-01-02T15:04:05"

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(iSO8601Format, s)
	return t, err
}
