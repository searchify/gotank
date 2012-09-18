
// Provides API access to a Searchify hosted IndexTank account & indexes.
// Example usage:
//   API_URL := "http://...api.searchify.com"	 		// your private API URL
//   apiClient, err := indextank.NewApiClient(API_URL)
//   if err != nil { ... }
//   idx = apiClient.GetIndex("idx")
//
//   // Add a document
//   docid := "mydoc1"
//   fields := map[string]string { "text": "This is a testing Go golang document!" }
//   variables := map[int]float32 { 0: -97.744444, 1: 30.428562 })
//   err := idx.AddDocument(docid, fields, variables, nil)
//   if err != nil {
//      // handle error
//   }
//
//   // Now search the index
//   searchResults, err := idx.Search("golang")
package indextank

import (
	"strings"
	"net/url"
	"errors"
)

// Provides an interface manage indexes.
// To add & delete documents, and perform searches, get an Index from the ApiClient.
type ApiClient interface {
	// Gets a search index client.
	GetIndex(name string) Index
	// Creates a new search index on the server
	CreateIndex(name string) (Index, error)
	// Creates a new search index on the server, with options. The only current option
	// is a boolean "public_search", which sets whether public searches are allowed.
	CreateIndexWithOptions(name string, options map[string]interface{}) (Index, error)
	// Updates options for a search index.
	UpdateIndex(name string, options map[string]interface{}) error
	// Deletes a search index.
	DeleteIndex(name string) error
	// Lists search indexes for this account.
	ListIndexes() (map[string]Index, error)
}

type indexTankClient struct {
	apiUrl string
}

// Returns a new ApiClient from a Searchify API URL.
func NewApiClient(apiUrl string) (ApiClient, error) {
	// validate URL
	uri, err := url.Parse(apiUrl)
	if err != nil {
		return nil, err
	}
	if uri.Scheme != "http" && uri.Scheme != "https" {
		return nil, errors.New("URL scheme must be http or https")
	}
	if strings.HasSuffix(apiUrl, "/") {
		apiUrl = apiUrl[0:len(apiUrl)-1]
	}
	client := indexTankClient{apiUrl}
	return &client, nil
}

// Returns a search Index for this account.
func (client *indexTankClient) GetIndex(name string) Index {
	indexUrl := makeIndexUrl(client.apiUrl, name)
	ic := IndexClient{url:indexUrl}
	return &ic
}

// Creates a new search index.
func (client *indexTankClient) CreateIndex(name string) (Index, error) {   // todo: add options param
	indexUrl := makeIndexUrl(client.apiUrl, name)
	index := IndexClient{url:indexUrl}
	return &index, index.CreateIndex()
}

// Creates a new search index, with optional parameters.
// Allowed parameters are currently:
// "public_search", a boolean - whether to enable searches to this index using the public API URL
func (client *indexTankClient) CreateIndexWithOptions(name string, options map[string]interface{}) (Index, error) {
	indexUrl := makeIndexUrl(client.apiUrl, name)
	index := IndexClient{url:indexUrl}
	return &index, index.CreateIndexWithOptions(options)
}

// Updates the options for this index.  Currently allowed index options:
// "public_search" - see the CreateIndexWithOptions doc above.
func (client *indexTankClient) UpdateIndex(name string, options map[string]interface{}) error {
	indexUrl := makeIndexUrl(client.apiUrl, name)
	index := IndexClient{url:indexUrl}
	return index.UpdateIndex(options)
}

// Permanently deletes the specified index and all its documents from the server.
func (client *indexTankClient) DeleteIndex(name string) error {
	indexUrl := makeIndexUrl(client.apiUrl, name)
	index := IndexClient{url:indexUrl}
	return index.DeleteIndex()
}

// Lists all indexes for this account, returning a map from index name to Index.
func (client *indexTankClient) ListIndexes() (map[string]Index, error) {
	uri := makeIndexUrl(client.apiUrl, "")

	m, err := doRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	indexMap := make(map[string]Index)
	//m := i.(map[string]interface{})
	for k, v := range m {
		indexUrl := uri + k //"/" + k
		indexClient := IndexClient{url:indexUrl, metadata:v.(map[string]interface{})}
		//indexes = append(indexes, indexClient)
		indexMap[k] = &indexClient
	}
	return indexMap, err
}

func (client *indexTankClient) String() string {
	return "IndexTankClient, API URL: " + client.apiUrl
}
