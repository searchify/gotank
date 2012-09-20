## Gotank - A Go client for the IndexTank API

## Install

    go get github.com/searchify/gotank/indextank

## Docs

<http://go.pkgdoc.org/github.com/searchify/gotank/indextank>

## Use

First, sign up for a Searchify account if you don't have one - <http://www.searchify.com>, or use the
[Heroku Searchify add-on](https://addons.heroku.com/searchify)


```go
    package main

    import (
        "github.com/searchify/gotank/indextank"
        "log"
    )

    func main() {
        // If running on Heroku, get your API URL from the environment:
        // API_URL := os.Getenv("SEARCHIFY_API_URL")
        API_URL := "http://...api.searchify.com"	 		// your private API URL from Searchify dashboard
        apiClient, err := indextank.NewApiClient(API_URL)
        if err != nil {
            log.Fatalln("Error creating client:", err)
        }
        idx = apiClient.GetIndex("idx")                     // use your index name here

        // Add a document
        docid := "mydoc1"
        fields := map[string]string { "text": "This is a testing Go golang document!" }
        variables := map[int]float32 { 0: -97.744444, 1: 30.428562 })
        err := idx.AddDocument(docid, fields, variables, nil)
        if err != nil {
            // handle errors
        }

        // Now search the index
        searchResults, err := idx.Search("golang")
        fmt.Printf("%d matches in %s seconds\n", searchResults.GetMatches(), searchResults.GetSearchTime())

        // this example is not completed yet
    }
```

## Notes

This is alpha -- use accordingly.  Please send bug fixes, code improvements, etc.

## Thank you

The IndexTank team


