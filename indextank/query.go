package indextank

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Provides an interface to build a search query, which can then be executed by
// calling Index.SearchWithQuery()
type Query interface {
	Start(int)
	NumResults(int)
	FetchFields(...string)
	SnippetFields(...string)
	FetchVariables()
	FetchCategories()
	ScoringFunction(int) Query
	QueryVariable(int, float64)
	QueryVariables(map[int]float64)
	DocumentVariableFilter(variable int, floor, ceil float64)
	FunctionFilter(variable int, floor, ceil float64)
	CategoryFilter(filters map[string][]string)
	ToQueryParams() string
}

type varRange struct {
	id    int
	floor float64
	ceil  float64
}

type queryState struct {
	queryString     string
	start           int
	length          int
	scoringFunction int
	fetchFields     []string
	snippetFields   []string
	queryVariables  map[int]float64
	fetchCategories bool
	fetchVariables  bool
	docvarFilters []varRange
	functionFilters []varRange
	categoryFilters map[string][]string
}

// Returns a Query for a given string.
func QueryForString(s string) Query {
	query := queryState{
		queryString:     s,
		length:          10,
		queryVariables:  map[int]float64{},
		categoryFilters: map[string][]string{},
	}
	return &query
}

func (q *queryState) Start(start int) {
	q.start = start
}

func (q *queryState) NumResults(length int) {
	q.length = length
}

func (q *queryState) FetchFields(fields ...string) {
	q.fetchFields = fields
}

func (q *queryState) SnippetFields(fields ...string) {
	q.snippetFields = fields
}

func (q *queryState) FetchVariables() {
	q.fetchVariables = true
}

func (q *queryState) FetchCategories() {
	q.fetchCategories = true
}

func (q *queryState) ScoringFunction(function int) Query {
	q.scoringFunction = function
	return q
}

func (q *queryState) QueryVariables(variables map[int]float64) {
	q.queryVariables = variables
}

func (q *queryState) QueryVariable(variable int, val float64) {
	q.queryVariables[variable] = val
}

func (q *queryState) DocumentVariableFilter(variable int, floor, ceil float64) {
	q.docvarFilters = append(q.docvarFilters, varRange{variable, floor, ceil})
}

func (q *queryState) FunctionFilter(variable int, floor, ceil float64) {
	q.functionFilters = append(q.functionFilters, varRange{variable, floor, ceil})
}

func (q *queryState) CategoryFilter(filters map[string][]string) {
	// todo
	// if len(filters) > 0 {
	//   q.categoryFilters.putAll(filters)
	// }
	for k, v := range filters {
		q.categoryFilters[k] = v
	}
}

func (q *queryState) String() string {
	return q.ToQueryParams()
}

func (q *queryState) ToQueryParams() string {
	params := map[string]string{}
	params["q"] = q.queryString
	s := "q=" + url.QueryEscape(q.queryString)
	if q.start > 0 {
		params["start"] = strconv.Itoa(q.start)
		s += "&start=" + strconv.Itoa(q.start)
	}
	params["len"] = strconv.Itoa(q.length)
	s += "&len=" + strconv.Itoa(q.length)
	if q.scoringFunction > 0 {
		params["function"] = strconv.Itoa(q.scoringFunction)
		s += "&function=" + strconv.Itoa(q.scoringFunction)
	}
	if q.snippetFields != nil && len(q.snippetFields) > 0 {
		params["snippet"] = strings.Join(q.snippetFields, ",")
		s += "&snippet=" + url.QueryEscape(strings.Join(q.snippetFields, ","))
	}
	if q.fetchFields != nil && len(q.fetchFields) > 0 {
		params["fetch"] = strings.Join(q.fetchFields, ",")
		s += "&fetch=" + url.QueryEscape(strings.Join(q.fetchFields, ","))
	}
	if len(q.queryVariables) > 0 {
		for k, v := range q.queryVariables {
			params["var"+strconv.Itoa(k)] = fmt.Sprintf("%f", v)
			// todo - format these the best way we can
			//s += "&var" + strconv.Itoa(k) + "=" + fmt.Sprintf("%f",v)
			s += "&var" + strconv.Itoa(k) + "=" + strconv.FormatFloat(v, 'g', -1, 64)
		}
	}
	if q.fetchVariables {
		params["fetch_variables"] = "*"
		s += "&fetch_variables=" + url.QueryEscape("*")
	}
	if q.fetchCategories {
		params["fetch_categories"] = "*"
		s += "&fetch_categories=" + url.QueryEscape("*")
	}

	if len(q.categoryFilters) > 0 {
		val, err := json.Marshal(q.categoryFilters)
		if err != nil {
			fmt.Printf("Error marshalling category filters: %v\n", err)
		}
		params["category_filters"] = string(val)
		s += "&category_filters" + url.QueryEscape(string(val))
	}

	if len(q.docvarFilters) > 0 {
		fmt.Printf("*** Adding DocvarFilters: %v\n", q.docvarFilters)
		rangeParams := formatRangeParam(q.docvarFilters)
		for k, v := range rangeParams {
			params["filter_docvar"+k] = v
		}
		/*
			for _, v := range q.docvarFilters {
				k := "filter_docvar" + strconv.Itoa(v.id)
				newValue := strconv.FormatFloat(v.floor, 'g', -1, 64) + ":" + strconv.FormatFloat(v.ceil, 'g', -1, 64)
				// was fmt.Sprintf("%f:%f", v.floor, v.ceil)
				totalValue := newValue
				if totalValue, ok := params[k]; ok {
					totalValue += "," + newValue
				}

				fmt.Printf("docvar %d: [%v,%v]\n", v.id, v.floor, v.ceil)
				//s += "&filter_docvar" + strconv.Itoa(k) + "=" + fmt.Sprintf("%f:%f", v.floor, v.ceil)
				//params["filter_docvar"+strconv.Itoa(k)] = fmt.Sprintf("%f:%f", v.floor, v.ceil)
				params[k] = totalValue
			} */
	}

	if len(q.functionFilters) > 0 {
		fmt.Printf("*** Adding FunctionFilters: %v\n", q.functionFilters)
		rangeParams := formatRangeParam(q.functionFilters)
		for k, v := range rangeParams {
			params["filter_function"+k] = v
		}
	}

	// todo: build a param map[string]string first, convert it to url params in a 2nd step to shorten code
	// on the other hand, we lose explicit ordering if we do this.
	s = toQueryString(params)

	return s
}

func formatRangeParam(ranges []varRange) map[string]string {
	params := make(map[string]string)

	for _, v := range ranges {
		//k := prefix + strconv.Itoa(v.id)
		k := strconv.Itoa(v.id)
		// todo handle NEGATIVE_INFINITY and POSITIVE_INFINITY, replace with "*"
		newValue := strconv.FormatFloat(v.floor, 'g', -1, 64) + ":" + strconv.FormatFloat(v.ceil, 'g', -1, 64)
		totalValue := newValue
		if totalValue, ok := params[k]; ok {
			totalValue += "," + newValue
		}
		fmt.Printf("range_var %d: [%v,%v]\n", v.id, v.floor, v.ceil)
		//s += "&filter_docvar" + strconv.Itoa(k) + "=" + fmt.Sprintf("%f:%f", v.floor, v.ceil)
		//params["filter_docvar"+strconv.Itoa(k)] = fmt.Sprintf("%f:%f", v.floor, v.ceil)
		params[k] = totalValue
	}
	return params
}
