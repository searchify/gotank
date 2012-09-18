package indextank

type deleteResults struct {
	hasErrors bool
	results []bool
	errors map[int]string
	elements []string
	failed []string
}

// Returns the results of a call to DeleteDocuments batch call
type BulkDeleteResults interface {
	HasErrors() bool
	GetResult(position int) bool
	GetDocid(position int) string
	GetErrorMessage(position int) (message string, ok bool)
	GetFailedDocids() []string
}

type addResults struct {
	hasErrors bool
	results []bool
	errors map[int]string
	elements []Document
	failed []Document
}

// Returns the results of a call to AddDocuments batch call
type BatchResults interface {
	HasErrors() bool
	GetResult(position int) bool
	GetDocument(position int) Document
	GetErrorMessage(position int) (message string, ok bool)
	GetFailedDocuments() []Document
}

// Bulk delete results
func newBulkResults(documentIds []string, r []deleteResult) (BulkDeleteResults) {
	if len(documentIds) != len(r) {
		panic("Something is wrong, len(documentIds) != len(r) in newBulkResults")
	}

	errorFlag := false
	n := len(documentIds)
	results := make([]bool, n, n)
	errors := map[int]string{}
	failed := make([]string, 0)

	for i,v := range r {
		results[i] = v.Deleted
		if !v.Deleted {
			errorFlag = true
			failed = append(failed, documentIds[i])
			errors[i] = v.Error
		}
	}

	return &deleteResults{
		hasErrors: errorFlag,
	    results: results,
		errors: errors,
		elements: documentIds,
		failed: failed,
	}
}

func (r *deleteResults) HasErrors() bool {
	return r.hasErrors
}

func (r *deleteResults) GetResult(i int) bool {
	return r.results[i]
}

func (r *deleteResults) GetDocid(i int) string {
	return r.elements[i]
}

func (r *deleteResults) GetErrorMessage(i int) (val string, ok bool) {
	val, ok = r.errors[i]
	return
}

func (r *deleteResults) GetFailedDocids() []string {
	return r.failed
}

// Batch add results
func newBatchResults(documents []Document, r []addResult) (BatchResults) {
	if len(documents) != len(r) {
		panic("Something is wrong, len(documents) != len(r) in newBatchResults")
	}

	errorFlag := false
	n := len(documents)
	results := make([]bool, n, n)
	errors := map[int]string{}
	failed := make([]Document, 0)

	for i,v := range r {
		doc := documents[i]
		results[i] = v.Added
		if !v.Added {
			errorFlag = true
			failed = append(failed, doc)
			errors[i] = v.Error
		}
	}

	return &addResults{
		hasErrors: errorFlag,
		results: results,
		errors: errors,
		elements: documents,
		failed: failed,
	}
}

func (r *addResults) HasErrors() bool {
	return r.hasErrors
}

func (r *addResults) GetResult(i int) bool {
	return r.results[i]
}

func (r *addResults) GetDocument(i int) Document {
	return r.elements[i]
}

func (r *addResults) GetErrorMessage(i int) (val string, ok bool) {
	val, ok = r.errors[i]
	return
}

func (r *addResults) GetFailedDocuments() []Document {
	return r.failed
}
