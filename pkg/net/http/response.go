package http

// Response contains the materialized HTTP response.
type Response struct {
	Headers Headers `json:"headers"`
	Status  string  `json:"status"`
	Body    []byte  `json:"body,omitempty"`
	// URL is the final response URL after redirects. Custom clients that do not
	// populate it leave callers without redirect resolution information.
	URL        string `json:"url,omitempty"`
	StatusCode int    `json:"statusCode"`
}
