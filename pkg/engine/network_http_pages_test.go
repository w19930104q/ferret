package engine

import (
	"context"
	"errors"
	stdhttp "net/http"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/ferret/v2/pkg/encoding"
	ferretnet "github.com/MontFerret/ferret/v2/pkg/net"
	nethttp "github.com/MontFerret/ferret/v2/pkg/net/http"
	"github.com/MontFerret/ferret/v2/pkg/source"
)

type (
	pageResponse struct {
		statusCode int
		body       string
		finalURL   string
		err        error
	}

	pagedHTTPClient struct {
		mu        sync.Mutex
		responses map[string]pageResponse
		calls     []string
	}

	blockingPagesClient struct {
		started chan context.Context
	}
)

func (c *pagedHTTPClient) Do(_ context.Context, req *nethttp.Request) (*nethttp.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, req.URL)

	scripted, ok := c.responses[req.URL]
	if !ok {
		return nil, errors.New("paged client: unexpected url " + req.URL)
	}

	if scripted.err != nil {
		return nil, scripted.err
	}

	status := scripted.statusCode
	if status == 0 {
		status = stdhttp.StatusOK
	}

	return &nethttp.Response{
		StatusCode: status,
		Status:     stdhttp.StatusText(status),
		Body:       []byte(scripted.body),
		URL:        scripted.finalURL,
	}, nil
}

func (c *pagedHTTPClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.calls)
}

func (c *pagedHTTPClient) recordedCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.calls...)
}

func (c *blockingPagesClient) Do(ctx context.Context, _ *nethttp.Request) (*nethttp.Response, error) {
	c.started <- ctx
	<-ctx.Done()

	return nil, ctx.Err()
}

func newPagedEngine(t *testing.T, client nethttp.Client) *Engine {
	t.Helper()

	network := mustNewTestNetwork(t, ferretnet.WithHTTPClient(client))
	engine := mustNewEngine(t, WithNetwork(network))

	return engine
}

func TestHTTPPagesFlattensItemsInFQL(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[{"id":1},{"id":2}],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[{"id":3}]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
		RETURN page.items
))
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `[{"id":1},{"id":2},{"id":3}]` {
		t.Fatalf("unexpected output: %s", got)
	}

	if client.callCount() != 2 {
		t.Fatalf("expected one request per page, got %d", client.callCount())
	}
}

func TestHTTPPagesLazyUntilFirstConsumption(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[2]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	// The iterable is created and passed through a LET without being consumed.
	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
LET pages = IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
RETURN TRUE
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `true` {
		t.Fatalf("unexpected output: %s", got)
	}

	if client.callCount() != 0 {
		t.Fatalf("expected no request until iteration starts, got %d", client.callCount())
	}
}

func TestHTTPPagesLimitStopsFetching(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[1,2],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[3,4],"next":"https://api.test/p3"}`},
		"https://api.test/p3": {body: `{"items":[5]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
	LIMIT 1
	RETURN page.items
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `[[1,2]]` {
		t.Fatalf("unexpected output: %s", got)
	}

	// The FOR/LIMIT backedge evaluates ITNEXT before ITLIM, so the VM probes
	// one extra item while stopping; pages never fetches beyond that probe and
	// the iterator Close emitted at loop end blocks every later request.
	if client.callCount() != 2 {
		t.Fatalf("expected LIMIT to stop after one page plus the VM boundary probe, got %d", client.callCount())
	}
}

func TestHTTPPagesLimitStopsWithinFirstPage(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[1,2,3],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[4]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
		LIMIT 1
		RETURN page.items
))
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `[1,2,3]` {
		t.Fatalf("unexpected output: %s", got)
	}

	// LIMIT 1 ends the page loop after the first page; the VM's backedge still
	// performs one ITNEXT boundary probe, so page two is fetched once but never
	// processed, and loop-end Close prevents any further request.
	if client.callCount() != 2 {
		t.Fatalf("expected LIMIT to stop after the first page plus one boundary probe, got %d", client.callCount())
	}
}

func TestHTTPPagesFollowsRedirectedRelativeNext(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/v1/start": {
			body:     `{"items":[1],"next":"next-page"}`,
			finalURL: "https://api.test/v1/redirected/",
		},
		"https://api.test/v1/redirected/next-page": {body: `{"items":[2]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/v1/start" })
		RETURN page.items
))
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `[1,2]` {
		t.Fatalf("unexpected output: %s", got)
	}

	calls := client.recordedCalls()
	if len(calls) != 2 || calls[1] != "https://api.test/v1/redirected/next-page" {
		t.Fatalf("relative next must resolve against the final response url, got %v", calls)
	}
}

func TestHTTPPagesEmptyPageFollowsNext(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[],"next":"https://api.test/p3"}`},
		"https://api.test/p3": {body: `{"items":["done"]}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	out, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
		RETURN page.items
))
`))
	if err != nil {
		t.Fatal(err)
	}

	if got := string(out.Data); got != `["done"]` {
		t.Fatalf("unexpected output: %s", got)
	}

	if client.callCount() != 3 {
		t.Fatalf("expected empty pages to keep following next, got %d", client.callCount())
	}
}

func TestHTTPPagesRepeatedURLSurfacesError(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[2],"next":"https://api.test/p1"}`},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	_, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
		RETURN page.items
))
`))
	if err == nil {
		t.Fatal("expected repeated page url to fail the query")
	}

	if client.callCount() != 2 {
		t.Fatalf("expected no request for the repeated url, got %d", client.callCount())
	}
}

func TestHTTPPagesErrorDoesNotLookLikeEnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		page pageResponse
	}{
		{name: "non-2xx", page: pageResponse{statusCode: 503, body: `busy`}},
		{name: "bad json", page: pageResponse{body: `{broken`}},
		{name: "missing items", page: pageResponse{body: `{"next":null}`}},
		{name: "wrong items type", page: pageResponse{body: `{"items":1}`}},
		{name: "wrong next type", page: pageResponse{body: `{"items":[],"next":1}`}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := &pagedHTTPClient{responses: map[string]pageResponse{
				"https://api.test/fail": tc.page,
			}}
			engine := newPagedEngine(t, client)
			defer func() { _ = engine.Close() }()

			_, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/fail" })
		RETURN page.items
))
`))
			if err == nil {
				t.Fatal("expected page failure to propagate as a query error")
			}
		})
	}
}

func TestHTTPPagesNetworkFailurePropagates(t *testing.T) {
	t.Parallel()

	client := &pagedHTTPClient{responses: map[string]pageResponse{
		"https://api.test/down": {err: errors.New("connection refused")},
	}}
	engine := newPagedEngine(t, client)
	defer func() { _ = engine.Close() }()

	_, err := collectEngine(engine, context.Background(), source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/down" })
		RETURN page.items
))
`))
	if err == nil {
		t.Fatal("expected network failure to propagate as a query error")
	}
}

func TestHTTPPagesCancellationStopsRequests(t *testing.T) {
	t.Parallel()

	blocking := &blockingPagesClient{started: make(chan context.Context, 1)}
	engine := newPagedEngine(t, blocking)
	defer func() { _ = engine.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run executes the FLATTEN loop synchronously, which drives the page
	// iterator; it is run in a goroutine so the test can cancel while its first
	// request is blocked. Run only reports admission failures; the terminal
	// cancellation error surfaces through Collect on the returned handle.
	finished := make(chan struct {
		output encoding.Output
		err    error
	}, 1)

	go func() {
		output, runErr := engine.Run(ctx, source.NewAnonymous(`
RETURN FLATTEN((
	FOR page IN IO::NET::HTTP::PAGES({ url: "https://api.test/p1" })
		RETURN page.items
))
`))
		finished <- struct {
			output encoding.Output
			err    error
		}{output: output, err: runErr}
	}()

	select {
	case received := <-blocking.started:
		if received == nil {
			t.Fatal("http client received a nil context")
		}

		// The engine derives its own execution context; canceling the parent
		// must cancel the request context observed by the injected client.
		cancel()

		select {
		case <-received.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("request context did not observe parent cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page request never reached the http client")
	}

	result := <-finished
	if result.err != nil {
		t.Fatalf("execution admission failed: %v", result.err)
	}

	defer func() { _ = result.output.Close() }()

	content, err := result.output.Collect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from collection, got content=%v err=%v", content, err)
	}

	if content != nil {
		t.Fatalf("canceled query must not produce content, got %v", content)
	}
}
