package http_test

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"sync"
	"testing"

	ferretnet "github.com/MontFerret/ferret/v2/pkg/net"
	nethttp "github.com/MontFerret/ferret/v2/pkg/net/http"
	"github.com/MontFerret/ferret/v2/pkg/runtime"
	stdlibhttp "github.com/MontFerret/ferret/v2/pkg/stdlib/io/net/http"
)

type (
	scriptedResponse struct {
		statusCode int
		body       string
		finalURL   string
		err        error
	}

	scriptedHTTPClient struct {
		mu        sync.Mutex
		responses map[string]scriptedResponse
		calls     []string
		headers   []nethttp.Headers
	}
)

func newScriptedClient(responses map[string]scriptedResponse) *scriptedHTTPClient {
	return &scriptedHTTPClient{responses: responses}
}

func (c *scriptedHTTPClient) Do(_ context.Context, req *nethttp.Request) (*nethttp.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, req.URL)
	c.headers = append(c.headers, req.Headers)

	scripted, ok := c.responses[req.URL]
	if !ok {
		return nil, errors.New("scripted client: unexpected url " + req.URL)
	}

	if scripted.err != nil {
		return nil, scripted.err
	}

	status := scripted.statusCode
	if status == 0 {
		status = 200
	}

	return &nethttp.Response{
		StatusCode: status,
		Status:     "200 OK",
		Body:       []byte(scripted.body),
		URL:        scripted.finalURL,
	}, nil
}

func (c *scriptedHTTPClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.calls)
}

func (c *scriptedHTTPClient) recordedCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.calls...)
}

func pagesContext(t *testing.T, client nethttp.Client) context.Context {
	t.Helper()

	network, err := ferretnet.New(ferretnet.WithHTTPClient(client))
	if err != nil {
		t.Fatalf("create network: %v", err)
	}

	return ferretnet.WithNetwork(context.Background(), network)
}

func pagesParams(url string) runtime.Value {
	return runtime.NewObjectWith(map[string]runtime.Value{
		"url": runtime.NewString(url),
	})
}

func drainIterable(t *testing.T, ctx context.Context, iterable runtime.Iterable) ([]runtime.Value, error) {
	t.Helper()

	iter, err := iterable.Iterate(ctx)
	if err != nil {
		return nil, err
	}

	var pages []runtime.Value

	for {
		value, _, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return pages, err
		}

		pages = append(pages, value)
	}

	if closer, ok := iter.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return pages, err
		}
	}

	return pages, nil
}

func asPage(t *testing.T, value runtime.Value) *runtime.Object {
	t.Helper()

	page, ok := value.(*runtime.Object)
	if !ok {
		t.Fatalf("expected page object, got %T", value)
	}

	return page
}

func pageItems(t *testing.T, ctx context.Context, page *runtime.Object) *runtime.Array {
	t.Helper()

	value, err := page.Get(ctx, runtime.String("items"))
	if err != nil {
		t.Fatalf("get items: %v", err)
	}

	items, ok := value.(*runtime.Array)
	if !ok {
		t.Fatalf("expected items array, got %T", value)
	}

	return items
}

func TestPAGESLazyLoading(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1,2],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[3],"next":null}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	if client.callCount() != 0 {
		t.Fatalf("expected no request before iteration, got %d", client.callCount())
	}

	iter, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if client.callCount() != 0 {
		t.Fatalf("expected no request before first Next, got %d", client.callCount())
	}

	first, _, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("first next: %v", err)
	}

	if client.callCount() != 1 {
		t.Fatalf("expected one request after first Next, got %d", client.callCount())
	}

	items := pageItems(t, ctx, asPage(t, first))
	length, err := items.Length(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if length != 2 {
		t.Fatalf("expected 2 items on first page, got %d", length)
	}

	second, _, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("second next: %v", err)
	}

	if client.callCount() != 2 {
		t.Fatalf("expected second request only when next page consumed, got %d", client.callCount())
	}

	items = pageItems(t, ctx, asPage(t, second))
	length, _ = items.Length(ctx)

	if length != 1 {
		t.Fatalf("expected 1 item on second page, got %d", length)
	}

	if _, _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	if client.callCount() != 2 {
		t.Fatalf("expected no request after exhaustion, got %d", client.callCount())
	}

	if closer, ok := iter.(io.Closer); ok {
		_ = closer.Close()
	}
}

func TestPAGESPreservesOrderAndDuplicates(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":["a","b","a"],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":["b","c"]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	pages, err := drainIterable(t, ctx, iterable.(runtime.Iterable))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	if len(pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(pages))
	}

	var got []string

	for _, value := range pages {
		items := pageItems(t, ctx, asPage(t, value))
		length, _ := items.Length(ctx)

		for i := range int(length) {
			item, err := items.At(ctx, runtime.Int(i))
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, item.String())
		}
	}

	want := []string{"a", "b", "a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestPAGESEmptyPageFollowsNext(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[],"next":"https://api.test/p3"}`},
		"https://api.test/p3": {body: `{"items":[1]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	pages, err := drainIterable(t, ctx, iterable.(runtime.Iterable))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	if len(pages) != 3 {
		t.Fatalf("expected empty pages to still be yielded, got %d", len(pages))
	}

	if client.callCount() != 3 {
		t.Fatalf("expected 3 requests, got %d", client.callCount())
	}
}

func TestPAGESMissingAndNullNextEnds(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/nullable": {body: `{"items":[1],"next":null}`},
		"https://api.test/missing":  {body: `{"items":[1]}`},
	})

	ctx := pagesContext(t, client)

	for _, url := range []string{"https://api.test/nullable", "https://api.test/missing"} {
		iterable, err := stdlibhttp.PAGES(ctx, pagesParams(url))
		if err != nil {
			t.Fatalf("pages: %v", err)
		}

		pages, err := drainIterable(t, ctx, iterable.(runtime.Iterable))
		if err != nil {
			t.Fatalf("drain %s: %v", url, err)
		}

		if len(pages) != 1 {
			t.Fatalf("%s: expected 1 page, got %d", url, len(pages))
		}
	}

	if client.callCount() != 2 {
		t.Fatalf("expected 2 requests, got %d", client.callCount())
	}
}

func TestPAGESRelativeNextResolvedAgainstFinalURL(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/v1/start": {
			body:     `{"items":[1],"next":"next-page"}`,
			finalURL: "https://api.test/v1/redirected/",
		},
		"https://api.test/v1/redirected/next-page": {body: `{"items":[2]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/v1/start"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	pages, err := drainIterable(t, ctx, iterable.(runtime.Iterable))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	if len(pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(pages))
	}

	calls := client.recordedCalls()
	if calls[1] != "https://api.test/v1/redirected/next-page" {
		t.Fatalf("expected relative next resolved against final response url, got %q", calls[1])
	}
}

func TestPAGESRelativeNextFallsBackToRequestURL(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/v1/start": {
			body:     `{"items":[1],"next":"next-page"}`,
			finalURL: "",
		},
		"https://api.test/v1/next-page": {body: `{"items":[2]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/v1/start"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	if _, err := drainIterable(t, ctx, iterable.(runtime.Iterable)); err != nil {
		t.Fatalf("drain: %v", err)
	}

	calls := client.recordedCalls()
	if calls[1] != "https://api.test/v1/next-page" {
		t.Fatalf("expected fallback resolution against request url, got %q", calls[1])
	}
}

func TestPAGESRepeatedPageURL(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[2],"next":"https://api.test/p1"}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	iter, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := iter.Next(ctx); err != nil {
		t.Fatalf("first page: %v", err)
	}

	if _, _, err := iter.Next(ctx); err != nil {
		t.Fatalf("second page: %v", err)
	}

	if _, _, err := iter.Next(ctx); err == nil {
		t.Fatal("expected repeated page url error")
	}

	if client.callCount() != 2 {
		t.Fatalf("expected no request for repeated url, got %d", client.callCount())
	}
}

func TestPAGESIndependentIterations(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[2]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	first, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := first.Next(ctx); err != nil {
		t.Fatalf("first iter first page: %v", err)
	}

	if _, _, err := first.Next(ctx); err != nil {
		t.Fatalf("first iter second page: %v", err)
	}

	if _, _, err := first.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected first iter EOF, got %v", err)
	}

	second, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	page, _, err := second.Next(ctx)
	if err != nil {
		t.Fatalf("independent iter must restart from first page: %v", err)
	}

	items := pageItems(t, ctx, asPage(t, page))
	firstItem, err := items.At(ctx, runtime.ZeroInt)
	if err != nil {
		t.Fatal(err)
	}

	if firstItem.String() != "1" {
		t.Fatalf("expected independent traversal to restart at page 1, got item %s", firstItem)
	}

	if _, _, err := second.Next(ctx); err != nil {
		t.Fatalf("second iter second page: %v", err)
	}

	if _, _, err := second.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF on second iter, got %v", err)
	}

	if client.callCount() != 4 {
		t.Fatalf("expected each iteration to fetch independently (4 requests), got %d", client.callCount())
	}
}

func TestPAGESCloseStopsFetching(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {body: `{"items":[2],"next":"https://api.test/p3"}`},
		"https://api.test/p3": {body: `{"items":[3]}`},
	})

	ctx := pagesContext(t, client)

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	iter, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := iter.Next(ctx); err != nil {
		t.Fatalf("first page: %v", err)
	}

	closer := iter.(io.Closer)
	if err := closer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := closer.Close(); err != nil {
		t.Fatalf("close must be idempotent: %v", err)
	}

	if _, _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after close, got %v", err)
	}

	if client.callCount() != 1 {
		t.Fatalf("expected no requests after close, got %d", client.callCount())
	}
}

func TestPAGESCancellationStopsFetching(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1],"next":"https://api.test/p2"}`},
		"https://api.test/p2": {err: context.Canceled},
	})

	ctx, cancel := context.WithCancel(pagesContext(t, client))
	defer cancel()

	iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/p1"))
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	iter, err := iterable.(runtime.Iterable).Iterate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := iter.Next(ctx); err != nil {
		t.Fatalf("first page: %v", err)
	}

	cancel()

	if _, _, err := iter.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation to propagate, got %v", err)
	}
}

func TestPAGESPropagatesErrors(t *testing.T) {
	t.Run("non-2xx status", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {statusCode: 500, body: `boom`},
		})

		ctx := pagesContext(t, client)

		iterable, err := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		if err != nil {
			t.Fatalf("pages: %v", err)
		}

		iter, err := iterable.(runtime.Iterable).Iterate(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected non-2xx to error instead of ending")
		}
	})

	t.Run("malformed JSON", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {body: `{not json`},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected malformed JSON to error")
		}
	})

	t.Run("items field missing", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {body: `{"next":null}`},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected missing items to error")
		}
	})

	t.Run("items field wrong type", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {body: `{"items":"nope"}`},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected non-array items to error")
		}
	})

	t.Run("next field wrong type", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {body: `{"items":[],"next":42}`},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected non-string next to error")
		}
	})

	t.Run("root not an object", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {body: `[1,2,3]`},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected non-object root to error")
		}
	})

	t.Run("network failure", func(t *testing.T) {
		client := newScriptedClient(map[string]scriptedResponse{
			"https://api.test/bad": {err: errors.New("connection refused")},
		})

		ctx := pagesContext(t, client)

		iterable, _ := stdlibhttp.PAGES(ctx, pagesParams("https://api.test/bad"))
		iter, _ := iterable.(runtime.Iterable).Iterate(ctx)

		if _, _, err := iter.Next(ctx); err == nil {
			t.Fatal("expected network failure to propagate")
		}
	})
}

func TestPAGESHeadersForwarded(t *testing.T) {
	client := newScriptedClient(map[string]scriptedResponse{
		"https://api.test/p1": {body: `{"items":[1]}`},
	})

	network, err := ferretnet.New(ferretnet.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}

	ctx := ferretnet.WithNetwork(context.Background(), network)

	params := runtime.NewObjectWith(map[string]runtime.Value{
		"url": runtime.NewString("https://api.test/p1"),
		"headers": runtime.NewObjectWith(map[string]runtime.Value{
			"X-Token": runtime.NewString("secret"),
		}),
	})

	iterable, err := stdlibhttp.PAGES(ctx, params)
	if err != nil {
		t.Fatalf("pages: %v", err)
	}

	if _, err := drainIterable(t, ctx, iterable.(runtime.Iterable)); err != nil {
		t.Fatalf("drain: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	if len(client.headers) != 1 || stdhttp.Header(client.headers[0]).Get("X-Token") != "secret" {
		t.Fatalf("expected headers forwarded to every request, got %#v", client.headers)
	}
}

func TestPAGESRejectsInvalidArguments(t *testing.T) {
	ctx := pagesContext(t, newScriptedClient(nil))

	if _, err := stdlibhttp.PAGES(ctx, runtime.NewString("not-a-map")); err == nil {
		t.Fatal("expected non-map argument to fail")
	}

	if _, err := stdlibhttp.PAGES(ctx, runtime.NewObject()); err == nil {
		t.Fatal("expected missing url to fail")
	}
}
