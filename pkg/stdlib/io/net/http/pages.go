package http

import (
	"context"
	"hash/fnv"
	"io"
	nurl "net/url"

	ferretencoding "github.com/MontFerret/ferret/v2/pkg/encoding"
	encodingjson "github.com/MontFerret/ferret/v2/pkg/encoding/json"
	ferretnet "github.com/MontFerret/ferret/v2/pkg/net"
	nethttp "github.com/MontFerret/ferret/v2/pkg/net/http"
	"github.com/MontFerret/ferret/v2/pkg/runtime"
)

type (
	// pageIterable lazily fetches JSON pages over HTTP. Every call to Iterate
	// starts an independent traversal from the initial URL; no request is sent
	// until the produced iterator is first consumed.
	pageIterable struct {
		client  nethttp.Client
		url     string
		headers nethttp.Headers
	}

	// pageIterator walks pages on demand. It keeps only the current page's
	// body and the next URL, so fully consumed pages are released as traversal
	// advances instead of accumulating the whole collection.
	pageIterator struct {
		client    nethttp.Client
		headers   nethttp.Headers
		page      *runtime.Object
		nextURL   string
		seen      map[string]struct{}
		exhausted bool
		closed    bool
		pageIndex int64
	}
)

// pages creates a lazily evaluated iterable of JSON pages.
// The argument must contain a url field and an optional headers map.
// Each yielded page is an object with items (an array) and next (a URL
// string or null/missing, both of which terminate traversal).
// Pages are fetched on demand, so the first GET happens only when iteration
// starts and each following page is requested after the current page is
// consumed. Relative next values are resolved against the final response URL.
// Repeating a page URL fails the traversal instead of looping forever.
//
// FQL usage:
//
//	LET all = FLATTEN((
//		FOR page IN IO::NET::HTTP::PAGES({
//			url: "https://example.test/api/items",
//			headers: { "Accept": "application/json" }
//		})
//			RETURN page.items
//	))
//
// @param params {Map} Parameters containing url and optional headers fields.
// @return {Iterable} Lazy iterable of page objects.
func PAGES(ctx context.Context, arg runtime.Value) (runtime.Value, error) {
	params, err := runtime.CastArg[runtime.Map](arg, 0)
	if err != nil {
		return runtime.None, err
	}

	p, err := newParamsFrom(ctx, params)
	if err != nil {
		return runtime.None, err
	}

	client, err := ferretnet.HTTPClientFrom(ctx)
	if err != nil {
		return runtime.None, err
	}

	headers, err := headersFromMap(ctx, p.Headers)
	if err != nil {
		return runtime.None, err
	}

	return &pageIterable{
		client:  client,
		url:     p.URL.String(),
		headers: headers,
	}, nil
}

func (p *pageIterable) Type() runtime.Type {
	return runtime.TypeIterable
}

func (p *pageIterable) String() string {
	return "[Iterable]"
}

func (p *pageIterable) Hash() uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte("stdlib.http.pages:"))
	_, _ = hasher.Write([]byte(p.url))

	return hasher.Sum64()
}

// Copy preserves the lazy source. The copy and the original produce
// independent traversals through Iterate and share no per-page state.
func (p *pageIterable) Copy() runtime.Value {
	return p
}

func (p *pageIterable) Iterate(_ context.Context) (runtime.Iterator, error) {
	return &pageIterator{
		client:  p.client,
		headers: p.headers,
		nextURL: p.url,
		seen:    make(map[string]struct{}),
	}, nil
}

// Next yields one page object per call. A page with an empty items array is
// still yielded; its next link decides whether traversal continues.
func (it *pageIterator) Next(ctx context.Context) (runtime.Value, runtime.Value, error) {
	if it.closed || it.exhausted || it.nextURL == "" {
		it.exhausted = true

		return runtime.None, runtime.None, io.EOF
	}

	if err := it.fetch(ctx, it.nextURL); err != nil {
		return runtime.None, runtime.None, err
	}

	if it.nextURL == "" {
		it.exhausted = true
	}

	page := it.page
	key := runtime.NewInt64(it.pageIndex)
	it.pageIndex++

	return page, key, nil
}

// Close stops traversal idempotently. After Close the iterator must not issue
// further requests, including when a FOR loop exits early through LIMIT.
func (it *pageIterator) Close() error {
	it.closed = true
	it.exhausted = true
	it.page = nil

	return nil
}

// fetch retrieves and validates one page and advances the cursor to its items.
func (it *pageIterator) fetch(ctx context.Context, target string) error {
	if _, repeated := it.seen[target]; repeated {
		return runtime.Errorf(runtime.ErrInvalidOperation, "http.pages: repeated page url %q", target)
	}

	it.seen[target] = struct{}{}

	res, err := it.client.Do(ctx, &nethttp.Request{
		Method:  "GET",
		URL:     target,
		Headers: it.headers,
	})
	if err != nil {
		return err
	}

	if res == nil {
		return runtime.Error(runtime.ErrUnexpected, "http response is nil")
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return runtime.Errorf(runtime.ErrUnexpected, "http.pages: unexpected status code %d", res.StatusCode)
	}

	page, err := it.decodePage(ctx, res.Body)
	if err != nil {
		return err
	}

	if err := it.validateItems(ctx, page); err != nil {
		return err
	}

	baseURL := res.URL
	if baseURL == "" {
		// Older injected clients do not report the final response URL; the
		// request URL is the only resolution base available.
		baseURL = target
	}

	next, err := it.pageNext(ctx, page, baseURL)
	if err != nil {
		return err
	}

	it.page = page
	it.nextURL = next

	return nil
}

func (it *pageIterator) decodePage(ctx context.Context, body []byte) (*runtime.Object, error) {
	decoder := ferretencoding.Decoder(encodingjson.Default)
	if selected, resolverErr := ferretencoding.DecoderFrom(ctx, encodingjson.ContentType); resolverErr == nil {
		decoder = selected
	}

	value, err := ferretencoding.DecodeBytes(ctx, decoder, body)
	if err != nil {
		return nil, runtime.Error(err, "http.pages: page body is not valid JSON")
	}

	// The default JSON decoder returns *runtime.Object; context-injected
	// decoders may provide another runtime.Map implementation.
	page, ok := value.(runtime.Map)
	if !ok {
		return nil, runtime.TypeError(runtime.TypeOf(value), runtime.TypeMap)
	}

	object, ok := page.(*runtime.Object)
	if !ok {
		object = runtime.NewObjectOf(0)

		forEachErr := page.ForEach(ctx, func(_ context.Context, v, k runtime.Value) (runtime.Boolean, error) {
			if setErr := object.Set(ctx, k, v); setErr != nil {
				return runtime.False, setErr
			}

			return runtime.True, nil
		})
		if forEachErr != nil {
			return nil, forEachErr
		}
	}

	return object, nil
}

func (it *pageIterator) validateItems(ctx context.Context, page runtime.Map) error {
	itemsKey := runtime.String("items")
	hasItems, err := page.ContainsKey(ctx, itemsKey)
	if err != nil {
		return err
	}

	if !hasItems {
		return runtime.Error(runtime.ErrInvalidArgument, "http.pages: page is missing items field")
	}

	value, err := page.Get(ctx, itemsKey)
	if err != nil {
		return err
	}

	if _, ok := value.(*runtime.Array); !ok {
		return runtime.Errorf(runtime.ErrInvalidType, "http.pages: page items must be an array, but got %s", runtime.TypeName(runtime.TypeOf(value)))
	}

	return nil
}

func (it *pageIterator) pageNext(ctx context.Context, page runtime.Map, baseURL string) (string, error) {
	nextKey := runtime.String("next")
	hasNext, err := page.ContainsKey(ctx, nextKey)
	if err != nil {
		return "", err
	}

	if !hasNext {
		return "", nil
	}

	value, err := page.Get(ctx, nextKey)
	if err != nil {
		return "", err
	}

	if value == runtime.None {
		return "", nil
	}

	next, ok := value.(runtime.String)
	if !ok {
		return "", runtime.Errorf(runtime.ErrInvalidType, "http.pages: page next must be a string or null, but got %s", runtime.TypeName(runtime.TypeOf(value)))
	}

	ref := next.String()
	if ref == "" {
		return "", nil
	}

	base, err := nurl.Parse(baseURL)
	if err != nil {
		return "", runtime.Errorf(err, "http.pages: cannot parse response url %q", baseURL)
	}

	resolved, err := nurl.Parse(ref)
	if err != nil {
		return "", runtime.Errorf(err, "http.pages: cannot parse next url %q", ref)
	}

	return base.ResolveReference(resolved).String(), nil
}
