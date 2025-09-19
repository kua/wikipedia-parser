package wikihtml

import (
	"strings"
	"testing"
)

func TestConvertInternalAndExternalLinks(t *testing.T) {
	text := "See [[Foo Bar|Foo]] and [[Baz#Section]] plus [https://example.com Example]"
	html := Convert("en", text)
	wantInternal := `<a href="https://en.wikipedia.org/wiki/Foo_Bar">Foo</a>`
	if html == "" || !contains(html, wantInternal) {
		t.Fatalf("missing internal link: %s", html)
	}
	wantFragment := `<a href="https://en.wikipedia.org/wiki/Baz#Section">Baz#Section</a>`
	if !contains(html, wantFragment) {
		t.Fatalf("missing fragment link: %s", html)
	}
	wantExternal := `<a href="https://example.com">Example</a>`
	if !contains(html, wantExternal) {
		t.Fatalf("missing external link: %s", html)
	}
}

func TestConvertNewlines(t *testing.T) {
	html := Convert("en", "Line1\nLine2")
	if html != "Line1<br/>Line2" {
		t.Fatalf("unexpected newline conversion: %s", html)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
