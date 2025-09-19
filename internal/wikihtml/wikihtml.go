package wikihtml

import (
	"html"
	"net/url"
	"strings"
)

func Convert(lang, text string) string {
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	var b strings.Builder
	for i := 0; i < len(text); {
		rest := text[i:]
		switch {
		case strings.HasPrefix(rest, "[["):
			end := strings.Index(rest, "]]")
			if end == -1 {
				writeEscaped(&b, rest)
				i = len(text)
				continue
			}
			inner := rest[2:end]
			if link := internalLink(lang, inner); link != "" {
				b.WriteString(link)
			} else {
				writeEscaped(&b, rest[:end+2])
			}
			i += end + 2
		case strings.HasPrefix(rest, "["):
			end := strings.Index(rest, "]")
			if end == -1 {
				writeEscaped(&b, rest)
				i = len(text)
				continue
			}
			inner := rest[1:end]
			if link := externalLink(inner); link != "" {
				b.WriteString(link)
			} else {
				writeEscaped(&b, rest[:end+1])
			}
			i += end + 1
		default:
			next := strings.IndexAny(rest, "[<>&")
			if next == -1 {
				writeEscaped(&b, rest)
				i = len(text)
			} else if next == 0 {
				writeEscaped(&b, rest[:1])
				i++
			} else {
				writeEscaped(&b, rest[:next])
				i += next
			}
		}
	}
	return strings.ReplaceAll(b.String(), "\n", "<br/>")
}

func internalLink(lang, inner string) string {
	parts := strings.SplitN(inner, "|", 2)
	target := strings.TrimSpace(parts[0])
	original := target
	if target == "" {
		return ""
	}
	display := ""
	if len(parts) == 2 {
		display = strings.TrimSpace(parts[1])
	}
	fragment := ""
	if idx := strings.Index(target, "#"); idx != -1 {
		fragment = target[idx+1:]
		target = target[:idx]
	}
	target = strings.TrimLeft(target, ":")
	if target == "" {
		return ""
	}
	if display == "" {
		display = original
	}
	href := wikiURL(lang, target, fragment)
	return `<a href="` + href + `">` + html.EscapeString(display) + `</a>`
}

func externalLink(inner string) string {
	fields := strings.Fields(inner)
	if len(fields) == 0 {
		return ""
	}
	href := fields[0]
	if !(strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://")) {
		return ""
	}
	text := strings.TrimSpace(inner[len(href):])
	if text == "" {
		text = href
	}
	return `<a href="` + html.EscapeString(href) + `">` + html.EscapeString(text) + `</a>`
}

func wikiURL(lang, target, fragment string) string {
	segment := normalizeTitle(target)
	u := url.URL{
		Scheme: "https",
		Host:   lang + ".wikipedia.org",
		Path:   "/wiki/" + escapeSegment(segment),
	}
	if fragment != "" {
		u.Fragment = escapeFragment(fragment)
	}
	return u.String()
}

func normalizeTitle(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "\t", "_")
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	return s
}

func escapeSegment(s string) string {
	return strings.ReplaceAll(url.PathEscape(s), "+", "%20")
}

func escapeFragment(s string) string {
	s = normalizeTitle(s)
	return strings.ReplaceAll(url.PathEscape(s), "+", "%20")
}

func writeEscaped(b *strings.Builder, s string) {
	b.WriteString(html.EscapeString(s))
}
