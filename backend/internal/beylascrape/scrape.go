// backend/internal/beylascrape/scrape.go
package beylascrape

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// scrapeOne fetches one Beyla pod's Prometheus /metrics text over HTTP.
func (s *Source) scrapeOne(ctx context.Context, podIP string) (string, error) {
	url := fmt.Sprintf("http://%s:%d/metrics", podIP, s.cfg.Port)

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("beylascrape: building request for %s: %w", url, err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("beylascrape: scraping %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("beylascrape: scraping %s: unexpected status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("beylascrape: reading response from %s: %w", url, err)
	}
	return string(body), nil
}

// cacheEntry is one pod's fetched /metrics text. done is closed once text
// and err are set; until then other callers wait on it (in-flight sharing).
type cacheEntry struct {
	done chan struct{}
	text string
	err  error
	at   time.Time
}

// fetch returns pod's /metrics text, through the per-pod cache when
// Config.CacheTTL > 0. The returned time is when the text was actually fetched (for a cache
// hit, up to CacheTTL ago). Only successful fetches are reused after they
// complete; a failed one is retried by the next caller.
func (s *Source) fetch(ctx context.Context, pod podTarget) (string, time.Time, error) {
	if s.cfg.CacheTTL <= 0 {
		text, err := s.scrapeOne(ctx, pod.ip)
		return text, time.Now(), err
	}
	key := pod.key + "@" + pod.ip
	s.cacheMu.Lock()
	if e, ok := s.cache[key]; ok {
		select {
		case <-e.done:
			if e.err == nil && time.Since(e.at) < s.cfg.CacheTTL {
				s.cacheMu.Unlock()
				return e.text, e.at, nil
			}
		default: // in flight: share it
			s.cacheMu.Unlock()
			select {
			case <-e.done:
				return e.text, e.at, e.err
			case <-ctx.Done():
				return "", time.Time{}, ctx.Err()
			}
		}
	}
	e := &cacheEntry{done: make(chan struct{})}
	s.cache[key] = e
	// Drop entries that can no longer be served (pods that went away).
	for k, old := range s.cache {
		select {
		case <-old.done:
			if time.Since(old.at) >= s.cfg.CacheTTL {
				delete(s.cache, k)
			}
		default:
		}
	}
	s.cacheMu.Unlock()

	e.text, e.err = s.scrapeOne(ctx, pod.ip)
	e.at = time.Now()
	close(e.done)
	return e.text, e.at, e.err
}
