// backend/internal/beylascrape/scrape.go
package beylascrape

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
