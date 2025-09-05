package enhanced

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
)

// GCSBatchRequest represents a GCS JSON API batch request
type GCSBatchRequest struct {
	Requests []GCSSubRequest
}

// GCSSubRequest represents a single request within a batch
type GCSSubRequest struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    interface{}       `json:"body,omitempty"`
}

// GCSBatchResponse represents a GCS JSON API batch response
type GCSBatchResponse struct {
	Responses []GCSSubResponse
}

// GCSSubResponse represents a single response within a batch
type GCSSubResponse struct {
	StatusCode int                    `json:"status_code"`
	Headers    map[string]string      `json:"headers,omitempty"`
	Body       map[string]interface{} `json:"body,omitempty"`
	Error      *GCSError              `json:"error,omitempty"`
}

// GCSError represents a GCS API error
type GCSError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason,omitempty"`
}

// GCSBatchDeleter handles native GCS JSON API batch delete operations
type GCSBatchDeleter struct {
	bucket      string
	credentials oauth2.TokenSource
	httpClient  *http.Client
	baseURL     string
	config      *config.GCSConfig
}

// NewGCSBatchDeleter creates a new GCS batch deleter
func NewGCSBatchDeleter(bucket string, tokenSource oauth2.TokenSource, cfg *config.GCSConfig) *GCSBatchDeleter {
	baseURL := "https://storage.googleapis.com"
	if cfg.Endpoint != "" {
		baseURL = strings.TrimSuffix(cfg.Endpoint, "/")
	}

	return &GCSBatchDeleter{
		bucket:      bucket,
		credentials: tokenSource,
		httpClient:  &http.Client{Timeout: 5 * time.Minute},
		baseURL:     baseURL,
		config:      cfg,
	}
}

// DeleteBatch performs a native GCS JSON API batch delete operation
func (d *GCSBatchDeleter) DeleteBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	if len(keys) == 0 {
		return &BatchResult{SuccessCount: 0}, nil
	}

	// GCS batch API supports up to 100 requests per batch
	const maxBatchSize = 100

	if len(keys) <= maxBatchSize {
		return d.deleteSingleBatch(ctx, keys)
	}

	// Split into multiple batches if needed
	return d.deleteMultipleBatches(ctx, keys, maxBatchSize)
}

// deleteSingleBatch deletes a single batch of up to 100 keys
func (d *GCSBatchDeleter) deleteSingleBatch(ctx context.Context, keys []string) (*BatchResult, error) {
	log.Debug().
		Int("key_count", len(keys)).
		Msg("starting GCS JSON API batch delete")

	// Create batch request
	batchRequest := d.createBatchRequest(keys)

	// Send batch request
	batchResponse, err := d.sendBatchRequest(ctx, batchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to send GCS batch request: %w", err)
	}

	// Process batch response
	return d.processBatchResponse(batchResponse, keys)
}

// deleteMultipleBatches handles deletion across multiple batch requests
func (d *GCSBatchDeleter) deleteMultipleBatches(ctx context.Context, keys []string, batchSize int) (*BatchResult, error) {
	var totalSuccess int
	var allFailedKeys []FailedKey
	var allErrors []error

	// Process in batches
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batchKeys := keys[i:end]
		result, err := d.deleteSingleBatch(ctx, batchKeys)
		if err != nil {
			// If entire batch fails, mark all keys as failed
			for _, key := range batchKeys {
				allFailedKeys = append(allFailedKeys, FailedKey{
					Key:   key,
					Error: err,
				})
			}
			allErrors = append(allErrors, err)
			continue
		}

		totalSuccess += result.SuccessCount
		allFailedKeys = append(allFailedKeys, result.FailedKeys...)
		allErrors = append(allErrors, result.Errors...)
	}

	return &BatchResult{
		SuccessCount: totalSuccess,
		FailedKeys:   allFailedKeys,
		Errors:       allErrors,
	}, nil
}

// createBatchRequest creates a GCS JSON API batch request for delete operations
func (d *GCSBatchDeleter) createBatchRequest(keys []string) *GCSBatchRequest {
	requests := make([]GCSSubRequest, len(keys))

	for i, key := range keys {
		// URL-encode the object name
		encodedKey := url.PathEscape(key)

		requests[i] = GCSSubRequest{
			Method: "DELETE",
			URL:    fmt.Sprintf("/storage/v1/b/%s/o/%s", d.bucket, encodedKey),
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}
	}

	return &GCSBatchRequest{
		Requests: requests,
	}
}

// sendBatchRequest sends the batch request to GCS JSON API
func (d *GCSBatchDeleter) sendBatchRequest(ctx context.Context, batchRequest *GCSBatchRequest) (*GCSBatchResponse, error) {
	// Create multipart/mixed content
	body, contentType, err := d.createMultipartContent(batchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart content: %w", err)
	}

	// Create HTTP request
	batchURL := d.baseURL + "/batch/storage/v1"
	req, err := http.NewRequestWithContext(ctx, "POST", batchURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)

	// Add authentication
	token, err := d.credentials.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to get auth token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	// Send request
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send batch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("batch request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse batch response
	return d.parseBatchResponse(resp)
}

// createMultipartContent creates multipart/mixed content for the batch request
func (d *GCSBatchDeleter) createMultipartContent(batchRequest *GCSBatchRequest) (io.Reader, string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	for i, subRequest := range batchRequest.Requests {
		// Create a part for each subrequest
		partWriter, err := writer.CreatePart(nil)
		if err != nil {
			return nil, "", fmt.Errorf("failed to create part %d: %w", i, err)
		}

		// Write HTTP request format
		requestLine := fmt.Sprintf("%s %s HTTP/1.1\r\n", subRequest.Method, subRequest.URL)
		if _, err := partWriter.Write([]byte(requestLine)); err != nil {
			return nil, "", fmt.Errorf("failed to write request line for part %d: %w", i, err)
		}

		// Write headers
		for key, value := range subRequest.Headers {
			headerLine := fmt.Sprintf("%s: %s\r\n", key, value)
			if _, err := partWriter.Write([]byte(headerLine)); err != nil {
				return nil, "", fmt.Errorf("failed to write header for part %d: %w", i, err)
			}
		}

		// End headers
		if _, err := partWriter.Write([]byte("\r\n")); err != nil {
			return nil, "", fmt.Errorf("failed to write header separator for part %d: %w", i, err)
		}

		// Write body if present
		if subRequest.Body != nil {
			bodyBytes, err := json.Marshal(subRequest.Body)
			if err != nil {
				return nil, "", fmt.Errorf("failed to marshal body for part %d: %w", i, err)
			}
			if _, err := partWriter.Write(bodyBytes); err != nil {
				return nil, "", fmt.Errorf("failed to write body for part %d: %w", i, err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("failed to close multipart writer: %w", err)
	}

	contentType := fmt.Sprintf("multipart/mixed; boundary=%s", writer.Boundary())
	return &buf, contentType, nil
}

// parseBatchResponse parses the multipart/mixed batch response
func (d *GCSBatchDeleter) parseBatchResponse(resp *http.Response) (*GCSBatchResponse, error) {
	contentType := resp.Header.Get("Content-Type")
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, fmt.Errorf("failed to parse content type: %w", err)
	}

	boundary, ok := params["boundary"]
	if !ok {
		return nil, fmt.Errorf("no boundary found in content type")
	}

	reader := multipart.NewReader(resp.Body, boundary)
	var responses []GCSSubResponse

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read part: %w", err)
		}

		subResponse, err := d.parseSubResponse(part)
		if err != nil {
			log.Warn().Err(err).Msg("failed to parse sub-response")
			continue
		}

		responses = append(responses, *subResponse)
		part.Close()
	}

	return &GCSBatchResponse{Responses: responses}, nil
}

// parseSubResponse parses a single sub-response from the batch
func (d *GCSBatchDeleter) parseSubResponse(part io.Reader) (*GCSSubResponse, error) {
	content, err := io.ReadAll(part)
	if err != nil {
		return nil, fmt.Errorf("failed to read part content: %w", err)
	}

	lines := strings.Split(string(content), "\r\n")
	if len(lines) < 1 {
		return nil, fmt.Errorf("invalid response format")
	}

	// Parse status line
	statusLine := lines[0]
	parts := strings.Split(statusLine, " ")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid status line: %s", statusLine)
	}

	statusCode, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid status code: %s", parts[1])
	}

	// Parse headers
	headers := make(map[string]string)
	i := 1
	for ; i < len(lines) && lines[i] != ""; i++ {
		headerParts := strings.SplitN(lines[i], ": ", 2)
		if len(headerParts) == 2 {
			headers[headerParts[0]] = headerParts[1]
		}
	}

	// Parse body if present
	var body map[string]interface{}
	var gcsError *GCSError

	if i+1 < len(lines) {
		bodyContent := strings.Join(lines[i+1:], "\r\n")
		if strings.TrimSpace(bodyContent) != "" {
			if err := json.Unmarshal([]byte(bodyContent), &body); err != nil {
				// Try to parse as error
				if jsonErr := json.Unmarshal([]byte(bodyContent), &gcsError); jsonErr != nil {
					log.Debug().Str("body", bodyContent).Msg("failed to parse response body as JSON")
				}
			}
		}
	}

	return &GCSSubResponse{
		StatusCode: statusCode,
		Headers:    headers,
		Body:       body,
		Error:      gcsError,
	}, nil
}

// processBatchResponse processes the batch response and creates a BatchResult
func (d *GCSBatchDeleter) processBatchResponse(batchResponse *GCSBatchResponse, keys []string) (*BatchResult, error) {
	var successCount int
	var failedKeys []FailedKey
	var errors []error

	if len(batchResponse.Responses) != len(keys) {
		return nil, fmt.Errorf("response count (%d) doesn't match request count (%d)",
			len(batchResponse.Responses), len(keys))
	}

	for i, subResponse := range batchResponse.Responses {
		key := keys[i]

		if d.isSuccessfulDelete(subResponse.StatusCode) {
			successCount++
		} else {
			err := d.createErrorFromSubResponse(subResponse, key)
			failedKeys = append(failedKeys, FailedKey{
				Key:   key,
				Error: err,
			})
			errors = append(errors, err)
		}
	}

	log.Debug().
		Int("success_count", successCount).
		Int("failed_count", len(failedKeys)).
		Msg("GCS JSON API batch delete completed")

	return &BatchResult{
		SuccessCount: successCount,
		FailedKeys:   failedKeys,
		Errors:       errors,
	}, nil
}

// isSuccessfulDelete determines if a status code represents successful deletion
func (d *GCSBatchDeleter) isSuccessfulDelete(statusCode int) bool {
	switch statusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		// 200/204 = successful delete, 404 = already deleted
		return true
	default:
		return false
	}
}

// createErrorFromSubResponse creates an error from a failed sub-response
func (d *GCSBatchDeleter) createErrorFromSubResponse(subResponse GCSSubResponse, key string) error {
	if subResponse.Error != nil {
		return &googleapi.Error{
			Code:    subResponse.Error.Code,
			Message: fmt.Sprintf("GCS delete failed for %s: %s", key, subResponse.Error.Message),
		}
	}

	return fmt.Errorf("GCS delete failed for %s with status %d", key, subResponse.StatusCode)
}
