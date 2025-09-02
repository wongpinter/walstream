package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/wongpinter/walstreamer/logging"
)

const (
	defaultPubSubAPIBase   = "https://pubsub.googleapis.com/v1"
	defaultHTTPTimeout     = 30 * time.Second
	defaultRetentionPeriod = 7 * 24 * time.Hour // 7 days
)

// BQSubscriberError represents custom errors from BQSubscriber operations
type BQSubscriberError struct {
	Operation string
	Status    int
	Message   string
	Err       error
}

func (e *BQSubscriberError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s failed: %v (status: %d, message: %s)", e.Operation, e.Err, e.Status, e.Message)
	}
	return fmt.Sprintf("%s failed (status: %d, message: %s)", e.Operation, e.Status, e.Message)
}

func (e *BQSubscriberError) Unwrap() error {
	return e.Err
}

// BQSubscriber manages BigQuery subscriptions using the REST API.
// It provides functionality to create and delete BigQuery subscriptions
// for streaming data from Pub/Sub topics directly to BigQuery tables.
type BQSubscriber struct {
	projectID string
	client    *http.Client
	logger    *logging.Logger
	baseURL   string // API base URL, can be overridden for testing
}

// BQSubscriptionConfig represents the configuration for a BigQuery subscription.
// It defines how messages from a Pub/Sub topic should be delivered to a BigQuery table.
type BQSubscriptionConfig struct {
	// Required fields
	TopicID        string // ID of the source Pub/Sub topic
	SubscriptionID string // ID for the new subscription
	TableRef       string // Format: "project.dataset.table"

	// Optional fields with defaults
	RetentionDuration time.Duration // Default: 7 days
	WriteMetadata     bool          // Whether to include Pub/Sub message metadata
}

// BQSubscriberOption defines a function type for configuring BQSubscriber
type BQSubscriberOption func(*BQSubscriber)

// WithBaseURL sets a custom base URL for the Pub/Sub API
func WithBaseURL(baseURL string) BQSubscriberOption {
	return func(s *BQSubscriber) {
		s.baseURL = baseURL
	}
}

// WithHTTPClient sets a custom HTTP client for the subscriber
func WithHTTPClient(client *http.Client) BQSubscriberOption {
	return func(s *BQSubscriber) {
		s.client = client
	}
}

// WithCredentialsFile sets up authentication using a credentials file
func WithCredentialsFile(ctx context.Context, credentialsFile string) BQSubscriberOption {
	return func(s *BQSubscriber) {
		if s.client != nil {
			return // Don't override if client is already set
		}

		// readfile credentials to byte
		credsBytes, err := os.ReadFile(credentialsFile)
		if err != nil {
			// Log error but don't fail - will fall back to default credentials
			s.logger.Error().Err(err).Msg("Failed to read credentials file")
			return
		}

		creds, err := google.CredentialsFromJSON(ctx, credsBytes,
			"https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/pubsub")
		if err != nil {
			// Log error but don't fail - will fall back to default credentials
			s.logger.Error().Err(err).Msg("Failed to load credentials from file")
			return
		}
		s.client = &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &oauth2.Transport{
				Source: creds.TokenSource,
				Base:   http.DefaultTransport,
			},
		}
	}
}

// NewBQSubscriber creates a new BigQuery subscription manager.
// By default, it uses application default credentials and the default Pub/Sub API base URL.
// Use options to customize the behavior, such as WithCredentialsFile for explicit credentials.
func NewBQSubscriber(ctx context.Context, projectID string, logger *logging.Logger, opts ...BQSubscriberOption) (*BQSubscriber, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	sub := &BQSubscriber{
		projectID: projectID,
		logger:    logger.WithComponent("bqsubscriber"),
		baseURL:   defaultPubSubAPIBase,
	}

	// Apply all options first
	for _, opt := range opts {
		opt(sub)
	}

	// If no client was set by options, create default one
	if sub.client == nil {
		creds, err := google.FindDefaultCredentials(ctx,
			"https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/pubsub")
		if err != nil {
			return nil, fmt.Errorf("failed to load default credentials: %w", err)
		}

		sub.client = &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &oauth2.Transport{
				Source: creds.TokenSource,
				Base:   http.DefaultTransport,
			},
		}
	}

	return sub, nil
}

// CreateSubscription creates a new BigQuery subscription using the REST API.
// It sets up a subscription that will automatically export messages from the
// specified Pub/Sub topic to a BigQuery table.
func (s *BQSubscriber) CreateSubscription(ctx context.Context, cfg BQSubscriptionConfig) error {
	if cfg.RetentionDuration == 0 {
		cfg.RetentionDuration = defaultRetentionPeriod
	}

	subsURL := fmt.Sprintf("%s/projects/%s/subscriptions/%s",
		s.baseURL, s.projectID, cfg.SubscriptionID)

	body := map[string]interface{}{
		"topic": fmt.Sprintf("projects/%s/topics/%s", s.projectID, cfg.TopicID),
		"bigqueryConfig": map[string]interface{}{
			"table":          cfg.TableRef,
			"writeMetadata":  cfg.WriteMetadata,
			"useTableSchema": true,
		},
		"retainAckedMessages":      true,
		"messageRetentionDuration": fmt.Sprintf("%.0fs", cfg.RetentionDuration.Seconds()),
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", subsURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	s.logger.Debug().
		Str("method", req.Method).
		Str("url", req.URL.String()).
		Str("topic", cfg.TopicID).
		Str("subscription", cfg.SubscriptionID).
		Str("table", cfg.TableRef).
		Msg("Creating BigQuery subscription")

	resp, err := s.client.Do(req)
	if err != nil {
		return &BQSubscriberError{
			Operation: "CreateSubscription",
			Err:       err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		var errResp map[string]interface{}
		if err := json.Unmarshal(body, &errResp); err != nil {
			return &BQSubscriberError{
				Operation: "CreateSubscription",
				Status:    resp.StatusCode,
				Message:   string(body),
			}
		}
		return &BQSubscriberError{
			Operation: "CreateSubscription",
			Status:    resp.StatusCode,
			Message:   fmt.Sprintf("%v", errResp),
		}
	}

	s.logger.Info().
		Str("subscription", cfg.SubscriptionID).
		Str("table", cfg.TableRef).
		Msg("BigQuery subscription created successfully")

	return nil
}

// DeleteSubscription deletes a BigQuery subscription.
// It will return an error if the subscription doesn't exist or if there's a permission issue.
func (s *BQSubscriber) DeleteSubscription(ctx context.Context, subscriptionID string) error {
	subsURL := fmt.Sprintf("%s/projects/%s/subscriptions/%s",
		s.baseURL, s.projectID, subscriptionID)

	req, err := http.NewRequestWithContext(ctx, "DELETE", subsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	s.logger.Debug().
		Str("method", req.Method).
		Str("url", req.URL.String()).
		Str("subscription", subscriptionID).
		Msg("Deleting BigQuery subscription")

	resp, err := s.client.Do(req)
	if err != nil {
		return &BQSubscriberError{
			Operation: "DeleteSubscription",
			Err:       err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		var errResp map[string]interface{}
		if err := json.Unmarshal(body, &errResp); err != nil {
			return &BQSubscriberError{
				Operation: "DeleteSubscription",
				Status:    resp.StatusCode,
				Message:   string(body),
			}
		}
		return &BQSubscriberError{
			Operation: "DeleteSubscription",
			Status:    resp.StatusCode,
			Message:   fmt.Sprintf("%v", errResp),
		}
	}

	s.logger.Info().
		Str("subscription", subscriptionID).
		Msg("BigQuery subscription deleted successfully")

	return nil
}
