// Example implementation of BookingApiClient
// This is a minimal working example - candidates should implement their own optimized version

use crate::part3_api::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub struct ExampleBookingApiClient {
    config: ClientConfig,
    stats: Arc<Mutex<ClientStats>>,
    request_count: Arc<Mutex<u32>>,
    last_request_time: Arc<Mutex<Option<Instant>>>,
}

#[async_trait]
impl ApiClient for ExampleBookingApiClient {
    async fn search(&self, request: SearchRequest) -> Result<SearchResponse, ApiError> {
        // Simple rate limiting
        {
            let mut last_time = self.last_request_time.lock().unwrap();
            let mut count = self.request_count.lock().unwrap();

            let now = Instant::now();
            if let Some(last) = *last_time {
                if now.duration_since(last)
                    < Duration::from_millis(1000 / self.config.max_requests_per_second as u64)
                {
                    return Err(ApiError::RateLimitExceeded(
                        "Rate limit exceeded".to_string(),
                    ));
                }
            }

            *last_time = Some(now);
            *count += 1;
        }

        // Simulate network delay
        sleep(Duration::from_millis(50)).await;

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.requests_sent += 1;
            stats.requests_succeeded += 1;
        }

        // Create mock response
        let results = request
            .hotel_ids
            .into_iter()
            .map(|hotel_id| SearchResult {
                hotel_id,
                available: true,
                price: Some(100.0),
                currency: Some("USD".to_string()),
            })
            .collect();

        Ok(SearchResponse {
            search_id: format!("search_{}", rand::random::<u32>()),
            results,
            rate_limit_remaining: Some(self.config.max_requests_per_second - 1),
            processing_time_ms: 50,
        })
    }

    async fn book(&self, _request: BookingRequest) -> Result<BookingResponse, ApiError> {
        // Bookings have higher priority - bypass some rate limits
        sleep(Duration::from_millis(100)).await;

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.requests_sent += 1;
            stats.requests_succeeded += 1;
        }

        Ok(BookingResponse {
            booking_id: format!("booking_{}", rand::random::<u32>()),
            status: "confirmed".to_string(),
            confirmation_code: Some(format!("CONF{}", rand::random::<u16>())),
            rate_limit_remaining: None, // Bookings don't count against rate limit
            processing_time_ms: 100,
        })
    }

    fn stats(&self) -> ClientStats {
        self.stats.lock().unwrap().clone()
    }

    async fn set_system_health(&self, health: SystemHealth) -> f64 {
        match health {
            SystemHealth::Healthy => 1.0,
            SystemHealth::Degraded => 0.6,
            SystemHealth::Unhealthy => 0.2,
        }
    }

    async fn cancel_request(&self, _correlation_id: &str) -> bool {
        // Simple implementation - always return false (not found)
        false
    }

    async fn update_config(&self, _config: ClientConfig) -> Result<(), ClientError> {
        // Simple implementation - just return success
        Ok(())
    }

    async fn pause(&self, _drain: bool) -> Result<(), ClientError> {
        // Simple implementation
        Ok(())
    }

    async fn resume(&self) -> Result<(), ClientError> {
        // Simple implementation
        Ok(())
    }

    async fn reset_circuit_breakers(&self) -> usize {
        // Simple implementation - return 0 (no circuit breakers reset)
        0
    }
}

impl ExampleBookingApiClient {
    pub async fn new(config: ClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            config,
            stats: Arc::new(Mutex::new(ClientStats::default())),
            request_count: Arc::new(Mutex::new(0)),
            last_request_time: Arc::new(Mutex::new(None)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_example_search() {
        let config = ClientConfig {
            base_url: "https://api.example.com".to_string(),
            api_key: "test_key".to_string(),
            max_requests_per_second: 10,
            max_burst_size: 20,
            max_concurrent_requests: 5,
            timeout_ms: 5000,
            retry_config: RetryConfig::default(),
            circuit_breaker_config: CircuitBreakerConfig::default(),
            queue_size_per_priority: 100,
            health_check_interval_ms: 30000,
        };

        let client = ExampleBookingApiClient::new(config).await.unwrap();

        let request = SearchRequest {
            hotel_ids: vec!["hotel1".to_string(), "hotel2".to_string()],
            check_in: "2025-06-01".to_string(),
            check_out: "2025-06-05".to_string(),
            guests: 2,
            priority: RequestPriority::Medium,
            idempotency_key: None,
            context: RequestContext {
                correlation_id: "test_correlation".to_string(),
                ..Default::default()
            },
        };

        let result = client.search(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.results.len(), 2);
        assert!(response.search_id.starts_with("search_"));

        // Check stats
        let stats = client.stats();
        assert_eq!(stats.requests_sent, 1);
        assert_eq!(stats.requests_succeeded, 1);
    }

    #[tokio::test]
    async fn test_example_booking() {
        let config = ClientConfig {
            base_url: "https://api.example.com".to_string(),
            api_key: "test_key".to_string(),
            max_requests_per_second: 10,
            max_burst_size: 20,
            max_concurrent_requests: 5,
            timeout_ms: 5000,
            retry_config: RetryConfig::default(),
            circuit_breaker_config: CircuitBreakerConfig::default(),
            queue_size_per_priority: 100,
            health_check_interval_ms: 30000,
        };

        let client = ExampleBookingApiClient::new(config).await.unwrap();

        let request = BookingRequest {
            search_id: "search_123".to_string(),
            hotel_id: "hotel1".to_string(),
            guest_name: "John Doe".to_string(),
            payment_info: PaymentInfo {
                card_type: "VISA".to_string(),
                last_four: "1234".to_string(),
                expiry: "12/25".to_string(),
                token: Some("token_123".to_string()),
            },
            priority: RequestPriority::High,
            idempotency_key: "booking_123".to_string(),
            context: RequestContext {
                correlation_id: "test_booking".to_string(),
                ..Default::default()
            },
        };

        let result = client.book(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.booking_id.starts_with("booking_"));
        assert_eq!(response.status, "confirmed");
        assert!(response.confirmation_code.is_some());
    }

    #[tokio::test]
    async fn test_example_rate_limiting() {
        let config = ClientConfig {
            base_url: "https://api.example.com".to_string(),
            api_key: "test_key".to_string(),
            max_requests_per_second: 2, // Very low for testing
            max_burst_size: 20,
            max_concurrent_requests: 5,
            timeout_ms: 5000,
            retry_config: RetryConfig::default(),
            circuit_breaker_config: CircuitBreakerConfig::default(),
            queue_size_per_priority: 100,
            health_check_interval_ms: 30000,
        };

        let client = ExampleBookingApiClient::new(config).await.unwrap();

        let request = SearchRequest {
            hotel_ids: vec!["hotel1".to_string()],
            check_in: "2025-06-01".to_string(),
            check_out: "2025-06-05".to_string(),
            guests: 2,
            priority: RequestPriority::Medium,
            idempotency_key: None,
            context: RequestContext {
                correlation_id: "test_rate_limit".to_string(),
                ..Default::default()
            },
        };

        // First request should succeed
        let result1 = client.search(request.clone()).await;
        assert!(result1.is_ok());

        // Second request immediately should be rate limited
        let result2 = client.search(request.clone()).await;
        assert!(result2.is_err());

        if let Err(ApiError::RateLimitExceeded(_)) = result2 {
            // Expected
        } else {
            panic!("Expected rate limit error");
        }
    }
}
