// Part 3: Rate-Limited API Client Implementation (Advanced Difficulty)
// This component is our customer-facing API that must handle extreme traffic while maintaining reliability

use async_trait::async_trait;
use std::time::Duration;
use thiserror::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio::time::{Instant as TokioInstant, sleep};
use std::collections::BinaryHeap;

// Enhanced error types for API client
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    #[error("Request timeout after {0}ms")]
    Timeout(u64),

    #[error("Circuit breaker open for {service_name}")]
    CircuitBreakerOpen {
        service_name: String,
        retry_after_ms: Option<u64>,
    },

    #[error("API error: {status_code} - {message}")]
    ApiResponseError {
        status_code: u16,
        message: String,
        is_retryable: bool,
    },

    #[error("Preempted by higher priority request")]
    RequestPreempted,

    #[error("Client error: {0}")]
    ClientError(String),

    #[error("Request queue full")]
    QueueFull,

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Initialization error: {0}")]
    InitError(String),
}

// Enhanced client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub api_key: String,
    pub max_requests_per_second: u32,
    pub max_burst_size: u32,
    pub max_concurrent_requests: u32,
    pub timeout_ms: u64,
    pub retry_config: RetryConfig,
    pub circuit_breaker_config: CircuitBreakerConfig,
    pub queue_size_per_priority: usize,
    pub health_check_interval_ms: u64,
}

// Enhanced retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_multiplier: f64,
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub reset_timeout_ms: u64,
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            reset_timeout_ms: 30000,
            half_open_max_requests: 1,
        }
    }
}

// Request priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestPriority {
    Low = 0,
    Medium = 1,
    High = 2,
    Critical = 3,
}

impl Default for RequestPriority {
    fn default() -> Self {
        RequestPriority::Medium
    }
}

// Enhanced client statistics
#[derive(Debug, Default, Clone)]
pub struct ClientStats {
    pub requests_sent: usize,
    pub requests_succeeded: usize,
    pub requests_failed: usize,
    pub requests_throttled: usize,
    pub requests_retried: usize,
    pub requests_preempted: usize,
    pub requests_timeout: usize,
    pub requests_circuit_broken: usize,
    pub average_response_time_ms: f64,
    pub p95_response_time_ms: f64,
    pub p99_response_time_ms: f64,
    pub max_response_time_ms: f64,
    pub active_requests: usize,
    pub queue_depth: usize,
    pub circuit_breaker_open: bool,
    pub current_rate_limit: u32,
    pub adaptive_rate_limit_multiplier: f64,
}

// Request and response types (enhanced for the assessment)
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub hotel_ids: Vec<String>,
    pub check_in: String,
    pub check_out: String,
    pub guests: u32,
    pub priority: RequestPriority,
    pub idempotency_key: Option<String>,
    pub context: RequestContext,
}

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub correlation_id: String,
    pub client_info: Option<ClientInfo>,
    pub request_deadline: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub ip: String,
    pub user_agent: String,
    pub country: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SearchResponse {
    pub search_id: String,
    pub results: Vec<SearchResult>,
    pub rate_limit_remaining: Option<u32>,
    pub processing_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub hotel_id: String,
    pub available: bool,
    pub price: Option<f64>,
    pub currency: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BookingRequest {
    pub search_id: String,
    pub hotel_id: String,
    pub guest_name: String,
    pub payment_info: PaymentInfo,
    pub priority: RequestPriority,
    pub idempotency_key: String,
    pub context: RequestContext,
}

#[derive(Debug, Clone)]
pub struct PaymentInfo {
    pub card_type: String,
    pub last_four: String,
    pub expiry: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BookingResponse {
    pub booking_id: String,
    pub status: String,
    pub confirmation_code: Option<String>,
    pub rate_limit_remaining: Option<u32>,
    pub processing_time_ms: u64,
}

// Health status for adaptively adjusting rate limits
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

// API client trait with enhanced requirements
#[async_trait]
pub trait ApiClient: Send + Sync + 'static {
    // Basic search operation
    async fn search(&self, request: SearchRequest) -> Result<SearchResponse, ApiError>;

    // Basic booking operation
    async fn book(&self, request: BookingRequest) -> Result<BookingResponse, ApiError>;

    // Get client statistics
    fn stats(&self) -> ClientStats;

    // Configure adaptive rate limiting based on system health
    async fn set_system_health(&self, health: SystemHealth) -> f64;

    // Cancel a pending request if it hasn't been processed yet
    async fn cancel_request(&self, correlation_id: &str) -> bool;

    // Update client configuration
    async fn update_config(&self, config: ClientConfig) -> Result<(), ClientError>;

    // Pause/resume processing (for maintenance windows)
    async fn pause(&self, drain: bool) -> Result<(), ClientError>;
    async fn resume(&self) -> Result<(), ClientError>;

    // Forcibly clear circuit breakers (emergency use only)
    async fn reset_circuit_breakers(&self) -> usize;
}

// Booking API client to implement
pub struct BookingApiClient {
    config: ClientConfig,
    stats: Arc<Mutex<ClientStats>>,
    token_bucket: Arc<TokenBucket>,
    circuit_breaker: Arc<Mutex<CircuitBreaker>>,
    request_queue: Arc<Mutex<BinaryHeap<QueuedRequest>>>,
}

struct QueuedRequest {
    priority: RequestPriority,
    request_id: String,
    timestamp: TokioInstant,
}

impl PartialEq for QueuedRequest {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.timestamp == other.timestamp
    }
}

impl Eq for QueuedRequest {}

impl PartialOrd for QueuedRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.priority.cmp(&self.priority) {
            std::cmp::Ordering::Equal => self.timestamp.cmp(&other.timestamp),
            other => other,
        }
    }
}

struct TokenBucket {
    tokens: AtomicU32,
    max_tokens: u32,
    refill_rate: f64,
    last_refill: Arc<Mutex<TokioInstant>>,
}

impl TokenBucket {
    fn new(max_tokens: u32, refill_rate: f64) -> Self {
        Self {
            tokens: AtomicU32::new(max_tokens),
            max_tokens,
            refill_rate,
            last_refill: Arc::new(Mutex::new(TokioInstant::now())),
        }
    }
    
    async fn acquire(&self, tokens_needed: u32) -> bool {
        let mut last_refill = self.last_refill.lock().await;
        let now = TokioInstant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();
        
        let tokens_to_add = (elapsed * self.refill_rate) as u32;
        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.max_tokens);
            self.tokens.store(new_tokens, Ordering::Relaxed);
            *last_refill = now;
        }
        
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < tokens_needed {
                return false;
            }
            if self.tokens.compare_exchange(current, current - tokens_needed, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                return true;
            }
        }
    }
}

struct CircuitBreaker {
    failures: usize,
    state: CircuitState,
    last_failure_time: Option<TokioInstant>,
}

enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self {
            failures: 0,
            state: CircuitState::Closed,
            last_failure_time: None,
        }
    }
    
    fn record_success(&mut self) {
        self.failures = 0;
        self.state = CircuitState::Closed;
    }
    
    fn record_failure(&mut self, config: &CircuitBreakerConfig) {
        self.failures += 1;
        self.last_failure_time = Some(TokioInstant::now());
        
        if self.failures >= config.failure_threshold as usize {
            self.state = CircuitState::Open;
        }
    }
    
    async fn can_attempt(&mut self, config: &CircuitBreakerConfig) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    if TokioInstant::now().duration_since(last_failure).as_millis() as u64 >= config.reset_timeout_ms {
                        self.state = CircuitState::HalfOpen;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }
}

#[async_trait]
impl ApiClient for BookingApiClient {
    async fn search(&self, _request: SearchRequest) -> Result<SearchResponse, ApiError> {
        // TODO: Implement with:
        // - Rate limiting using token bucket algorithm
        // - Priority-based queueing
        // - Circuit breaker pattern
        // - Retry with exponential backoff and jitter
        // - Detailed telemetry collection
        // - Adaptive throttling based on system health
        Err(ApiError::Other("Not implemented".to_string()))
    }

    async fn book(&self, _request: BookingRequest) -> Result<BookingResponse, ApiError> {
        // TODO: Implement with higher priority than search requests
        // Bookings should be able to preempt search requests when needed
        Err(ApiError::Other("Not implemented".to_string()))
    }

    fn stats(&self) -> ClientStats {
        // TODO: Implement comprehensive statistics
        ClientStats::default()
    }

    async fn set_system_health(&self, health: SystemHealth) -> f64 {
        // TODO: Implement adaptive rate limiting based on system health
        // - Healthy: 100% of configured rate
        // - Degraded: 60% of configured rate
        // - Unhealthy: 20% of configured rate
        match health {
            SystemHealth::Healthy => 1.0,
            SystemHealth::Degraded => 0.6,
            SystemHealth::Unhealthy => 0.2,
        }
    }

    async fn cancel_request(&self, _correlation_id: &str) -> bool {
        // TODO: Implement request cancellation
        false
    }

    async fn update_config(&self, _config: ClientConfig) -> Result<(), ClientError> {
        // TODO: Implement dynamic configuration updates
        Err(ClientError::ConfigError("Not implemented".to_string()))
    }

    async fn pause(&self, _drain: bool) -> Result<(), ClientError> {
        // TODO: Implement graceful pause
        Err(ClientError::ConfigError("Not implemented".to_string()))
    }

    async fn resume(&self) -> Result<(), ClientError> {
        // TODO: Implement resume
        Err(ClientError::ConfigError("Not implemented".to_string()))
    }

    async fn reset_circuit_breakers(&self) -> usize {
        // TODO: Implement circuit breaker reset
        0
    }
}

impl BookingApiClient {
    // Create a new client with the given configuration
    pub async fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let token_bucket = Arc::new(TokenBucket::new(
            config.max_burst_size,
            config.max_requests_per_second as f64,
        ));
        
        let circuit_breaker = Arc::new(Mutex::new(CircuitBreaker::new()));
        
        Ok(Self {
            config,
            stats: Arc::new(Mutex::new(ClientStats::default())),
            token_bucket,
            circuit_breaker,
            request_queue: Arc::new(Mutex::new(BinaryHeap::new())),
        })
    }

    // Helper to calculate exponential backoff with jitter
    pub fn calculate_backoff(retry_attempt: u32, config: &RetryConfig) -> Duration {
        let base_backoff_ms = (config.initial_backoff_ms as f64
            * config.backoff_multiplier.powf(retry_attempt as f64))
        .min(config.max_backoff_ms as f64);

        // Apply jitter to prevent thundering herd
        let jitter = rand::random::<f64>() * config.jitter_factor * base_backoff_ms;
        let backoff_ms = base_backoff_ms * (1.0 - config.jitter_factor / 2.0) + jitter;

        Duration::from_millis(backoff_ms as u64)
    }
}

// Enhanced mock server for testing (you can modify or extend this)
#[cfg(test)]
pub mod mock_server {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, Copy)]
    pub enum ServerMode {
        Normal,
        Degraded,
        Overloaded,
        PartialOutage,
        CompleteOutage,
    }

    pub struct MockServer {
        mode: std::sync::atomic::AtomicU8,
        request_count: AtomicUsize,
        search_responses: Mutex<HashMap<String, SearchResponse>>,
        booking_responses: Mutex<HashMap<String, BookingResponse>>,
        fail_next_requests: AtomicUsize,
        delay_ms: AtomicUsize,
        rate_limit: AtomicUsize,
        rate_limit_window_ms: AtomicUsize,
        recent_requests: Mutex<Vec<(Instant, String)>>,
        dropped_request_count: AtomicUsize,
    }

    impl MockServer {
        pub fn new() -> Self {
            Self {
                mode: std::sync::atomic::AtomicU8::new(0), // Normal mode
                request_count: AtomicUsize::new(0),
                search_responses: Mutex::new(HashMap::new()),
                booking_responses: Mutex::new(HashMap::new()),
                fail_next_requests: AtomicUsize::new(0),
                delay_ms: AtomicUsize::new(0),
                rate_limit: AtomicUsize::new(100), // Default: 100 requests per window
                rate_limit_window_ms: AtomicUsize::new(1000), // Default: 1-second window
                recent_requests: Mutex::new(Vec::new()),
                dropped_request_count: AtomicUsize::new(0),
            }
        }

        pub fn set_mode(&self, mode: ServerMode) {
            let mode_value = match mode {
                ServerMode::Normal => 0,
                ServerMode::Degraded => 1,
                ServerMode::Overloaded => 2,
                ServerMode::PartialOutage => 3,
                ServerMode::CompleteOutage => 4,
            };
            self.mode.store(mode_value, Ordering::SeqCst);
        }

        pub fn set_delay(&self, delay_ms: usize) {
            self.delay_ms.store(delay_ms, Ordering::SeqCst);
        }

        pub fn set_rate_limit(&self, limit: usize, window_ms: usize) {
            self.rate_limit.store(limit, Ordering::SeqCst);
            self.rate_limit_window_ms.store(window_ms, Ordering::SeqCst);
        }

        pub fn fail_next_requests(&self, count: usize) {
            self.fail_next_requests.store(count, Ordering::SeqCst);
        }

        pub async fn add_search_response(&self, hotel_id: &str, response: SearchResponse) {
            let mut responses = self.search_responses.lock().await;
            responses.insert(hotel_id.to_string(), response);
        }

        pub async fn add_booking_response(&self, hotel_id: &str, response: BookingResponse) {
            let mut responses = self.booking_responses.lock().await;
            responses.insert(hotel_id.to_string(), response);
        }

        // Enhanced implementation - check rate limits, simulate failures based on mode
        pub async fn handle_search(
            &self,
            request: SearchRequest,
        ) -> Result<SearchResponse, ApiError> {
            self.request_count.fetch_add(1, Ordering::SeqCst);

            // Check server mode
            let mode = self.mode.load(Ordering::SeqCst);
            match mode {
                4 => {
                    // Complete outage
                    return Err(ApiError::NetworkError("Service unavailable".to_string()));
                }
                3 => {
                    // Partial outage - 50% chance of failure
                    if rand::random::<f32>() < 0.5 {
                        return Err(ApiError::ApiResponseError {
                            status_code: 503,
                            message: "Service temporarily unavailable".to_string(),
                            is_retryable: true,
                        });
                    }
                }
                _ => {}
            }

            // Apply rate limiting
            let now = Instant::now();
            let limit = self.rate_limit.load(Ordering::SeqCst);
            let window_ms = self.rate_limit_window_ms.load(Ordering::SeqCst);

            let mut recent = self.recent_requests.lock().await;

            // Clean up old requests beyond the window
            let window_duration = Duration::from_millis(window_ms as u64);
            recent.retain(|(timestamp, _)| now.duration_since(*timestamp) < window_duration);

            // Check if we've hit the rate limit
            if recent.len() >= limit {
                self.dropped_request_count.fetch_add(1, Ordering::SeqCst);
                return Err(ApiError::RateLimitExceeded(format!(
                    "Rate limit of {} requests per {}ms exceeded",
                    limit, window_ms
                )));
            }

            // Track this request
            recent.push((now, request.context.correlation_id.clone()));

            // Simulate delay
            let delay = self.delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                // Add jitter for realism
                let jitter = if mode > 0 {
                    rand::random::<usize>() % delay
                } else {
                    0
                };
                tokio::time::sleep(Duration::from_millis((delay + jitter) as u64)).await;
            }

            // Simulate failures
            let fail_count = self.fail_next_requests.load(Ordering::SeqCst);
            if fail_count > 0 {
                self.fail_next_requests
                    .store(fail_count - 1, Ordering::SeqCst);
                return Err(ApiError::ApiResponseError {
                    status_code: 500,
                    message: "Internal Server Error".to_string(),
                    is_retryable: true,
                });
            }

            // Return mock response
            let responses = self.search_responses.lock().await;
            if let Some(hotel_id) = request.hotel_ids.first() {
                if let Some(response) = responses.get(hotel_id) {
                    let mut response = response.clone();
                    response.rate_limit_remaining = Some((limit - recent.len()) as u32);
                    return Ok(response);
                }
            }

            // Default response
            Ok(SearchResponse {
                search_id: format!("search-{}", rand::random::<u32>()),
                results: vec![],
                rate_limit_remaining: Some((limit - recent.len()) as u32),
                processing_time_ms: delay as u64,
            })
        }

        // Similar to handle_search but for booking
        pub async fn handle_booking(
            &self,
            request: BookingRequest,
        ) -> Result<BookingResponse, ApiError> {
            self.request_count.fetch_add(1, Ordering::SeqCst);

            // Prioritize bookings - they bypass rate limits but still affected by outages
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == 4 {
                // Complete outage
                return Err(ApiError::NetworkError("Service unavailable".to_string()));
            }

            // Apply delay based on server mode
            let delay = self.delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                let actual_delay = match mode {
                    0 => delay,
                    1 => delay * 2, // Degraded adds 2x delay
                    2 => delay * 3, // Overloaded adds 3x delay
                    _ => delay * 5, // Partial outage adds 5x delay
                };
                tokio::time::sleep(Duration::from_millis(actual_delay as u64)).await;
            }

            // Simulate failures based on mode
            let fail_probability = match mode {
                0 => 0.0, // Normal: no random failures
                1 => 0.1, // Degraded: 10% failure
                2 => 0.3, // Overloaded: 30% failure
                _ => 0.5, // Partial outage: 50% failure
            };

            if rand::random::<f64>() < fail_probability {
                return Err(ApiError::ApiResponseError {
                    status_code: 500,
                    message: "Internal Server Error".to_string(),
                    is_retryable: true,
                });
            }

            // Return mock response
            let responses = self.booking_responses.lock().await;
            if let Some(response) = responses.get(&request.hotel_id) {
                return Ok(response.clone());
            }

            // Default response
            Ok(BookingResponse {
                booking_id: format!("booking-{}", rand::random::<u32>()),
                status: "confirmed".to_string(),
                confirmation_code: Some(format!("CONF{}", rand::random::<u16>())),
                rate_limit_remaining: None, // Bookings don't count against rate limit
                processing_time_ms: delay as u64,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_server::{MockServer, ServerMode};
    use std::sync::Arc;
    use std::time::Instant;

    #[tokio::test]
    async fn test_adaptive_rate_limiting() {
        // TODO: Implement this test
        // - Create a mock server that simulates different health states
        // - Configure client with appropriate settings
        // - Test that client adapts rate limits based on server health
        // - Verify statistics reflect the adaptations
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        // TODO: Implement this test
        // - Create a mock server that consistently fails
        // - Configure client with circuit breaker settings
        // - Send requests until circuit breaker trips
        // - Verify that subsequent requests fail fast with CircuitBreakerOpen
        // - Wait for reset timeout
        // - Verify circuit breaker allows half-open testing
    }

    #[tokio::test]
    async fn test_prioritization_and_preemption() {
        // TODO: Implement this test
        // - Create a mock server with limited concurrency
        // - Send many low priority requests to saturate the server
        // - Then send high priority requests
        // - Verify high priority requests complete before low priority ones
        // - Verify some low priority requests were preempted
    }

    #[tokio::test]
    async fn test_retry_with_backoff() {
        // TODO: Implement this test
        // - Create a mock server that fails a specific number of times
        // - Send a request that triggers retries
        // - Measure time between retries to verify backoff
        // - Verify request eventually succeeds
        // - Check that retry statistics are updated
    }

    #[tokio::test]
    async fn test_extreme_load_handling() {
        // TODO: Implement this test
        // - Create a client with limited capacity
        // - Simultaneously send hundreds or thousands of requests
        // - Verify client maintains stability
        // - Check that low priority requests are rejected when overloaded
        // - Verify high priority requests still get through
        // - Check statistics for throughput and latency
    }
}
