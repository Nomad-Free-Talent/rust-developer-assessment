// Example implementation of AvailabilityCache for demonstration purposes
// This is a minimal working example - candidates should implement their own optimized version

use crate::part1_cache::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct ExampleCache {
    data: Arc<Mutex<HashMap<String, CacheEntry>>>,
    config: CacheConfig,
    stats: Arc<Mutex<CacheStats>>,
}

struct CacheEntry {
    data: Vec<u8>,
    created_at: Instant,
    ttl: Duration,
    access_count: usize,
    last_accessed: Instant,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

impl AvailabilityCache for ExampleCache {
    fn new(config: CacheConfig) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            config,
            stats: Arc::new(Mutex::new(CacheStats::default())),
        }
    }

    fn store(
        &self,
        hotel_id: &str,
        check_in: &str,
        check_out: &str,
        data: Vec<u8>,
        ttl: Option<Duration>,
    ) -> bool {
        let key = create_cache_key(hotel_id, check_in, check_out);
        let ttl = ttl.unwrap_or_else(|| Duration::from_secs(self.config.default_ttl_seconds));

        let mut cache = self.data.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();

        // Simple size check (not perfect but demonstrates the concept)
        let item_size = calculate_item_size(&key, &data);
        let max_size_bytes = self.config.max_size_mb * 1024 * 1024;

        // Calculate current size
        let current_size: usize = cache
            .values()
            .map(|entry| calculate_item_size("", &entry.data))
            .sum();

        if current_size + item_size > max_size_bytes {
            // Simple eviction: remove oldest entry
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
                stats.eviction_count += 1;
            }
        }

        let entry = CacheEntry {
            data,
            created_at: Instant::now(),
            ttl,
            access_count: 0,
            last_accessed: Instant::now(),
        };

        cache.insert(key, entry);
        stats.items_count = cache.len();

        true
    }

    fn get(&self, hotel_id: &str, check_in: &str, check_out: &str) -> Option<(Vec<u8>, bool)> {
        let key = create_cache_key(hotel_id, check_in, check_out);
        let mut cache = self.data.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();

        stats.total_lookups += 1;

        if let Some(entry) = cache.get_mut(&key) {
            if entry.is_expired() {
                cache.remove(&key);
                stats.expired_count += 1;
                stats.miss_count += 1;
                stats.items_count = cache.len();
                return None;
            }

            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            stats.hit_count += 1;
            Some((entry.data.clone(), true))
        } else {
            stats.miss_count += 1;
            None
        }
    }

    fn stats(&self) -> CacheStats {
        let cache = self.data.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();

        // Update current stats
        stats.items_count = cache.len();
        stats.size_bytes = cache
            .values()
            .map(|entry| calculate_item_size("", &entry.data))
            .sum();

        if stats.total_lookups > 0 {
            stats.average_lookup_time_ns = 1000; // Placeholder - real implementation would measure
        }

        stats.clone()
    }

    fn set_eviction_policy(&self, _policy: EvictionPolicy) {
        // Simple implementation - just store the policy
        // Real implementation would change eviction behavior
    }

    fn prefetch(&self, keys: Vec<(String, String, String)>, ttl: Option<Duration>) -> usize {
        // Simple implementation - in real system this would trigger backend calls
        let mut count = 0;
        for (hotel_id, check_in, check_out) in keys {
            // Simulate fetching data
            let dummy_data = vec![1, 2, 3, 4, 5];
            if self.store(&hotel_id, &check_in, &check_out, dummy_data, ttl) {
                count += 1;
            }
        }
        count
    }

    fn invalidate(
        &self,
        hotel_id: Option<&str>,
        check_in: Option<&str>,
        check_out: Option<&str>,
    ) -> usize {
        let mut cache = self.data.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();

        let keys_to_remove: Vec<String> = cache
            .keys()
            .filter(|key| {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() != 3 {
                    return false;
                }

                let matches_hotel = hotel_id.is_none_or(|h| parts[0] == h);
                let matches_checkin = check_in.is_none_or(|c| parts[1] == c);
                let matches_checkout = check_out.is_none_or(|c| parts[2] == c);

                matches_hotel && matches_checkin && matches_checkout
            })
            .cloned()
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            cache.remove(&key);
        }

        stats.items_count = cache.len();
        count
    }

    fn resize(&self, _new_max_size_mb: usize) -> bool {
        // Simple implementation - just update config
        // Real implementation would enforce new size immediately
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_cache_basic_operations() {
        let config = CacheConfig::default();
        let cache = ExampleCache::new(config);

        // Test store and get
        let data = vec![1, 2, 3, 4, 5];
        assert!(cache.store("hotel1", "2025-06-01", "2025-06-05", data.clone(), None));

        let result = cache.get("hotel1", "2025-06-01", "2025-06-05");
        assert!(result.is_some());
        let (retrieved_data, is_hit) = result.unwrap();
        assert_eq!(retrieved_data, data);
        assert!(is_hit);

        // Test miss
        let miss_result = cache.get("hotel2", "2025-06-01", "2025-06-05");
        assert!(miss_result.is_none());

        // Test stats
        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(stats.items_count, 1);
    }

    #[test]
    fn test_example_cache_invalidation() {
        let config = CacheConfig::default();
        let cache = ExampleCache::new(config);

        // Add some test data
        cache.store("hotel1", "2025-06-01", "2025-06-05", vec![1], None);
        cache.store("hotel1", "2025-06-10", "2025-06-15", vec![2], None);
        cache.store("hotel2", "2025-06-01", "2025-06-05", vec![3], None);

        // Invalidate all hotel1 entries
        let invalidated = cache.invalidate(Some("hotel1"), None, None);
        assert_eq!(invalidated, 2);

        // Verify hotel1 entries are gone
        assert!(cache.get("hotel1", "2025-06-01", "2025-06-05").is_none());
        assert!(cache.get("hotel1", "2025-06-10", "2025-06-15").is_none());

        // But hotel2 entry should still be there
        assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());
    }
}
