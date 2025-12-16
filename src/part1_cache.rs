// Part 1: Hotel Availability Cache Implementation (Moderate Difficulty)
// This component serves as the middleware between our high-traffic customer-facing API and supplier systems

use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

// Enhanced stats for the cache
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub size_bytes: usize,
    pub items_count: usize,
    pub hit_count: usize,
    pub miss_count: usize,
    pub eviction_count: usize,
    pub expired_count: usize,
    pub rejected_count: usize,
    pub average_lookup_time_ns: u64,
    pub total_lookups: usize,
}

// Cache configuration options
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_size_mb: usize,
    pub default_ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub shards_count: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_mb: 100,
            default_ttl_seconds: 300,
            cleanup_interval_seconds: 60,
            shards_count: 16,
        }
    }
}

// Sharded cache implementation
pub struct ShardedCache {
    shards: Vec<Arc<RwLock<HashMap<String, CacheEntry>>>>,
    config: CacheConfig,
    stats: Arc<RwLock<CacheStats>>,
    eviction_policy: Arc<RwLock<EvictionPolicy>>,
}

struct CacheEntry {
    data: Vec<u8>,
    created_at: Instant,
    ttl: Duration,
    access_count: usize,
    last_accessed: Instant,
    size_bytes: usize,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

impl ShardedCache {
    fn get_shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }
}

impl AvailabilityCache for ShardedCache {
    fn new(config: CacheConfig) -> Self {
        let shards_count = config.shards_count.max(1);
        let shards: Vec<_> = (0..shards_count)
            .map(|_| Arc::new(RwLock::new(HashMap::new())))
            .collect();
        
        Self {
            shards,
            config,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            eviction_policy: Arc::new(RwLock::new(EvictionPolicy::LeastRecentlyUsed)),
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
        let start_time = Instant::now();
        let key = create_cache_key(hotel_id, check_in, check_out);
        let shard_index = self.get_shard_index(&key);
        let shard = &self.shards[shard_index];
        
        let ttl = ttl.unwrap_or_else(|| Duration::from_secs(self.config.default_ttl_seconds));
        let item_size = calculate_item_size(&key, &data);
        let max_size_bytes = self.config.max_size_mb * 1024 * 1024;
        
        let mut shard_guard = shard.write();
        let mut stats_guard = self.stats.write();
        
        let current_size: usize = shard_guard.values().map(|e| e.size_bytes).sum();
        
        if current_size + item_size > max_size_bytes {
            stats_guard.rejected_count += 1;
            return false;
        }
        
        let entry = CacheEntry {
            data,
            created_at: Instant::now(),
            ttl,
            access_count: 0,
            last_accessed: Instant::now(),
            size_bytes: item_size,
        };
        
        shard_guard.insert(key, entry);
        stats_guard.items_count += 1;
        stats_guard.size_bytes += item_size;
        
        let lookup_time = start_time.elapsed().as_nanos() as u64;
        stats_guard.total_lookups += 1;
        if stats_guard.total_lookups > 0 {
            stats_guard.average_lookup_time_ns = 
                (stats_guard.average_lookup_time_ns * (stats_guard.total_lookups - 1) as u64 + lookup_time) / stats_guard.total_lookups as u64;
        }
        
        true
    }

    fn get(&self, hotel_id: &str, check_in: &str, check_out: &str) -> Option<(Vec<u8>, bool)> {
        let start_time = Instant::now();
        let key = create_cache_key(hotel_id, check_in, check_out);
        let shard_index = self.get_shard_index(&key);
        let shard = &self.shards[shard_index];
        
        let mut shard_guard = shard.write();
        let mut stats_guard = self.stats.write();
        
        stats_guard.total_lookups += 1;
        
        if let Some(entry) = shard_guard.get_mut(&key) {
            if entry.is_expired() {
                shard_guard.remove(&key);
                stats_guard.expired_count += 1;
                stats_guard.miss_count += 1;
                return None;
            }
            
            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            stats_guard.hit_count += 1;
            
            let lookup_time = start_time.elapsed().as_nanos() as u64;
            if stats_guard.total_lookups > 0 {
                stats_guard.average_lookup_time_ns = 
                    (stats_guard.average_lookup_time_ns * (stats_guard.total_lookups - 1) as u64 + lookup_time) / stats_guard.total_lookups as u64;
            }
            
            Some((entry.data.clone(), true))
        } else {
            stats_guard.miss_count += 1;
            None
        }
    }

    fn stats(&self) -> CacheStats {
        let mut stats = self.stats.read().clone();
        
        // update stats from all shards
        let mut total_items = 0;
        let mut total_size = 0;
        for shard in &self.shards {
            let shard_guard = shard.read();
            total_items += shard_guard.len();
            total_size += shard_guard.values().map(|e| e.size_bytes).sum::<usize>();
        }
        stats.items_count = total_items;
        stats.size_bytes = total_size;
        
        stats
    }

    fn set_eviction_policy(&self, policy: EvictionPolicy) {
        *self.eviction_policy.write() = policy;
    }

    fn prefetch(&self, keys: Vec<(String, String, String)>, ttl: Option<Duration>) -> usize {
        0
    }

    fn invalidate(
        &self,
        hotel_id: Option<&str>,
        check_in: Option<&str>,
        check_out: Option<&str>,
    ) -> usize {
        0
    }

    fn resize(&self, new_max_size_mb: usize) -> bool {
        false
    }
}

// Eviction policy to use
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvictionPolicy {
    LeastRecentlyUsed,
    LeastFrequentlyUsed,
    TimeToLive,
}

// Cache trait to implement with enhanced requirements
pub trait AvailabilityCache: Send + Sync + 'static {
    // Initialize a new cache with the given configuration
    fn new(config: CacheConfig) -> Self
    where
        Self: Sized;

    // Store availability data for a hotel on specific dates
    // TTL specifies how long the item should remain in the cache (None uses default from config)
    // Returns true if stored successfully, false if rejected (e.g., capacity limits)
    fn store(
        &self,
        hotel_id: &str,
        check_in: &str,
        check_out: &str,
        data: Vec<u8>,
        ttl: Option<Duration>,
    ) -> bool;

    // Retrieve availability data if it exists and is not expired
    // The bool in the tuple indicates if this was a cache hit
    fn get(&self, hotel_id: &str, check_in: &str, check_out: &str) -> Option<(Vec<u8>, bool)>;

    // Get cache statistics
    fn stats(&self) -> CacheStats;

    // Set the eviction policy to use
    fn set_eviction_policy(&self, policy: EvictionPolicy);

    // Prefetch data for given keys - useful for warming the cache ahead of expected demand
    fn prefetch(&self, keys: Vec<(String, String, String)>, ttl: Option<Duration>) -> usize;

    // Bulk invalidate entries matching a pattern
    // For example, invalidate all entries for a specific hotel
    fn invalidate(
        &self,
        hotel_id: Option<&str>,
        check_in: Option<&str>,
        check_out: Option<&str>,
    ) -> usize;

    // Resize the cache (this might drop items if downsizing)
    fn resize(&self, new_max_size_mb: usize) -> bool;
}

// Helper function to create a cache key (you may modify this as needed)
pub fn create_cache_key(hotel_id: &str, check_in: &str, check_out: &str) -> String {
    format!("{}:{}:{}", hotel_id, check_in, check_out)
}

// Optional: Helper for calculating item size - implement if useful for your solution
pub fn calculate_item_size(key: &str, data: &[u8]) -> usize {
    key.len() + data.len() + std::mem::size_of::<Instant>() // Add more fields as needed for your implementation
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // Example of a more complex test for cache behavior under contention
    // #[test]
    // fn test_concurrent_access_with_contention() {
    //     let config = CacheConfig {
    //         max_size_mb: 5,
    //         default_ttl_seconds: 300,
    //         cleanup_interval_seconds: 60,
    //         shards_count: 8,
    //     };
    //
    //     let cache = Arc::new(YourCacheImplementation::new(config));
    //     let threads_count = 16; // High number of threads to create contention
    //     let operations_per_thread = 1000;
    //
    //     // Generate some popular keys that will have contention
    //     let popular_hotels = vec!["hotel1", "hotel2", "hotel3"];
    //     let popular_dates = vec![("2025-06-01", "2025-06-05"), ("2025-07-01", "2025-07-10")];
    //
    //     // Pre-populate cache with some data
    //     for hotel in &popular_hotels {
    //         for (check_in, check_out) in &popular_dates {
    //             let data = vec![1, 2, 3, 4, 5]; // Example data
    //             cache.store(hotel, check_in, check_out, data, None);
    //         }
    //     }
    //
    //     let mut handles = vec![];
    //     for i in 0..threads_count {
    //         let cache_clone = Arc::clone(&cache);
    //         let popular_hotels = popular_hotels.clone();
    //         let popular_dates = popular_dates.clone();
    //
    //         let handle = thread::spawn(move || {
    //             let mut rng = rand::thread_rng();
    //
    //             for j in 0..operations_per_thread {
    //                 // 80% of operations target popular items (creating contention)
    //                 let use_popular = rand::random::<f64>() < 0.8;
    //
    //                 let hotel_id;
    //                 let check_in;
    //                 let check_out;
    //
    //                 if use_popular {
    //                     // Use a popular hotel/date combination
    //                     hotel_id = popular_hotels[j % popular_hotels.len()].to_string();
    //                     let date_pair = &popular_dates[j % popular_dates.len()];
    //                     check_in = date_pair.0.to_string();
    //                     check_out = date_pair.1.to_string();
    //                 } else {
    //                     // Use a unique hotel/date combination
    //                     hotel_id = format!("hotel{}", i * 1000 + j);
    //                     check_in = format!("2025-{:02}-01", (j % 12) + 1);
    //                     check_out = format!("2025-{:02}-10", (j % 12) + 1);
    //                 }
    //
    //                 // Mix of read-heavy operations
    //                 if j % 10 < 8 {  // 80% reads
    //                     let _ = cache_clone.get(&hotel_id, &check_in, &check_out);
    //                 } else if j % 10 < 9 {  // 10% writes
    //                     let data = vec![i as u8, j as u8, 1, 2, 3, 4, 5];
    //                     cache_clone.store(&hotel_id, &check_in, &check_out, data, None);
    //                 } else {  // 10% invalidations
    //                     cache_clone.invalidate(Some(&hotel_id), None, None);
    //                 }
    //             }
    //         });
    //
    //         handles.push(handle);
    //     }
    //
    //     // Wait for all threads to complete
    //     for handle in handles {
    //         handle.join().unwrap();
    //     }
    //
    //     // Check cache stats
    //     let stats = cache.stats();
    //     println!("Cache stats after contention test: {:?}", stats);
    //
    //     // Verify we got substantial hits due to popular keys
    //     assert!(stats.hit_count > (threads_count * operations_per_thread / 2) as usize,
    //             "Expected significant cache hits due to popular keys");
    //
    //     // Verify average lookup time is reasonable
    //     assert!(stats.average_lookup_time_ns < 1_000_000, // 1ms
    //             "Average lookup time too high: {}ns", stats.average_lookup_time_ns);
    // }

    // #[test]
    // fn test_expiration_and_ttl() {
    //     let config = CacheConfig {
    //         max_size_mb: 5,
    //         default_ttl_seconds: 5, // Short TTL for testing
    //         cleanup_interval_seconds: 1,
    //         shards_count: 4,
    //     };
    //
    //     let cache = YourCacheImplementation::new(config);
    //
    //     let hotel_id = "hotel123";
    //     let check_in = "2025-06-01";
    //     let check_out = "2025-06-05";
    //     let data = vec![1, 2, 3, 4, 5];
    //
    //     // Store with default TTL
    //     assert!(cache.store(hotel_id, check_in, check_out, data.clone(), None));
    //
    //     // Store with custom shorter TTL
    //     let hotel_id2 = "hotel456";
    //     assert!(cache.store(hotel_id2, check_in, check_out, data.clone(),
    //                          Some(Duration::from_secs(2))));
    //
    //     // Verify both are initially available
    //     assert!(cache.get(hotel_id, check_in, check_out).is_some());
    //     assert!(cache.get(hotel_id2, check_in, check_out).is_some());
    //
    //     // Wait for the shorter TTL to expire
    //     thread::sleep(Duration::from_secs(3));
    //
    //     // hotel_id2 should be expired, hotel_id should still be valid
    //     assert!(cache.get(hotel_id, check_in, check_out).is_some());
    //     assert!(cache.get(hotel_id2, check_in, check_out).is_none());
    //
    //     // Wait for the longer TTL to expire
    //     thread::sleep(Duration::from_secs(3));
    //
    //     // Now both should be expired
    //     assert!(cache.get(hotel_id, check_in, check_out).is_none());
    //     assert!(cache.get(hotel_id2, check_in, check_out).is_none());
    //
    //     // Check expiration stats
    //     let stats = cache.stats();
    //     assert!(stats.expired_count >= 2, "Expected at least 2 expired items");
    // }

    // #[test]
    // fn test_eviction_policy_lru() {
    //     let config = CacheConfig {
    //         max_size_mb: 1, // Small size to force evictions
    //         default_ttl_seconds: 3600,
    //         cleanup_interval_seconds: 60,
    //         shards_count: 2,
    //     };
    //
    //     let cache = YourCacheImplementation::new(config);
    //     cache.set_eviction_policy(EvictionPolicy::LeastRecentlyUsed);
    //
    //     // Fill cache with items
    //     let large_data = vec![0; 250 * 1024]; // 250KB items
    //
    //     // Add 4 items totaling ~1MB to fill the cache
    //     for i in 0..4 {
    //         let hotel_id = format!("hotel{}", i);
    //         assert!(cache.store(&hotel_id, "2025-06-01", "2025-06-05", large_data.clone(), None));
    //     }
    //
    //     // Access item 0 and 2 to make them recently used
    //     assert!(cache.get("hotel0", "2025-06-01", "2025-06-05").is_some());
    //     assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());
    //
    //     // Add another item, which should evict least recently used (hotel1 or hotel3)
    //     assert!(cache.store("hotel4", "2025-06-01", "2025-06-05", large_data.clone(), None));
    //
    //     // hotel0 and hotel2 should still be in cache
    //     assert!(cache.get("hotel0", "2025-06-01", "2025-06-05").is_some());
    //     assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());
    //
    //     // Either hotel1 or hotel3 should be evicted
    //     let evicted = cache.get("hotel1", "2025-06-01", "2025-06-05").is_none() ||
    //                   cache.get("hotel3", "2025-06-01", "2025-06-05").is_none();
    //     assert!(evicted, "Expected LRU eviction to remove hotel1 or hotel3");
    //
    //     // Verify eviction stats
    //     let stats = cache.stats();
    //     assert!(stats.eviction_count > 0, "Expected evictions to occur");
    // }

    // #[test]
    // fn test_prefetch_and_invalidate() {
    //     let config = CacheConfig::default();
    //     let cache = YourCacheImplementation::new(config);
    //
    //     // Define some keys to prefetch
    //     let keys = vec![
    //         ("hotel1".to_string(), "2025-06-01".to_string(), "2025-06-05".to_string()),
    //         ("hotel1".to_string(), "2025-06-10".to_string(), "2025-06-15".to_string()),
    //         ("hotel2".to_string(), "2025-06-01".to_string(), "2025-06-05".to_string()),
    //     ];
    //
    //     // This would trigger backend calls in a real implementation
    //     // We'll simulate it by pre-populating the cache
    //     for (hotel, check_in, check_out) in &keys {
    //         let data = vec![1, 2, 3, 4, 5];
    //         cache.store(hotel, check_in, check_out, data, None);
    //     }
    //
    //     // Test bulk invalidation for a specific hotel
    //     let invalidated = cache.invalidate(Some("hotel1"), None, None);
    //     assert_eq!(invalidated, 2, "Expected 2 items to be invalidated");
    //
    //     // Verify hotel1 entries are gone
    //     assert!(cache.get("hotel1", "2025-06-01", "2025-06-05").is_none());
    //     assert!(cache.get("hotel1", "2025-06-10", "2025-06-15").is_none());
    //
    //     // But hotel2 entry should still be there
    //     assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());
    //
    //     // Test prefetching (would trigger backend calls in real impl)
    //     let prefetched = cache.prefetch(keys, None);
    //     assert_eq!(prefetched, 3, "Expected 3 items to be prefetched");
    //
    //     // All items should be in cache now
    //     assert!(cache.get("hotel1", "2025-06-01", "2025-06-05").is_some());
    //     assert!(cache.get("hotel1", "2025-06-10", "2025-06-15").is_some());
    //     assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());
    // }

    // #[test]
    // fn test_cache_resize() {
    //     let config = CacheConfig {
    //         max_size_mb: 10,
    //         default_ttl_seconds: 300,
    //         cleanup_interval_seconds: 60,
    //         shards_count: 4,
    //     };
    //
    //     let cache = YourCacheImplementation::new(config);
    //
    //     // Add some data
    //     let medium_data = vec![0; 100 * 1024]; // 100KB
    //     for i in 0..50 {
    //         let hotel_id = format!("hotel{}", i);
    //         cache.store(&hotel_id, "2025-06-01", "2025-06-05", medium_data.clone(), None);
    //     }
    //
    //     // Resize to smaller capacity
    //     assert!(cache.resize(2));
    //
    //     // Cache should evict items to maintain size limit
    //     let stats = cache.stats();
    //     assert!(stats.size_bytes <= 2 * 1024 * 1024,
    //             "Cache size exceeds 2MB after resizing: {}", stats.size_bytes);
    //     assert!(stats.items_count < 50,
    //             "Expected some items to be evicted after resizing");
    //
    //     // Resize to larger capacity
    //     assert!(cache.resize(20));
    //
    //     // Add more data
    //     for i in 50..150 {
    //         let hotel_id = format!("hotel{}", i);
    //         cache.store(&hotel_id, "2025-06-01", "2025-06-05", medium_data.clone(), None);
    //     }
    //
    //     // Cache should accommodate the data
    //     let new_stats = cache.stats();
    //     assert!(new_stats.items_count > stats.items_count,
    //             "Cache should accommodate more items after upsizing");
    // }
}
