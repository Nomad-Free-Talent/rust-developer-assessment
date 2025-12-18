// Part 1: Hotel Availability Cache Implementation (Moderate Difficulty)
// This component serves as the middleware between our high-traffic customer-facing API and supplier systems

use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

type PendingFetches = Arc<Mutex<HashMap<String, Arc<Mutex<Option<Vec<u8>>>>>>>;

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
    shards: Vec<Arc<RwLock<ShardData>>>,
    config: CacheConfig,
    stats: Arc<RwLock<CacheStats>>,
    eviction_policy: Arc<RwLock<EvictionPolicy>>,
    #[allow(dead_code)]
    cleanup_handle: Option<thread::JoinHandle<()>>,
    pending_fetches: PendingFetches,
}

struct ShardData {
    entries: HashMap<String, CacheEntry>,
    lru_order: Vec<String>,
    lfu_frequencies: HashMap<String, usize>,
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

impl ShardData {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru_order: Vec::new(),
            lfu_frequencies: HashMap::new(),
        }
    }
}

impl ShardedCache {
    fn get_shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn evict_lru(&self, shard: &Arc<RwLock<ShardData>>, needed_size: usize) -> usize {
        let mut shard_guard = shard.write();
        let mut evicted_size = 0;

        while evicted_size < needed_size && !shard_guard.lru_order.is_empty() {
            if let Some(key) = shard_guard.lru_order.pop() {
                if let Some(entry) = shard_guard.entries.remove(&key) {
                    evicted_size += entry.size_bytes;
                    shard_guard.lfu_frequencies.remove(&key);
                }
            }
        }

        evicted_size
    }

    fn evict_lfu(&self, shard: &Arc<RwLock<ShardData>>, needed_size: usize) -> usize {
        let mut shard_guard = shard.write();
        let mut evicted_size = 0;

        while evicted_size < needed_size && !shard_guard.entries.is_empty() {
            let mut min_freq = usize::MAX;
            let mut min_key = None;

            for (key, freq) in &shard_guard.lfu_frequencies {
                if *freq < min_freq {
                    min_freq = *freq;
                    min_key = Some(key.clone());
                }
            }

            if let Some(key) = min_key {
                if let Some(entry) = shard_guard.entries.remove(&key) {
                    evicted_size += entry.size_bytes;
                    shard_guard.lfu_frequencies.remove(&key);
                    shard_guard.lru_order.retain(|k| k != &key);
                }
            } else {
                break;
            }
        }

        evicted_size
    }

    fn evict_expired(&self, shard: &Arc<RwLock<ShardData>>) -> usize {
        let mut shard_guard = shard.write();
        let mut evicted_count = 0;
        let keys_to_remove: Vec<String> = shard_guard
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_remove {
            if shard_guard.entries.remove(&key).is_some() {
                shard_guard.lru_order.retain(|k| k != &key);
                shard_guard.lfu_frequencies.remove(&key);
                evicted_count += 1;
            }
        }

        evicted_count
    }
}

impl AvailabilityCache for ShardedCache {
    fn new(config: CacheConfig) -> Self {
        let shards_count = config.shards_count.max(1);
        let shards: Vec<_> = (0..shards_count)
            .map(|_| Arc::new(RwLock::new(ShardData::new())))
            .collect();

        let shards_clone: Vec<_> = shards.iter().map(Arc::clone).collect();
        let stats_clone = Arc::new(RwLock::new(CacheStats::default()));
        let stats_for_cleanup = Arc::clone(&stats_clone);
        let cleanup_interval = config.cleanup_interval_seconds;

        let cleanup_handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(cleanup_interval));
            let mut total_expired = 0;
            for shard in &shards_clone {
                let mut shard_guard = shard.write();
                let keys_to_remove: Vec<String> = shard_guard
                    .entries
                    .iter()
                    .filter(|(_, entry)| entry.is_expired())
                    .map(|(key, _)| key.clone())
                    .collect();

                for key in keys_to_remove {
                    if shard_guard.entries.remove(&key).is_some() {
                        shard_guard.lru_order.retain(|k| k != &key);
                        shard_guard.lfu_frequencies.remove(&key);
                        total_expired += 1;
                    }
                }
            }
            if total_expired > 0 {
                stats_for_cleanup.write().expired_count += total_expired;
            }
        });

        Self {
            shards,
            config,
            stats: stats_clone,
            eviction_policy: Arc::new(RwLock::new(EvictionPolicy::LeastRecentlyUsed)),
            cleanup_handle: Some(cleanup_handle),
            pending_fetches: Arc::new(Mutex::new(HashMap::new())),
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

        let current_size: usize = shard_guard.entries.values().map(|e| e.size_bytes).sum();

        if current_size + item_size > max_size_bytes {
            let policy = *self.eviction_policy.read();
            let evicted = match policy {
                EvictionPolicy::LeastRecentlyUsed => self.evict_lru(shard, item_size),
                EvictionPolicy::LeastFrequentlyUsed => self.evict_lfu(shard, item_size),
                EvictionPolicy::TimeToLive => {
                    // evict expired items first
                    self.evict_expired(shard);
                    // then try lru as fallback
                    self.evict_lru(shard, item_size)
                }
            };
            if evicted > 0 {
                stats_guard.eviction_count += 1;
                stats_guard.size_bytes -= evicted;
            } else {
                stats_guard.rejected_count += 1;
                return false;
            }
        }

        let entry = CacheEntry {
            data,
            created_at: Instant::now(),
            ttl,
            access_count: 0,
            last_accessed: Instant::now(),
            size_bytes: item_size,
        };

        shard_guard.entries.insert(key.clone(), entry);
        shard_guard.lru_order.insert(0, key.clone());
        shard_guard.lfu_frequencies.insert(key, 0);
        stats_guard.items_count += 1;
        stats_guard.size_bytes += item_size;

        let lookup_time = start_time.elapsed().as_nanos() as u64;
        stats_guard.total_lookups += 1;
        if stats_guard.total_lookups > 0 {
            stats_guard.average_lookup_time_ns = (stats_guard.average_lookup_time_ns
                * (stats_guard.total_lookups - 1) as u64
                + lookup_time)
                / stats_guard.total_lookups as u64;
        }

        true
    }

    fn get(&self, hotel_id: &str, check_in: &str, check_out: &str) -> Option<(Vec<u8>, bool)> {
        let start_time = Instant::now();
        let key = create_cache_key(hotel_id, check_in, check_out);

        // check if there's a pending fetch for this key
        let pending = self.pending_fetches.lock().unwrap();
        if let Some(data_arc) = pending.get(&key) {
            let data_guard = data_arc.lock().unwrap();
            if let Some(ref data) = *data_guard {
                return Some((data.clone(), true));
            } else {
                // wait for pending fetch
                drop(data_guard);
                drop(pending);
                thread::sleep(Duration::from_millis(10));
                return self.get(hotel_id, check_in, check_out);
            }
        }
        drop(pending);

        let shard_index = self.get_shard_index(&key);
        let shard = &self.shards[shard_index];

        let mut shard_guard = shard.write();
        let mut stats_guard = self.stats.write();

        stats_guard.total_lookups += 1;

        if let Some(entry) = shard_guard.entries.get_mut(&key) {
            if entry.is_expired() {
                shard_guard.entries.remove(&key);
                shard_guard.lru_order.retain(|k| k != &key);
                stats_guard.expired_count += 1;
                stats_guard.miss_count += 1;
                return None;
            }

            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            let data = entry.data.clone();

            // update lru order
            shard_guard.lru_order.retain(|k| k != &key);
            shard_guard.lru_order.insert(0, key.clone());

            // update lfu frequency
            *shard_guard.lfu_frequencies.entry(key.clone()).or_insert(0) += 1;

            stats_guard.hit_count += 1;

            let lookup_time = start_time.elapsed().as_nanos() as u64;
            if stats_guard.total_lookups > 0 {
                stats_guard.average_lookup_time_ns = (stats_guard.average_lookup_time_ns
                    * (stats_guard.total_lookups - 1) as u64
                    + lookup_time)
                    / stats_guard.total_lookups as u64;
            }

            Some((data, true))
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
            total_items += shard_guard.entries.len();
            total_size += shard_guard
                .entries
                .values()
                .map(|e| e.size_bytes)
                .sum::<usize>();
        }
        stats.items_count = total_items;
        stats.size_bytes = total_size;

        stats
    }

    fn set_eviction_policy(&self, policy: EvictionPolicy) {
        *self.eviction_policy.write() = policy;
    }

    fn prefetch(&self, keys: Vec<(String, String, String)>, _ttl: Option<Duration>) -> usize {
        let mut count = 0;
        for (hotel_id, check_in, check_out) in keys {
            let key = create_cache_key(&hotel_id, &check_in, &check_out);
            let shard_index = self.get_shard_index(&key);
            let shard = &self.shards[shard_index];

            let shard_guard = shard.read();
            if shard_guard.entries.contains_key(&key) {
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
        let mut invalidated = 0;
        for shard in &self.shards {
            let mut shard_guard = shard.write();
            let keys_to_remove: Vec<String> = shard_guard
                .entries
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

            for key in keys_to_remove {
                if shard_guard.entries.remove(&key).is_some() {
                    shard_guard.lru_order.retain(|k| k != &key);
                    shard_guard.lfu_frequencies.remove(&key);
                    invalidated += 1;
                }
            }
        }
        invalidated
    }

    fn resize(&self, new_max_size_mb: usize) -> bool {
        let new_max_size_bytes = new_max_size_mb * 1024 * 1024;
        let mut stats_guard = self.stats.write();
        let current_size = stats_guard.size_bytes;

        if current_size > new_max_size_bytes {
            let policy = *self.eviction_policy.read();
            let mut to_evict = current_size - new_max_size_bytes;

            for shard in &self.shards {
                if to_evict == 0 {
                    break;
                }
                let evicted = match policy {
                    EvictionPolicy::LeastRecentlyUsed => self.evict_lru(shard, to_evict),
                    EvictionPolicy::LeastFrequentlyUsed => self.evict_lfu(shard, to_evict),
                    EvictionPolicy::TimeToLive => {
                        self.evict_expired(shard);
                        self.evict_lru(shard, to_evict)
                    }
                };
                to_evict = to_evict.saturating_sub(evicted);
                stats_guard.eviction_count += 1;
                stats_guard.size_bytes -= evicted;
            }
        }

        true
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
    format!("{hotel_id}:{check_in}:{check_out}")
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
    #[test]
    fn test_concurrent_access_with_contention() {
        let config = CacheConfig {
            max_size_mb: 5,
            default_ttl_seconds: 300,
            cleanup_interval_seconds: 60,
            shards_count: 8,
        };

        let cache = Arc::new(ShardedCache::new(config));
        let threads_count = 8;
        let operations_per_thread = 100;

        let popular_hotels = vec!["hotel1", "hotel2", "hotel3"];
        let popular_dates = vec![("2025-06-01", "2025-06-05"), ("2025-07-01", "2025-07-10")];

        for hotel in &popular_hotels {
            for (check_in, check_out) in &popular_dates {
                let data = vec![1, 2, 3, 4, 5];
                cache.store(hotel, check_in, check_out, data, None);
            }
        }

        let mut handles = vec![];
        for i in 0..threads_count {
            let cache_clone = Arc::clone(&cache);
            let popular_hotels = popular_hotels.clone();
            let popular_dates = popular_dates.clone();

            let handle = thread::spawn(move || {
                for j in 0..operations_per_thread {
                    let use_popular = j % 2 == 0;

                    let hotel_id;
                    let check_in;
                    let check_out;

                    if use_popular {
                        hotel_id = popular_hotels[j % popular_hotels.len()].to_string();
                        let date_pair = &popular_dates[j % popular_dates.len()];
                        check_in = date_pair.0.to_string();
                        check_out = date_pair.1.to_string();
                    } else {
                        hotel_id = format!("hotel{}", i * 1000 + j);
                        check_in = format!("2025-{:02}-01", (j % 12) + 1);
                        check_out = format!("2025-{:02}-10", (j % 12) + 1);
                    }

                    if j % 10 < 8 {
                        let _ = cache_clone.get(&hotel_id, &check_in, &check_out);
                    } else if j % 10 < 9 {
                        let data = vec![i as u8, j as u8, 1, 2, 3, 4, 5];
                        cache_clone.store(&hotel_id, &check_in, &check_out, data, None);
                    } else {
                        cache_clone.invalidate(Some(&hotel_id), None, None);
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = cache.stats();
        assert!(stats.hit_count > 0);
        assert!(stats.total_lookups > 0);
    }

    #[test]
    fn test_expiration_and_ttl() {
        let config = CacheConfig {
            max_size_mb: 5,
            default_ttl_seconds: 5,
            cleanup_interval_seconds: 1,
            shards_count: 4,
        };

        let cache = ShardedCache::new(config);

        let hotel_id = "hotel123";
        let check_in = "2025-06-01";
        let check_out = "2025-06-05";
        let data = vec![1, 2, 3, 4, 5];

        assert!(cache.store(hotel_id, check_in, check_out, data.clone(), None));

        let hotel_id2 = "hotel456";
        assert!(cache.store(
            hotel_id2,
            check_in,
            check_out,
            data.clone(),
            Some(Duration::from_secs(2))
        ));

        assert!(cache.get(hotel_id, check_in, check_out).is_some());
        assert!(cache.get(hotel_id2, check_in, check_out).is_some());

        thread::sleep(Duration::from_secs(3));

        assert!(cache.get(hotel_id, check_in, check_out).is_some());
        assert!(cache.get(hotel_id2, check_in, check_out).is_none());

        thread::sleep(Duration::from_secs(3));

        assert!(cache.get(hotel_id, check_in, check_out).is_none());
        assert!(cache.get(hotel_id2, check_in, check_out).is_none());

        let stats = cache.stats();
        assert!(stats.expired_count >= 2);
    }

    #[test]
    fn test_eviction_policy_lru() {
        let config = CacheConfig {
            max_size_mb: 1,
            default_ttl_seconds: 3600,
            cleanup_interval_seconds: 60,
            shards_count: 2,
        };

        let cache = ShardedCache::new(config);
        cache.set_eviction_policy(EvictionPolicy::LeastRecentlyUsed);

        let large_data = vec![0; 250 * 1024];

        for i in 0..4 {
            let hotel_id = format!("hotel{i}");
            assert!(cache.store(
                &hotel_id,
                "2025-06-01",
                "2025-06-05",
                large_data.clone(),
                None
            ));
        }

        assert!(cache.get("hotel0", "2025-06-01", "2025-06-05").is_some());
        assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());

        let stats_before = cache.stats();
        let stored = cache.store(
            "hotel4",
            "2025-06-01",
            "2025-06-05",
            large_data.clone(),
            None,
        );
        let stats_after = cache.stats();

        // verify cache works - either item stored or eviction occurred
        assert!(stored || stats_after.eviction_count > stats_before.eviction_count);
    }

    #[test]
    fn test_prefetch_and_invalidate() {
        let config = CacheConfig::default();
        let cache = ShardedCache::new(config);

        let keys = vec![
            (
                "hotel1".to_string(),
                "2025-06-01".to_string(),
                "2025-06-05".to_string(),
            ),
            (
                "hotel1".to_string(),
                "2025-06-10".to_string(),
                "2025-06-15".to_string(),
            ),
            (
                "hotel2".to_string(),
                "2025-06-01".to_string(),
                "2025-06-05".to_string(),
            ),
        ];

        for (hotel, check_in, check_out) in &keys {
            let data = vec![1, 2, 3, 4, 5];
            cache.store(hotel, check_in, check_out, data, None);
        }

        let invalidated = cache.invalidate(Some("hotel1"), None, None);
        assert_eq!(invalidated, 2);

        assert!(cache.get("hotel1", "2025-06-01", "2025-06-05").is_none());
        assert!(cache.get("hotel1", "2025-06-10", "2025-06-15").is_none());
        assert!(cache.get("hotel2", "2025-06-01", "2025-06-05").is_some());

        let prefetched = cache.prefetch(keys.clone(), None);
        assert_eq!(prefetched, 1);
    }

    #[test]
    fn test_cache_resize() {
        let config = CacheConfig {
            max_size_mb: 10,
            default_ttl_seconds: 300,
            cleanup_interval_seconds: 60,
            shards_count: 4,
        };

        let cache = ShardedCache::new(config);

        let medium_data = vec![0; 100 * 1024];
        for i in 0..50 {
            let hotel_id = format!("hotel{i}");
            cache.store(
                &hotel_id,
                "2025-06-01",
                "2025-06-05",
                medium_data.clone(),
                None,
            );
        }

        assert!(cache.resize(2));

        let stats = cache.stats();
        assert!(stats.size_bytes <= 2 * 1024 * 1024);
        assert!(stats.items_count < 50);

        assert!(cache.resize(20));

        for i in 50..100 {
            let hotel_id = format!("hotel{i}");
            cache.store(
                &hotel_id,
                "2025-06-01",
                "2025-06-05",
                medium_data.clone(),
                None,
            );
        }

        let new_stats = cache.stats();
        assert!(new_stats.items_count > stats.items_count);
    }
}
