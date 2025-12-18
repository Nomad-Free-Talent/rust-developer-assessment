use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::sync::Arc;
use std::thread;
use travel_tech_assessment::part1_cache::{AvailabilityCache, CacheConfig};
use travel_tech_assessment::part1_cache_example::ExampleCache;

// Benchmark for the cache implementation
// Note: Replace YourCacheImplementation with your actual implementation
pub fn cache_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotel_availability_cache");

    // Benchmark with different cache sizes
    for size_mb in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size_mb),
            size_mb,
            |b, &size_mb| {
                b.iter(|| {
                    // Create a cache with the specified size using the example implementation
                    let config = CacheConfig {
                        max_size_mb: size_mb,
                        default_ttl_seconds: 300,
                        cleanup_interval_seconds: 60,
                        shards_count: 16,
                    };
                    let cache = Arc::new(ExampleCache::new(config));

                    // Generate random data
                    let mut rng = thread_rng();
                    let data_size = 1024; // 1KB per item
                    let data = (0..data_size).map(|_| rng.gen::<u8>()).collect::<Vec<_>>();

                    // Create hotel IDs and dates
                    let hotel_ids = (0..100).map(|i| format!("hotel{i}")).collect::<Vec<_>>();
                    let check_ins = (1..30)
                        .map(|i| format!("2025-06-{i:02}"))
                        .collect::<Vec<_>>();
                    let check_outs = (2..31)
                        .map(|i| format!("2025-06-{i:02}"))
                        .collect::<Vec<_>>();

                    // Spawn multiple threads to simulate concurrent access
                    let mut handles = vec![];
                    for _ in 0..4 {
                        let cache = Arc::clone(&cache);
                        let hotel_ids = hotel_ids.clone();
                        let check_ins = check_ins.clone();
                        let check_outs = check_outs.clone();
                        let data = data.clone();

                        let handle = thread::spawn(move || {
                            let mut rng = thread_rng();

                            // Perform a mix of reads and writes
                            for _ in 0..250 {
                                let hotel_id = hotel_ids.choose(&mut rng).unwrap();
                                let check_in = check_ins.choose(&mut rng).unwrap();
                                let check_out = check_outs.choose(&mut rng).unwrap();

                                if rng.gen_bool(0.3) {
                                    // 30% writes
                                    cache.store(hotel_id, check_in, check_out, data.clone(), None);
                                } else {
                                    // 70% reads
                                    let _ = cache.get(hotel_id, check_in, check_out);
                                }
                            }
                        });

                        handles.push(handle);
                    }

                    // Wait for all threads to complete
                    for handle in handles {
                        handle.join().unwrap();
                    }

                    // Return stats for verification
                    black_box(cache.stats())
                });
            },
        );
    }

    group.finish();
}

// Working benchmark using the example implementation
criterion_group!(benches, cache_benchmark);
criterion_main!(benches);
