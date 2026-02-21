use super::*;

#[tokio::test]
async fn throughput_3_ser() {
    throughput(false, 3, 1000).await;
}

#[tokio::test]
async fn throughput_3_lin() {
    throughput(true, 3, 1000).await;
}

async fn throughput(linearizable: bool, num_replicas: usize, num_clients: usize) {
    let local = tokio::task::LocalSet::new();

    local.spawn_local(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        panic!("timeout");
    });

    // Run the local task set.
    local
        .run_until(async move {
            let (_replicas, clients) = build_kv(linearizable, num_replicas, num_clients);

            let attempted = Arc::new(AtomicU64::new(0));
            let committed = Arc::new(AtomicU64::new(0));

            for client in clients {
                let attempted = Arc::clone(&attempted);
                let committed = Arc::clone(&committed);
                tokio::task::spawn_local(async move {
                    let attempted = Arc::clone(&attempted);
                    let committed = Arc::clone(&committed);
                    loop {
                        let i = thread_rng().gen_range(0..num_clients as i64 * 10); // thread_rng().gen::<i64>();
                        let txn = client.begin();
                        let old = txn.get(i).await.unwrap().unwrap_or_default();
                        txn.put(i, Some(old + 1));
                        let c = txn.commit().await.is_some() as u64;
                        attempted.fetch_add(1, Ordering::Relaxed);
                        committed.fetch_add(c, Ordering::Relaxed);

                        tokio::time::sleep(Duration::from_millis(
                            thread_rng().gen_range(1..=num_clients as u64),
                        ))
                        .await;
                    }
                });
            }

            /*
            let guard = pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build();
            */

            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(1000)).await;

                let a = attempted.swap(0, Ordering::Relaxed);
                let c = committed.swap(0, Ordering::Relaxed);

                println!("TPUT {a}, {c}");
            }

            /*
            if let Ok(guard) = guard {
                if let Ok(report) = guard.report().build() {
                    let file = std::fs::File::create("flamegraph.svg").unwrap();
                    let mut options = pprof::flamegraph::Options::default();
                    options.image_width = Some(2500);
                    report.flamegraph_with_options(file, &mut options).unwrap();
                }
            }
            */
        })
        .await;
}
