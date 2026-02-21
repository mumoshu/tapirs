use super::*;

#[ignore]
#[tokio::test(start_paused = true)]
async fn coordinator_recovery_3_loop() {
    loop {
        timeout_coordinator_recovery(3).await;
    }
}

#[tokio::test(start_paused = true)]
async fn coordinator_recovery_3() {
    timeout_coordinator_recovery(3).await;
}

#[tokio::test(start_paused = true)]
async fn coordinator_recovery_5() {
    timeout_coordinator_recovery(5).await;
}

#[ignore]
#[tokio::test(start_paused = true)]
async fn coordinator_recovery_7_loop() {
    loop {
        timeout_coordinator_recovery(7).await;
    }
}

#[tokio::test(start_paused = true)]
async fn coordinator_recovery_7() {
    timeout_coordinator_recovery(7).await;
}

async fn timeout_coordinator_recovery(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 20),
        coordinator_recovery(num_replicas, 42),
    )
    .await
    .unwrap();
}

async fn coordinator_recovery(num_replicas: usize, seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let (_replicas, clients) = build_kv(true, num_replicas, 3);

    'outer: for n in (0..50).step_by(2).chain((50..500).step_by(10)) {
        let conflicting = clients[2].begin();
        conflicting.get(n).await.unwrap();
        tokio::spawn(conflicting.only_prepare());

        //let conflicting = clients[2].begin();
        //conflicting.put(n, Some(1));
        //tokio::spawn(conflicting.only_prepare());

        let txn = clients[0].begin();
        txn.put(n, Some(42));
        let result = Arc::new(Mutex::new(Option::<Option<TapirTimestamp>>::None));

        {
            let result = Arc::clone(&result);
            tokio::spawn(async move {
                let ts = txn.commit2(Some(Duration::from_millis(n as u64))).await;
                *result.lock().unwrap() = Some(ts);
            });
        }

        Transport::sleep(Duration::from_millis(rng.gen_range(0..100u64))).await;

        for i in 0..128 {
            let txn = clients[1].begin();
            let read = txn.get(n).await.unwrap();
            println!("{n} try {i} read {read:?}");

            if let Ok(Some(ts)) = timeout(Duration::from_secs(5), txn.commit()).await {
                let result = result.lock().unwrap();
                if let Some(result) = *result {
                    if let Some(result) = result {
                        assert_eq!(read.is_some(), ts > result);
                    } else {
                        assert!(read.is_none());
                    }
                }
                continue 'outer;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        panic!("never recovered");
    }
}
