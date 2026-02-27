use super::*;
use crate::tapir::{Key, Value};
use crate::tapir::client::Transaction;
use crate::TapirTransport;

async fn commit_with_fault<K: Key, V: Value, T: TapirTransport<K, V>>(
    txn: Transaction<K, V, T>,
    inject: Option<Duration>,
) -> Option<TapirTimestamp> {
    if let Some(duration) = inject {
        let inner = txn.commit();
        let sleep = T::sleep(duration);
        futures::pin_mut!(sleep);
        futures::pin_mut!(inner);
        match futures::future::select(sleep, inner).await {
            futures::future::Either::Left(_) => std::future::pending().await,
            futures::future::Either::Right((result, _)) => result,
        }
    } else {
        txn.commit().await
    }
}

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
        conflicting.put(n, Some(1));
        tokio::spawn(conflicting.only_prepare());

        let txn = clients[0].begin();
        txn.put(n, Some(42));
        let result = Arc::new(Mutex::new(Option::<Option<TapirTimestamp>>::None));

        {
            let result = Arc::clone(&result);
            tokio::spawn(async move {
                let ts = commit_with_fault(txn, Some(Duration::from_millis(n as u64))).await;
                *result.lock().unwrap() = Some(ts);
            });
        }

        Transport::sleep(Duration::from_millis(rng.gen_range(0..100u64))).await;

        for i in 0..128 {
            // Inconsistent read (1 replica, fast) is fine for polling.
            // Just drop the RW txn without committing.
            let txn = clients[1].begin();
            let read = txn.get(n).await.unwrap();
            drop(txn);
            println!("{n} try {i} read {read:?}");

            if read.is_some() {
                continue 'outer;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        panic!("never recovered");
    }
}
