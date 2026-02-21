use super::*;

#[tokio::test]
async fn increment_sequential_3() {
    increment_sequential_timeout(3).await;
}

#[tokio::test]
async fn increment_sequential_7() {
    increment_sequential_timeout(7).await;
}

async fn increment_sequential_timeout(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        increment_sequential(num_replicas),
    )
    .await
    .unwrap();
}

async fn increment_sequential(num_replicas: usize) {
    let (_replicas, clients) = build_kv(true, num_replicas, 1);

    let mut committed = 0;
    for _ in 0..10 {
        println!("^^^^^^^^^^^^^^^^^^^ BEGINNING TXN");
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap().unwrap_or_default();
        txn.put(0, Some(old + 1));
        if txn.commit().await.is_some() {
            assert_eq!(committed, old);
            committed += 1;
        }

        Transport::sleep(Duration::from_millis(1000)).await;
    }

    eprintln!("committed = {committed}");
    assert!(committed > 0);
}

#[tokio::test]
async fn increment_parallel_3() {
    increment_parallel_timeout(3).await;
}

#[tokio::test]
async fn increment_parallel_7() {
    increment_parallel_timeout(7).await;
}

async fn increment_parallel_timeout(num_replicas: usize) {
    timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        increment_parallel(num_replicas),
    )
    .await
    .unwrap();
}

async fn increment_parallel(num_replicas: usize) {
    let (_replicas, clients) = build_kv(true, num_replicas, 2);

    let add = || async {
        let txn = clients[0].begin();
        let old = txn.get(0).await.unwrap().unwrap_or_default();
        txn.put(0, Some(old + 1));
        txn.commit().await.is_some()
    };

    let committed = join_all((0..5).map(|_| add()))
        .await
        .into_iter()
        .filter(|ok| *ok)
        .count() as i64;

    Transport::sleep(Duration::from_secs(3)).await;

    let txn = clients[1].begin();
    let result = txn.get(0).await.unwrap().unwrap_or_default();
    eprintln!("INCREMENT TEST result={result} committed={committed}");
    println!("{} {}", txn.commit().await.is_some(), result == committed);
}
