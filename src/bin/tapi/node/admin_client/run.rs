use crate::AdminAction;

pub(crate) async fn run(
    action: AdminAction,
    #[cfg(feature = "tls")] tls_config: Option<tapirs::tls::TlsConfig>,
) {
    #[cfg(feature = "tls")]
    let tls_connector = tls_config.as_ref().map(|c| {
        tapirs::tls::ReloadableTlsConnector::new(c)
            .unwrap_or_else(|e| panic!("admin TLS config error: {e}"))
    });

    // Multi-node operations with their own orchestration.
    match &action {
        AdminAction::Backup {
            admin_addrs,
            output,
        } => {
            let addrs: Vec<String> =
                admin_addrs.split(',').map(|s| s.trim().to_string()).collect();
            if let Err(e) = tapirs::node::node_client::backup_cluster::backup_cluster(
                addrs,
                output,
                #[cfg(feature = "tls")]
                &tls_connector,
            )
            .await
            {
                eprintln!("Backup failed: {e}");
                std::process::exit(1);
            }
            return;
        }
        AdminAction::Restore {
            backup_dir,
            admin_addrs,
            base_port,
        } => {
            let addrs: Vec<String> =
                admin_addrs.split(',').map(|s| s.trim().to_string()).collect();
            if let Err(e) = tapirs::node::node_client::restore_cluster::restore_cluster(
                addrs,
                backup_dir,
                *base_port,
                #[cfg(feature = "tls")]
                &tls_connector,
            )
            .await
            {
                eprintln!("Restore failed: {e}");
                std::process::exit(1);
            }
            return;
        }
        _ => {}
    }

    // Single-node operations (existing code).
    let (addr, request) = match action {
        AdminAction::Status { admin_listen_addr } => (
            admin_listen_addr,
            r#"{"command":"status"}"#.to_string(),
        ),
        AdminAction::AddReplica {
            admin_listen_addr,
            shard,
            listen_addr,
            storage,
            membership,
        } => {
            let storage_str = match storage {
                crate::StorageBackend::Memory => "memory",
                crate::StorageBackend::Disk => "disk",
            };
            let request = if membership.is_empty() {
                format!(
                    r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}","storage":"{storage_str}"}}"#
                )
            } else {
                let membership_json: Vec<String> =
                    membership.iter().map(|a| format!("\"{}\"", a)).collect();
                format!(
                    r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}","storage":"{storage_str}","membership":[{}]}}"#,
                    membership_json.join(",")
                )
            };
            (admin_listen_addr, request)
        }
        AdminAction::RemoveReplica {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"remove_replica","shard":{shard}}}"#),
        ),
        AdminAction::ViewChange {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"view_change","shard":{shard}}}"#),
        ),
        AdminAction::Leave {
            admin_listen_addr,
            shard,
        } => (
            admin_listen_addr,
            format!(r#"{{"command":"leave","shard":{shard}}}"#),
        ),
        AdminAction::WaitReady {
            admin_listen_addr,
            timeout,
        } => {
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(timeout);
            loop {
                if let Ok(Some(resp)) = tapirs::node::node_client::raw_admin_exchange(
                    &admin_listen_addr,
                    r#"{"command":"status"}"#,
                    #[cfg(feature = "tls")]
                    &tls_connector,
                )
                .await
                    && serde_json::from_str::<serde_json::Value>(&resp).is_ok()
                {
                    println!("ready");
                    return;
                }
                if std::time::Instant::now() >= deadline {
                    eprintln!(
                        "timeout: admin server at {} not ready after {}s",
                        admin_listen_addr, timeout
                    );
                    std::process::exit(1);
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        AdminAction::Backup { .. } | AdminAction::Restore { .. } => unreachable!(),
    };

    match tapirs::node::node_client::raw_admin_exchange(
        &addr,
        &request,
        #[cfg(feature = "tls")]
        &tls_connector,
    )
    .await
    {
        Ok(Some(response)) => {
            // Pretty-print if it's valid JSON, otherwise print raw.
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
                println!("{}", serde_json::to_string_pretty(&json).unwrap());
            } else {
                println!("{response}");
            }
        }
        Ok(None) => {
            eprintln!("no response from {addr}");
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
