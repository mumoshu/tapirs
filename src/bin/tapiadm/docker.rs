use serde::{Deserialize, Serialize};
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::process::Command;

const DISCOVERY_IP: &str = "172.28.0.2";
const SHARD_MGR_IP: &str = "172.28.0.3";
const NODE_IPS: [&str; 3] = ["172.28.0.11", "172.28.0.12", "172.28.0.13"];
const NODE_NAMES: [&str; 3] = ["node1", "node2", "node3"];
const DISCOVERY_PORT: u16 = 8080;
const SHARD_MGR_PORT: u16 = 9001;
const REPLICA_BASE_PORT: u16 = 6000;
const NUM_SHARDS: u32 = 2;
const WORK_DIR: &str = ".tapiadm";

// Host admin ports for initial nodes: 9011, 9012, 9013.
const HOST_ADMIN_BASE: u16 = 9011;

const DOCKERFILE: &str = include_str!("docker/Dockerfile");
const COMPOSE_YML: &str = include_str!("docker/docker-compose.yml");
const DOCKERIGNORE: &str = include_str!("docker/dockerignore");

#[derive(Serialize, Deserialize, Clone)]
struct DynamicNode {
    name: String,
    ip: String,
    host_admin_port: u16,
}

fn project_root() -> PathBuf {
    std::env::current_dir().expect("failed to get current dir")
}

fn work_dir() -> PathBuf {
    project_root().join(WORK_DIR)
}

fn nodes_json_path() -> PathBuf {
    work_dir().join("nodes.json")
}

fn read_dynamic_nodes() -> Vec<DynamicNode> {
    let path = nodes_json_path();
    if !path.exists() {
        return vec![];
    }
    let data = std::fs::read_to_string(&path).unwrap_or_else(|_| "[]".to_string());
    serde_json::from_str(&data).unwrap_or_default()
}

fn write_dynamic_nodes(nodes: &[DynamicNode]) {
    let path = nodes_json_path();
    let json = serde_json::to_string_pretty(nodes).expect("serialize nodes.json");
    std::fs::write(&path, json).expect("write nodes.json");
}

fn docker_compose(args: &[&str]) -> Result<(), String> {
    let wd = work_dir();
    let status = Command::new("docker")
        .arg("compose")
        .args(args)
        .current_dir(&wd)
        .status()
        .map_err(|e| format!("failed to run docker compose: {e}"))?;
    if !status.success() {
        return Err(format!("docker compose {} failed", args.join(" ")));
    }
    Ok(())
}

fn docker_compose_output(args: &[&str]) -> Result<String, String> {
    let wd = work_dir();
    let output = Command::new("docker")
        .arg("compose")
        .args(args)
        .current_dir(&wd)
        .output()
        .map_err(|e| format!("failed to run docker compose: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("docker compose {} failed: {stderr}", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn docker_cmd(args: &[&str]) -> Result<(), String> {
    let status = Command::new("docker")
        .args(args)
        .status()
        .map_err(|e| format!("failed to run docker: {e}"))?;
    if !status.success() {
        return Err(format!("docker {} failed", args.join(" ")));
    }
    Ok(())
}

fn docker_output(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("failed to run docker: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("docker {} failed: {stderr}", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn wait_for_tcp(addr: &str, timeout_secs: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if std::net::TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("timeout waiting for {addr}"));
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

fn send_admin_command(addr: &str, json: &str) -> Result<String, String> {
    let mut stream =
        std::net::TcpStream::connect(addr).map_err(|e| format!("connect to {addr}: {e}"))?;
    let mut line = json.to_string();
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .map_err(|e| format!("send to {addr}: {e}"))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|e| format!("shutdown write to {addr}: {e}"))?;
    let mut response = String::new();
    let reader = std::io::BufReader::new(&stream);
    for line_result in reader.lines() {
        let l = line_result.map_err(|e| format!("read from {addr}: {e}"))?;
        response = l;
        break;
    }
    Ok(response)
}

fn http_post(host: &str, port: u16, path: &str, body: &str) -> Result<String, String> {
    let addr = format!("{host}:{port}");
    let mut stream =
        std::net::TcpStream::connect(&addr).map_err(|e| format!("connect to {addr}: {e}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("send to {addr}: {e}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| format!("read from {addr}: {e}"))?;
    let resp_str = String::from_utf8_lossy(&response).to_string();
    let body = resp_str
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok(body)
}

pub fn up() -> Result<(), String> {
    let wd = work_dir();
    let root = project_root();

    // 1. Create working directory and write templates.
    std::fs::create_dir_all(&wd).map_err(|e| format!("create {WORK_DIR}: {e}"))?;
    std::fs::write(wd.join("Dockerfile"), DOCKERFILE)
        .map_err(|e| format!("write Dockerfile: {e}"))?;
    std::fs::write(wd.join("docker-compose.yml"), COMPOSE_YML)
        .map_err(|e| format!("write docker-compose.yml: {e}"))?;

    // 2. Write .dockerignore to project root (build context).
    std::fs::write(root.join(".dockerignore"), DOCKERIGNORE)
        .map_err(|e| format!("write .dockerignore: {e}"))?;

    // Initialize nodes.json.
    write_dynamic_nodes(&[]);

    // 3. Build image.
    println!("Building Docker image...");
    docker_compose(&["build"])?;

    // 4. Start discovery.
    println!("Starting discovery service...");
    docker_compose(&["up", "-d", "discovery"])?;
    wait_for_tcp(&format!("127.0.0.1:{DISCOVERY_PORT}"), 30)?;
    println!("  Discovery ready at 127.0.0.1:{DISCOVERY_PORT}");

    // 5. Start shard-manager.
    println!("Starting shard-manager...");
    docker_compose(&["up", "-d", "shard-manager"])?;
    wait_for_tcp(&format!("127.0.0.1:{SHARD_MGR_PORT}"), 30)?;
    println!("  Shard-manager ready at 127.0.0.1:{SHARD_MGR_PORT}");

    // 6. Start nodes.
    println!("Starting nodes...");
    docker_compose(&["up", "-d", "node1", "node2", "node3"])?;
    for (i, _ip) in NODE_IPS.iter().enumerate() {
        let port = HOST_ADMIN_BASE + i as u16;
        wait_for_tcp(&format!("127.0.0.1:{port}"), 30)?;
        println!("  {} ready at 127.0.0.1:{port}", NODE_NAMES[i]);
    }

    // 7. Bootstrap replicas.
    println!("Bootstrapping replicas...");
    for shard in 0..NUM_SHARDS {
        let mut replicas: Vec<String> = Vec::new();
        for (i, ip) in NODE_IPS.iter().enumerate() {
            let listen_port = REPLICA_BASE_PORT + shard as u16;
            let listen_addr = format!("{ip}:{listen_port}");
            let admin_addr = format!("127.0.0.1:{}", HOST_ADMIN_BASE + i as u16);

            let cmd = format!(
                r#"{{"command":"add_replica","shard":{shard},"listen_addr":"{listen_addr}"}}"#
            );
            let resp = send_admin_command(&admin_addr, &cmd)?;
            if !resp.contains("\"ok\":true") && !resp.contains("\"ok\": true") {
                return Err(format!(
                    "add_replica shard {shard} on {}: {resp}",
                    NODE_NAMES[i]
                ));
            }
            println!(
                "  Shard {shard}: replica on {} at {listen_addr}",
                NODE_NAMES[i]
            );

            replicas.push(listen_addr);

            // Register cumulative membership with discovery.
            let replicas_json: Vec<String> =
                replicas.iter().map(|r| format!("\"{r}\"")).collect();
            let body = format!(r#"{{"replicas":[{}]}}"#, replicas_json.join(","));
            let disc_resp = http_post(
                "127.0.0.1",
                DISCOVERY_PORT,
                &format!("/v1/shards/{shard}"),
                &body,
            )?;
            if !disc_resp.contains("\"ok\":true") && !disc_resp.contains("\"ok\": true") {
                return Err(format!(
                    "discovery register shard {shard}: {disc_resp}"
                ));
            }

            // Wait for view change settlement.
            if replicas.len() > 1 {
                println!("  Waiting for view change settlement...");
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }

    // 8. Register shard layout with shard-manager.
    println!("Registering shard layout with shard-manager...");
    let split_key = compute_split_key(NUM_SHARDS);
    for shard in 0..NUM_SHARDS {
        let (start, end) = shard_key_range(shard, NUM_SHARDS, &split_key);
        let body = serde_json::json!({
            "shard": shard,
            "key_range_start": start,
            "key_range_end": end,
        })
        .to_string();
        let resp = http_post("127.0.0.1", SHARD_MGR_PORT, "/v1/register", &body)?;
        if !resp.contains("\"ok\":true") && !resp.contains("\"ok\": true") {
            return Err(format!("register shard {shard} with shard-manager: {resp}"));
        }
        println!("  Shard {shard}: key range [{}, {})", start.as_deref().unwrap_or(""), end.as_deref().unwrap_or(""));
    }

    // 9. Generate client.toml.
    let mut client_toml = String::new();
    client_toml.push_str(&format!(
        "discovery_url = \"http://{DISCOVERY_IP}:{DISCOVERY_PORT}\"\n\n"
    ));
    for shard in 0..NUM_SHARDS {
        let (start, end) = shard_key_range(shard, NUM_SHARDS, &split_key);
        client_toml.push_str("[[shards]]\n");
        client_toml.push_str(&format!("id = {shard}\n"));
        let replicas: Vec<String> = NODE_IPS
            .iter()
            .map(|ip| format!("{ip}:{}", REPLICA_BASE_PORT + shard as u16))
            .collect();
        let replicas_toml: Vec<String> = replicas.iter().map(|r| format!("\"{r}\"")).collect();
        client_toml.push_str(&format!("replicas = [{}]\n", replicas_toml.join(", ")));
        if let Some(s) = &start {
            client_toml.push_str(&format!("key_range_start = \"{s}\"\n"));
        }
        if let Some(e) = &end {
            client_toml.push_str(&format!("key_range_end = \"{e}\"\n"));
        }
        client_toml.push('\n');
    }
    std::fs::write(wd.join("client.toml"), &client_toml)
        .map_err(|e| format!("write client.toml: {e}"))?;

    // 10. Print instructions.
    println!();
    println!("TAPIR cluster is ready!");
    println!();
    println!("REPL access:");
    println!("  cd {WORK_DIR} && docker compose run --rm client");
    println!("  # begin; put hello world; commit; begin; get hello; abort");
    println!();
    println!("Nodes:");
    for (i, ip) in NODE_IPS.iter().enumerate() {
        println!(
            "  {}  {ip}  admin: 127.0.0.1:{}",
            NODE_NAMES[i],
            HOST_ADMIN_BASE + i as u16
        );
    }
    println!();
    println!("Add a new node:");
    println!("  tapiadm docker add node");
    println!();
    println!("List nodes/replicas:");
    println!("  tapiadm docker get nodes");
    println!("  tapiadm docker get replicas");
    println!();
    println!("Tear down:");
    println!("  tapiadm docker down");

    Ok(())
}

pub fn down() -> Result<(), String> {
    let wd = work_dir();
    let root = project_root();

    // 1. Remove dynamically added nodes.
    let nodes = read_dynamic_nodes();
    for node in &nodes {
        println!("Removing dynamic node {}...", node.name);
        let _ = docker_cmd(&["stop", &node.name]);
        let _ = docker_cmd(&["rm", "-f", &node.name]);
        let vol = format!("{}-data", node.name);
        let _ = docker_cmd(&["volume", "rm", &vol]);
    }

    // 2. docker compose down.
    if wd.join("docker-compose.yml").exists() {
        println!("Stopping cluster...");
        docker_compose(&["down", "-v"])?;
    }

    // 3. Clean up .dockerignore.
    let dockerignore = root.join(".dockerignore");
    if dockerignore.exists() {
        let _ = std::fs::remove_file(&dockerignore);
    }

    println!("Cluster torn down.");
    Ok(())
}

pub fn add_node(name: Option<String>) -> Result<(), String> {
    let mut nodes = read_dynamic_nodes();

    // Auto-generate name.
    let name = name.unwrap_or_else(|| {
        let n = nodes.len() + 4; // 1-3 are initial nodes
        format!("tapir-node-{n}")
    });

    // Check name not already used.
    if nodes.iter().any(|n| n.name == name) {
        return Err(format!("node {name} already exists"));
    }

    // Find next available host admin port.
    let mut port = HOST_ADMIN_BASE + NODE_IPS.len() as u16; // start at 9014
    let used_ports: Vec<u16> = nodes.iter().map(|n| n.host_admin_port).collect();
    while used_ports.contains(&port) {
        port += 1;
    }

    // Get network name from compose project.
    let network = get_compose_network()?;

    // Get image name.
    let image = "tapiadm-tapi";

    // Run the container.
    println!("Creating node {name}...");
    docker_cmd(&[
        "run",
        "-d",
        "--name",
        &name,
        "--network",
        &network,
        "-p",
        &format!("{port}:9000"),
        "-v",
        &format!("{name}-data:/data"),
        image,
        "node",
        "--admin_listen_addr",
        "0.0.0.0:9000",
        "--persist_dir",
        "/data",
        "--discovery_url",
        &format!("http://{DISCOVERY_IP}:{DISCOVERY_PORT}"),
        "--shard_manager_url",
        &format!("http://{SHARD_MGR_IP}:{SHARD_MGR_PORT}"),
    ])?;

    // Discover container IP.
    let ip = docker_output(&[
        "inspect",
        "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        &name,
    ])?;

    // Wait for admin port.
    wait_for_tcp(&format!("127.0.0.1:{port}"), 30)?;

    // Save to nodes.json.
    nodes.push(DynamicNode {
        name: name.clone(),
        ip: ip.clone(),
        host_admin_port: port,
    });
    write_dynamic_nodes(&nodes);

    println!();
    println!("Node \"{name}\" is ready.");
    println!("  Container IP:  {ip}");
    println!("  Admin port:    127.0.0.1:{port} (host) / {ip}:9000 (internal)");
    println!();
    println!("To add a replica for shard 0 on port 6000:");
    println!("  tapi admin add-replica --admin_listen_addr 127.0.0.1:{port} --shard 0 --listen_addr {ip}:6000");
    println!("To remove a replica:");
    println!("  tapi admin remove-replica --admin_listen_addr 127.0.0.1:{port} --shard 0");

    Ok(())
}

pub fn remove_node(name: &str) -> Result<(), String> {
    let mut nodes = read_dynamic_nodes();
    let idx = nodes
        .iter()
        .position(|n| n.name == name)
        .ok_or_else(|| format!("node {name} not found in nodes.json"))?;

    println!("Removing node {name}...");
    let _ = docker_cmd(&["stop", name]);
    let _ = docker_cmd(&["rm", "-f", name]);
    let vol = format!("{name}-data");
    let _ = docker_cmd(&["volume", "rm", &vol]);

    nodes.remove(idx);
    write_dynamic_nodes(&nodes);

    println!("Node {name} removed.");
    Ok(())
}

pub fn get_nodes() -> Result<(), String> {
    println!(
        "{:<20} {:<16} {:<24} {}",
        "NAME", "CONTAINER IP", "ADMIN (HOST)", "STATUS"
    );

    // Initial compose nodes.
    for (i, ip) in NODE_IPS.iter().enumerate() {
        let port = HOST_ADMIN_BASE + i as u16;
        let status = if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            "running"
        } else {
            "unreachable"
        };
        println!(
            "{:<20} {:<16} {:<24} {}",
            NODE_NAMES[i],
            ip,
            format!("127.0.0.1:{port}"),
            status
        );
    }

    // Dynamic nodes.
    let nodes = read_dynamic_nodes();
    for node in &nodes {
        let status =
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", node.host_admin_port)).is_ok()
            {
                "running"
            } else {
                "unreachable"
            };
        println!(
            "{:<20} {:<16} {:<24} {}",
            node.name,
            node.ip,
            format!("127.0.0.1:{}", node.host_admin_port),
            status
        );
    }

    Ok(())
}

pub fn get_replicas() -> Result<(), String> {
    println!("{:<8} {:<20} {}", "SHARD", "NODE", "LISTEN ADDR");

    // Collect all nodes with their admin addresses.
    let mut all_nodes: Vec<(String, String)> = Vec::new(); // (name, host_admin_addr)
    for (i, _ip) in NODE_IPS.iter().enumerate() {
        all_nodes.push((
            NODE_NAMES[i].to_string(),
            format!("127.0.0.1:{}", HOST_ADMIN_BASE + i as u16),
        ));
    }
    let nodes = read_dynamic_nodes();
    for node in &nodes {
        all_nodes.push((
            node.name.clone(),
            format!("127.0.0.1:{}", node.host_admin_port),
        ));
    }

    // Query each node's status.
    #[derive(Deserialize)]
    struct StatusResp {
        #[serde(default)]
        shards: Option<Vec<ShardInfo>>,
    }
    #[derive(Deserialize)]
    struct ShardInfo {
        shard: u32,
        listen_addr: String,
    }

    let mut entries: Vec<(u32, String, String)> = Vec::new(); // (shard, node, addr)
    for (name, admin_addr) in &all_nodes {
        match send_admin_command(admin_addr, r#"{"command":"status"}"#) {
            Ok(resp) => {
                if let Ok(status) = serde_json::from_str::<StatusResp>(&resp) {
                    if let Some(shards) = status.shards {
                        for s in shards {
                            entries.push((s.shard, name.clone(), s.listen_addr));
                        }
                    }
                }
            }
            Err(_) => {
                // Node unreachable, skip.
            }
        }
    }

    entries.sort_by_key(|(shard, _, _)| *shard);
    for (shard, node, addr) in &entries {
        println!("{:<8} {:<20} {}", shard, node, addr);
    }

    Ok(())
}

fn get_compose_network() -> Result<String, String> {
    // Try to get the network name from docker compose.
    let output = docker_compose_output(&["config", "--format", "json"])?;
    // Parse JSON to find network names.
    if let Ok(config) = serde_json::from_str::<serde_json::Value>(&output) {
        if let Some(networks) = config.get("networks") {
            if let Some(obj) = networks.as_object() {
                for (_, net) in obj {
                    if let Some(name) = net.get("name") {
                        if let Some(s) = name.as_str() {
                            if s.contains("tapir-net") {
                                return Ok(s.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    // Fallback: compose project name + "_tapir-net"
    let wd = work_dir();
    let project_name = wd
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("tapiadm");
    Ok(format!("{project_name}_tapir-net"))
}

/// Compute the split key for evenly partitioning a-z across N shards.
fn compute_split_key(n: u32) -> Vec<String> {
    let chars: Vec<char> = ('a'..='z').collect();
    let per = chars.len() / n as usize;
    (1..n)
        .map(|i| chars[i as usize * per].to_string())
        .collect()
}

/// Get the key range (start, end) for a given shard index.
fn shard_key_range(
    shard: u32,
    total: u32,
    split_keys: &[String],
) -> (Option<String>, Option<String>) {
    let start = if shard == 0 {
        None
    } else {
        Some(split_keys[shard as usize - 1].clone())
    };
    let end = if shard == total - 1 {
        None
    } else {
        Some(split_keys[shard as usize].clone())
    };
    (start, end)
}
