use super::handle_request::handle_request;
use crate::node::Node;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub async fn handle_admin_connection<R, W>(reader: R, mut writer: W, node: &Node)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let resp = handle_request(node, &line).await;
        let mut out = serde_json::to_string(&resp).unwrap();
        out.push('\n');
        let _ = writer.write_all(out.as_bytes()).await;
    }
}
