mod handle_request;
mod start;

pub(crate) use self::start::start;

use super::Node;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn handle_admin_connection<R, W>(reader: R, mut writer: W, node: &Node)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let resp = handle_request::handle_request(node, &line).await;
        let mut out = serde_json::to_string(&resp).unwrap();
        out.push('\n');
        let _ = writer.write_all(out.as_bytes()).await;
    }
}
