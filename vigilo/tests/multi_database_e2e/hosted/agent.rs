//! Bounded HTTP fixture checks that a case receives its own owner's credential.
//! The task, connection set and socket all have deterministic shutdown ownership.
use std::sync::{
    Arc,
    atomic::{
        AtomicUsize,
        Ordering,
    },
};

use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpListener,
    sync::watch,
    task::{
        JoinHandle,
        JoinSet,
    },
};
use tokio_util::sync::CancellationToken;

use super::*;

pub(super) struct Agent {
    pub url: String,
    pub calls: Arc<AtomicUsize>,
    pub cancellations: Arc<AtomicUsize>,
    pub delay: Arc<AtomicUsize>,
    pub entered: Arc<tokio::sync::Notify>,
    pub hold_response: watch::Sender<bool>,
    pub drop_receipt: Arc<std::sync::atomic::AtomicBool>,
    pub receipt_gate: Arc<tokio::sync::Semaphore>,
    pub hold_receipt: Arc<std::sync::atomic::AtomicBool>,
    cancel: CancellationToken,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}
impl Agent {
    pub async fn new() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/invoke", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let cancellations = Arc::new(AtomicUsize::new(0));
        let delay = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (hold_response, response_held) = watch::channel(false);
        let drop_receipt = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let receipt_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let hold_receipt = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel = CancellationToken::new();
        let stopped = cancel.clone();
        let (count, latency, signal) = (calls.clone(), delay.clone(), entered.clone());
        let stops = cancellations.clone();
        let drop_response = drop_receipt.clone();
        let (gate, hold) = (receipt_gate.clone(), hold_receipt.clone());
        let task = tokio::spawn(async move {
            let mut connections: JoinSet<anyhow::Result<()>> = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=stopped.cancelled()=>{connections.abort_all();while connections.join_next().await.is_some(){};return Ok(());},
                    result=connections.join_next(),if !connections.is_empty()=>{
                        if let Err(error)=result.context("connection ended")?? {
                            let disconnected=error.chain().any(|e|e.downcast_ref::<std::io::Error>().is_some_and(|e|matches!(e.kind(),std::io::ErrorKind::BrokenPipe|std::io::ErrorKind::ConnectionReset)));
                            if !disconnected {return Err(error);}
                        }
                    },
                    accepted=listener.accept(),if connections.len()<16=>{
                        let (mut socket,_)=accepted?;
                        let (count,latency,signal)=(count.clone(),latency.clone(),signal.clone());
                        let stops = stops.clone();
                        let drop_response = drop_response.clone();
                        let (gate, hold) = (gate.clone(), hold.clone());
                        let mut response_held = response_held.clone();
                        connections.spawn(async move {
                            tokio::time::timeout(Duration::from_secs(15),async move {
                                let mut raw=Vec::new();let mut buf=[0u8;2048];
                                let split = loop {
                                    let n = socket.read(&mut buf).await?;
                                    // A stopped worker can close the socket mid-request.
                                    if n == 0 { return Ok(()); }
                                    raw.extend_from_slice(&buf[..n]);
                                    anyhow::ensure!(raw.len() < 65536, "request too large");
                                    if let Some(p) = raw.windows(4).position(|x| x == b"\r\n\r\n") {
                                        break p + 4;
                                    }
                                };
                                let header=String::from_utf8(raw[..split].to_vec())?;
                                let length=header.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length:").map(str::trim).and_then(|n|n.parse::<usize>().ok())).context("body length")?;
                                anyhow::ensure!(length<32768,"body too large");
                                while raw.len() < split + length {
                                    let n = socket.read(&mut buf).await?;
                                    if n == 0 { return Ok(()); }
                                    raw.extend_from_slice(&buf[..n]);
                                }
                                let body:Value=serde_json::from_slice(&raw[split..split+length])?;
                                if header.starts_with("POST /cancel ") {
                                    anyhow::ensure!(body["invocation_id"].as_str().and_then(|s| Uuid::parse_str(s).ok()).is_some(), "cancellation identity");
                                    anyhow::ensure!(header.lines().map(str::to_ascii_lowercase).any(|line| line.strip_prefix("authorization: bearer ").and_then(|id| Uuid::parse_str(id).ok()).is_some()), "cancellation authorization");
                                    stops.fetch_add(1, Ordering::SeqCst);
                                    socket.write_all(b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await?;
                                    return anyhow::Ok(());
                                }
                                let native = body["callback"].is_object();
                                let input = if native { &body["case"]["input"] } else { &body["input"] };
                                let expected=format!("authorization: bearer {}",input["owner"].as_str().context("owner input")?);
                                let authorized=header.lines().any(|h|h.to_ascii_lowercase()==expected);
                                count.fetch_add(1,Ordering::SeqCst);signal.notify_one();
                                response_held.wait_for(|held| !held).await?;
                                tokio::time::sleep(Duration::from_millis(latency.load(Ordering::SeqCst) as u64)).await;
                                if native && authorized {
                                    if hold.load(Ordering::SeqCst) {
                                        gate.acquire().await?.forget();
                                    }
                                    if drop_response.swap(false, Ordering::SeqCst) {
                                        return anyhow::Ok(());
                                    }
                                    let receipt = json!({"schema_version":"1.0.0","invocation_id":body["invocation_id"],"status":"accepted"}).to_string();
                                    let response = format!("HTTP/1.1 202 Accepted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", receipt.len(), receipt);
                                    socket.write_all(response.as_bytes()).await?;
                                    return anyhow::Ok(());
                                }
                                let response = if authorized {
                                    let text = input["fixture_response_text"].as_str().unwrap_or("good safe");
                                    let payload = json!({"text":text}).to_string();
                                    format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", payload.len(), payload)
                                } else {
                                    "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".into()
                                };
                                socket.write_all(response.as_bytes()).await?;
                                anyhow::Ok(())
                            }).await.context("agent fixture deadline")?
                        });
                    }
                }
            }
        });
        Ok(Self {
            url,
            calls,
            cancellations,
            delay,
            entered,
            hold_response,
            drop_receipt,
            receipt_gate,
            hold_receipt,
            cancel,
            task: Some(task),
        })
    }
    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(5), task).await???;
        }
        Ok(())
    }
}
impl Drop for Agent {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
async fn agent_accepts_work_after_a_partial_request_disconnects() -> anyhow::Result<()> {
    let mut agent = Agent::new().await?;
    let address = agent
        .url
        .strip_prefix("http://")
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let owner = Uuid::nil();
    for partial in [
        "POST /invoke HTTP/1.1\r\ncontent-len",
        "POST /invoke HTTP/1.1\r\ncontent-length: 20\r\n\r\n{",
    ] {
        let mut connection = tokio::net::TcpStream::connect(address).await?;
        connection.write_all(partial.as_bytes()).await?;
        connection.shutdown().await?;
        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            connection.read_to_end(&mut response),
        )
        .await??;
        assert!(response.is_empty());

        let response = client
            .post(&agent.url)
            .bearer_auth(owner)
            .json(&json!({"input": {"owner": owner}}))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }
    assert_eq!(agent.calls.load(Ordering::SeqCst), 2);
    agent.close().await
}
