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
    pub delay: Arc<AtomicUsize>,
    pub entered: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}
impl Agent {
    pub async fn new() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/invoke", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let delay = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let cancel = CancellationToken::new();
        let stopped = cancel.clone();
        let (count, latency, signal) = (calls.clone(), delay.clone(), entered.clone());
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
                        connections.spawn(async move {
                            tokio::time::timeout(Duration::from_secs(15),async move {
                                let mut raw=Vec::new();let mut buf=[0u8;2048];
                                let split=loop {let n=socket.read(&mut buf).await?;anyhow::ensure!(n>0,"truncated request");raw.extend_from_slice(&buf[..n]);anyhow::ensure!(raw.len()<65536,"request too large");if let Some(p)=raw.windows(4).position(|x|x==b"\r\n\r\n"){break p+4;}};
                                let header=String::from_utf8(raw[..split].to_vec())?;
                                let length=header.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length:").map(str::trim).and_then(|n|n.parse::<usize>().ok())).context("body length")?;
                                anyhow::ensure!(length<32768,"body too large");
                                while raw.len()<split+length {let n=socket.read(&mut buf).await?;anyhow::ensure!(n>0,"truncated body");raw.extend_from_slice(&buf[..n]);}
                                let body:Value=serde_json::from_slice(&raw[split..split+length])?;
                                let expected=format!("authorization: bearer {}",body["input"]["owner"].as_str().context("owner input")?);
                                let authorized=header.lines().any(|h|h.to_ascii_lowercase()==expected);
                                count.fetch_add(1,Ordering::SeqCst);signal.notify_one();
                                tokio::time::sleep(Duration::from_millis(latency.load(Ordering::SeqCst) as u64)).await;
                                let response=if authorized {"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 20\r\nconnection: close\r\n\r\n{\"text\":\"good safe\"}"} else {"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"};
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
            delay,
            entered,
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
