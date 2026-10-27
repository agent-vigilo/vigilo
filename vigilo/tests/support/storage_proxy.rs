//! Bounded local fault proxy for signed Ceph acceptance. All forwarding preserves
//! the signed Host header. Each fixture body is capped at 2 MiB and each handler
//! at ten seconds; dropping the proxy aborts every owned socket task.
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicU8,
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    sync::Notify,
    task::JoinSet,
};

/// Complete GET attempts and delivered body bytes, grouped by exact object path.
/// The fixture allows at most 1,024 distinct paths; it never retains bodies.
#[derive(Default, Clone, serde::Serialize)]
pub struct Traffic {
    pub gets: usize,
    pub bytes: usize,
    pub objects: BTreeMap<String, (usize, usize)>,
}

pub struct Proxy {
    pub endpoint: String,
    pub mode: Arc<AtomicU8>,
    pub requests: Arc<AtomicUsize>,
    pub entered: Arc<Notify>,
    pub traffic: Arc<Mutex<Traffic>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Proxy {
    pub async fn start(upstream: String) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let mode = Arc::new(AtomicU8::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let traffic = Arc::new(Mutex::new(Traffic::default()));
        let (m, r, n, t) = (
            mode.clone(),
            requests.clone(),
            entered.clone(),
            traffic.clone(),
        );
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    connection=listener.accept()=>{let Ok((socket,_))=connection else{break;};let (m,r,n,u,t)=(m.clone(),r.clone(),n.clone(),upstream.clone(),t.clone());tasks.spawn(async move {let _=tokio::time::timeout(Duration::from_secs(10),forward(socket,u,m,r,n,t)).await;});},
                    _=tasks.join_next(),if !tasks.is_empty()=>{},
                }
            }
        });
        Ok(Self {
            endpoint,
            mode,
            requests,
            entered,
            traffic,
            task,
        })
    }
    pub fn set(&self, mode: u8) {
        *self.traffic.lock().unwrap() = Traffic::default();
        self.mode.store(mode, Ordering::SeqCst);
        self.requests.store(0, Ordering::SeqCst);
    }
    pub fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}
async fn forward(
    mut socket: TcpStream,
    upstream: String,
    mode: Arc<AtomicU8>,
    requests: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    traffic: Arc<Mutex<Traffic>>,
) -> anyhow::Result<()> {
    let mut received = Vec::new();
    let mut buffer = [0_u8; 1024];
    let boundary = loop {
        let n = socket.read(&mut buffer).await?;
        anyhow::ensure!(n > 0, "closed request");
        received.extend_from_slice(&buffer[..n]);
        anyhow::ensure!(received.len() <= 64 * 1024, "request headers too large");
        if let Some(index) = received.windows(4).position(|s| s == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header = std::str::from_utf8(&received[..boundary])?;
    let mut lines = header.split("\r\n");
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().to_owned();
    let path = first.next().unwrap().to_owned();
    let mut headers = reqwest::header::HeaderMap::new();
    for line in lines.filter(|s| !s.is_empty()) {
        let (k, v) = line
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid header"))?;
        headers.insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
            v.trim().parse()?,
        );
    }
    let length = headers
        .get("content-length")
        .map(|n| n.to_str()?.parse::<usize>().map_err(anyhow::Error::from))
        .transpose()?
        .unwrap_or(0);
    anyhow::ensure!(length <= 2 * 1024 * 1024, "fixture body too large");
    if method == "GET" {
        let mut observed = traffic.lock().unwrap();
        anyhow::ensure!(
            observed.objects.contains_key(&path) || observed.objects.len() < 1024,
            "too many fixture objects"
        );
        observed.gets += 1;
        observed.objects.entry(path.clone()).or_default().0 += 1;
    }
    let request_index = requests.fetch_add(1, Ordering::SeqCst);
    let selected = mode.load(Ordering::SeqCst);
    // Delay one terminal read failure to measure serial recovery head-of-line
    // blocking; subsequent requests use the real store unchanged.
    if selected == 5 && request_index == 0 && method == "GET" {
        entered.notify_one();
        tokio::time::sleep(Duration::from_secs(2)).await;
        socket
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }
    if selected == 4 && method == "PUT" {
        let url = reqwest::Url::parse(&upstream)?;
        let mut remote = TcpStream::connect((
            url.host_str().unwrap(),
            url.port_or_known_default().unwrap(),
        ))
        .await?;
        remote.write_all(&received[..boundary]).await?;
        remote.write_all(b"x").await?;
        entered.notify_one();
        tokio::time::sleep(Duration::from_millis(200)).await;
        return Ok(()); // incomplete request closes both sockets
    }
    let mut body = received[boundary..].to_vec();
    anyhow::ensure!(body.len() <= length, "unexpected request body");
    body.resize(length, 0);
    socket
        .read_exact(&mut body[received.len() - boundary..])
        .await?;
    if selected == 1 {
        socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
        return Ok(());
    }
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .request(method.parse()?, format!("{upstream}{path}"))
        .headers(headers)
        .body(body)
        .send()
        .await?;
    let status = response.status().as_u16();
    let bytes = response.bytes().await?;
    anyhow::ensure!(bytes.len() <= 2 * 1024 * 1024, "fixture response too large");
    if selected == 2 && method == "PUT" {
        mode.store(0, Ordering::SeqCst);
        return Ok(());
    } // commit succeeded, response lost
    let head = format!(
        "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        bytes.len()
    );
    socket.write_all(head.as_bytes()).await?;
    if selected == 3 && method == "GET" {
        entered.notify_one();
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }
    socket.write_all(&bytes).await?;
    if method == "GET" {
        let mut observed = traffic.lock().unwrap();
        observed.bytes += bytes.len();
        observed.objects.get_mut(&path).unwrap().1 += bytes.len();
    }
    Ok(())
}
