//! Test-only PostgreSQL protocol observation. A single bounded plaintext loopback
//! connection records client exchanges independently of server statement counters.
//! Snapshot setup, fixture SQL, and oracle reads happen outside the enabled window.
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        AtomicU64,
        Ordering,
    },
};

use sqlx::{
    PgPool,
    postgres::{
        PgPoolOptions,
        PgSslMode,
    },
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
    task::JoinHandle,
};

pub(super) struct Probe {
    pub pool: PgPool,
    enabled: Arc<AtomicBool>,
    rounds: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    task: JoinHandle<()>,
}
impl Probe {
    pub async fn new(original: &PgPool) -> Self {
        let options = original.connect_options();
        let target = format!("{}:{}", options.get_host(), options.get_port());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let enabled = Arc::new(AtomicBool::new(false));
        let rounds = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let e = enabled.clone();
        let r = rounds.clone();
        let b = bytes.clone();
        let task = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            let mut server = TcpStream::connect(target).await.unwrap();
            let size = client.read_u32().await.unwrap();
            assert!((8..=65536).contains(&size));
            let mut startup = vec![0; size as usize - 4];
            client.read_exact(&mut startup).await.unwrap();
            server.write_u32(size).await.unwrap();
            server.write_all(&startup).await.unwrap();
            let (mut cr, mut cw) = client.into_split();
            let (mut sr, mut sw) = server.into_split();
            let frontend = async {
                while let Ok(tag) = cr.read_u8().await {
                    let size = cr.read_u32().await.unwrap();
                    assert!((4..=64 * 1024 * 1024).contains(&size));
                    if e.load(Ordering::SeqCst) {
                        b.fetch_add(u64::from(size) + 1, Ordering::SeqCst);
                        if tag == b'Q' || tag == b'S' {
                            r.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    sw.write_u8(tag).await.unwrap();
                    sw.write_u32(size).await.unwrap();
                    // Stream payloads in bounded blocks, including large bytea parameters.
                    let mut remaining = size as usize - 4;
                    let mut buffer = [0u8; 8192];
                    while remaining > 0 {
                        let len = remaining.min(buffer.len());
                        cr.read_exact(&mut buffer[..len]).await.unwrap();
                        sw.write_all(&buffer[..len]).await.unwrap();
                        remaining -= len;
                    }
                }
            };
            let backend = async {
                let _ = tokio::io::copy(&mut sr, &mut cw).await;
            };
            tokio::select! {_=frontend=>{},_=backend=>{}}
        });
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .test_before_acquire(false)
            .connect_with(
                (*options)
                    .clone()
                    .host("127.0.0.1")
                    .port(port)
                    .ssl_mode(PgSslMode::Disable),
            )
            .await
            .unwrap();
        Self {
            pool,
            enabled,
            rounds,
            bytes,
            task,
        }
    }
    pub fn start(&self) {
        self.rounds.store(0, Ordering::SeqCst);
        self.bytes.store(0, Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
    }
    pub fn stop(&self) -> (u64, u64) {
        self.enabled.store(false, Ordering::SeqCst);
        (
            self.rounds.load(Ordering::SeqCst),
            self.bytes.load(Ordering::SeqCst),
        )
    }
    pub async fn close(mut self) {
        self.pool.close().await;
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.task.abort();
    }
}
