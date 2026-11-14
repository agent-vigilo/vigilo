//! Bounded loopback PostgreSQL proxy for whole-worker protocol observations.
//! Counts frontend Query/Sync messages, not TCP packets or server statements.
//! Only the measured child uses this endpoint; setup and oracle connections bypass
//! it. No SQL or credentials are retained. Shutdown closes and joins every socket.
use std::{
    io::{
        self,
        Read,
        Write,
    },
    net::{
        Shutdown,
        TcpListener,
        TcpStream,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    thread::{
        self,
        JoinHandle,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
    ensure,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Complete counters from a successfully closed measured worker connection set.
pub struct Observation {
    /// Accepted database connections; absent in older observations.
    #[serde(default)]
    pub connections: Option<u64>,
    pub query_sync_exchanges: u64,
    pub frontend_bytes: u64,
}

/// Owns the forwarding threads and their sockets until measurement finishes.
pub struct Probe {
    pub url: String,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    task: Option<JoinHandle<Result<Observation>>>,
    rounds: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}
impl Probe {
    /// Only accepts loopback plaintext endpoints owned by the service harness.
    pub fn start(target: &str) -> Result<Self> {
        let mut url = url::Url::parse(target)?;
        ensure!(
            url.host_str() == Some("127.0.0.1"),
            "protocol probe requires loopback"
        );
        let port = url.port().context("database port missing")?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        url.set_port(Some(listener.local_addr()?.port()))
            .map_err(|_| anyhow::anyhow!("invalid database URL"))?;
        let query: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "sslmode")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(query)
            .append_pair("sslmode", "disable");
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let stopped = stop.clone();
        let owned = sockets.clone();
        let rounds = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let (measured_rounds, measured_bytes) = (rounds.clone(), bytes.clone());
        let task = thread::spawn(move || {
            let (rounds, bytes) = (measured_rounds, measured_bytes);
            let mut workers = Vec::new();
            let mut failure = None;
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        if workers.len() >= 128 {
                            failure = Some(anyhow::anyhow!("protocol connection limit exceeded"));
                            break;
                        }
                        let server = match TcpStream::connect_timeout(
                            &([127, 0, 0, 1], port).into(),
                            Duration::from_secs(5),
                        ) {
                            Ok(s) => s,
                            Err(e) => {
                                failure = Some(e.into());
                                break;
                            }
                        };
                        let prepared = (|| -> Result<_> {
                            client.set_nodelay(true)?;
                            server.set_nodelay(true)?;
                            Ok((
                                client.try_clone()?,
                                server.try_clone()?,
                                [client.try_clone()?, server.try_clone()?],
                            ))
                        })();
                        let (mut cr, mut sw, shutdown_sockets) = match prepared {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                        owned.lock().unwrap().extend(shutdown_sockets);
                        let r = rounds.clone();
                        let b = bytes.clone();
                        workers.push(thread::spawn(move || -> Result<()> {
                            let mut sr = server;
                            let mut cw = client;
                            let backend = thread::spawn(move || {
                                let result = io::copy(&mut sr, &mut cw);
                                let _ = cw.shutdown(Shutdown::Both);
                                result
                            });
                            let result = forward(&mut cr, &mut sw, &r, &b);
                            let _ = sw.shutdown(Shutdown::Both);
                            let back = backend
                                .join()
                                .map_err(|_| anyhow::anyhow!("backend probe panicked"))?;
                            result.context("frontend protocol framing failed")?;
                            match back {
                                Ok(_) => Ok(()),
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                                    ) =>
                                {
                                    Ok(())
                                }
                                Err(error) => {
                                    Err(error).context("backend protocol forwarding failed")
                                }
                            }
                        }));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => {
                        failure = Some(e.into());
                        break;
                    }
                }
            }
            // The child has exited, but its final frames may still be buffered.
            // Drain them before shutdown; abandonment remains bounded by Drop.
            let deadline = Instant::now() + Duration::from_secs(2);
            while failure.is_none()
                && workers.iter().any(|worker| !worker.is_finished())
                && Instant::now() < deadline
            {
                thread::sleep(Duration::from_millis(2));
            }
            if workers.iter().any(|worker| !worker.is_finished()) && failure.is_none() {
                failure = Some(anyhow::anyhow!(
                    "protocol connections did not close after the measured child exited"
                ));
            }
            for socket in owned.lock().unwrap().iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
            let connections = workers.len() as u64;
            for worker in workers {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        failure.get_or_insert(e);
                    }
                    Err(_) => {
                        failure.get_or_insert(anyhow::anyhow!("protocol probe panicked"));
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            Ok(Observation {
                connections: Some(connections),
                query_sync_exchanges: rounds.load(Ordering::SeqCst),
                frontend_bytes: bytes.load(Ordering::SeqCst),
            })
        });
        Ok(Self {
            url: url.to_string(),
            stop,
            sockets,
            task: Some(task),
            rounds,
            bytes,
        })
    }
    /// Read monotonic counters at a quiescent workload boundary. Final connection
    /// counts and forwarding validity are available only after `finish()` joins.
    pub fn snapshot(&self) -> Observation {
        Observation {
            connections: None,
            query_sync_exchanges: self.rounds.load(Ordering::SeqCst),
            frontend_bytes: self.bytes.load(Ordering::SeqCst),
        }
    }
    /// Stops acceptance and joins all forwarding work, rejecting observation errors.
    pub fn finish(mut self) -> Result<Observation> {
        self.stop.store(true, Ordering::SeqCst);
        let joined = self
            .task
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow::anyhow!("protocol probe panicked"))??;
        let snapshot = self.snapshot();
        ensure!(
            snapshot.query_sync_exchanges == joined.query_sync_exchanges
                && snapshot.frontend_bytes == joined.frontend_bytes,
            "protocol counters changed after forwarding joined"
        );
        Ok(joined)
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for socket in self.sockets.lock().unwrap().iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

fn forward(
    reader: &mut impl Read,
    writer: &mut impl Write,
    rounds: &AtomicU64,
    bytes: &AtomicU64,
) -> Result<()> {
    let mut length = [0; 4];
    reader.read_exact(&mut length)?;
    let size = u32::from_be_bytes(length);
    ensure!(
        (8..=65536).contains(&size),
        "invalid PostgreSQL startup size"
    );
    writer.write_all(&length)?;
    transfer(reader, writer, size - 4)?;
    loop {
        let mut tag = [0; 1];
        match reader.read(&mut tag) {
            Ok(0) => break,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => break,
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        reader.read_exact(&mut length)?;
        let size = u32::from_be_bytes(length);
        ensure!(
            (4..=64 * 1024 * 1024).contains(&size),
            "invalid PostgreSQL frame size"
        );
        bytes.fetch_add(u64::from(size) + 1, Ordering::SeqCst);
        if matches!(tag[0], b'Q' | b'S') {
            rounds.fetch_add(1, Ordering::SeqCst);
        }
        writer.write_all(&tag)?;
        writer.write_all(&length)?;
        transfer(reader, writer, size - 4)?;
        if tag[0] == b'X' {
            break;
        }
    }
    Ok(())
}
fn transfer(reader: &mut impl Read, writer: &mut impl Write, mut remaining: u32) -> io::Result<()> {
    let mut buffer = [0; 8192];
    while remaining > 0 {
        let count = (remaining as usize).min(buffer.len());
        reader.read_exact(&mut buffer[..count])?;
        writer.write_all(&buffer[..count])?;
        remaining -= count as u32;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_exchanges_without_retaining_payloads_and_rejects_truncation() {
        let mut wire = vec![];
        wire.extend_from_slice(&8u32.to_be_bytes());
        wire.extend_from_slice(&196608u32.to_be_bytes());
        for tag in *b"PBESQX" {
            wire.push(tag);
            wire.extend_from_slice(&4u32.to_be_bytes());
        }
        let rounds = AtomicU64::new(0);
        let bytes = AtomicU64::new(0);
        let mut out = vec![];
        forward(&mut wire.as_slice(), &mut out, &rounds, &bytes).unwrap();
        assert_eq!(out, wire);
        assert_eq!(rounds.load(Ordering::SeqCst), 2);
        assert_eq!(bytes.load(Ordering::SeqCst), 30);
        for cut in [3, 7, 10, wire.len() - 1] {
            assert!(
                forward(
                    &mut &wire[..cut],
                    &mut vec![],
                    &AtomicU64::new(0),
                    &AtomicU64::new(0)
                )
                .is_err()
            );
        }
    }
    #[test]
    fn connection_reset_at_frame_boundary_preserves_complete_observations() {
        struct Reset;
        impl Read for Reset {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::ConnectionReset))
            }
        }
        let mut wire = vec![];
        wire.extend_from_slice(&8u32.to_be_bytes());
        wire.extend_from_slice(&196608u32.to_be_bytes());
        wire.push(b'Q');
        wire.extend_from_slice(&4u32.to_be_bytes());
        let rounds = AtomicU64::new(0);
        let mut output = vec![];
        forward(
            &mut wire.as_slice().chain(Reset),
            &mut output,
            &rounds,
            &AtomicU64::new(0),
        )
        .unwrap();
        assert_eq!(output, wire);
        assert_eq!(rounds.load(Ordering::SeqCst), 1);
        wire.push(b'Q');
        assert!(
            forward(
                &mut wire.as_slice().chain(Reset),
                &mut vec![],
                &AtomicU64::new(0),
                &AtomicU64::new(0)
            )
            .is_err()
        );
    }
}
