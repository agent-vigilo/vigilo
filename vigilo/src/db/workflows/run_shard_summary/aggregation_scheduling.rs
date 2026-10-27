//! Fair, bounded scheduling of independent shard aggregation pages.
//!
//! Candidate discovery provides a fixed bounded set. Each shard has at most one
//! active page; shards with more ready work rejoin after waiting first visits.
//! The scheduler stops admitting work on budget exhaustion or an operation error
//! and observes every already-started result before returning. Page transactions
//! remain owned by their futures, including when an outer database deadline drops
//! the whole pass.

use std::{
    collections::VecDeque,
    future::Future,
};

use futures_util::{
    StreamExt,
    stream::FuturesUnordered,
};

use super::aggregation::AggregationConfig;

/// Sealed and caught-up pages count as progress without another readiness probe.
#[derive(Clone, Copy, Debug)]
pub(super) enum PageOutcome {
    Advanced,
    Sealed,
    CaughtUp,
    Deferred,
}

/// Schedules first visits before revisits, bounded by page attempts and active
/// futures. The time predicate is checked before every admission; it does not
/// cancel in-flight transactions. A fatal operation error stops admission but
/// cannot discard the results of other operations that have already started.
pub(super) async fn drain<T, F, Fut, Budget>(
    candidates: Vec<T>,
    config: &AggregationConfig,
    has_time: Budget,
    operation: F,
) -> anyhow::Result<usize>
where
    T: Copy,
    F: Fn(T) -> Fut,
    Fut: Future<Output = anyhow::Result<PageOutcome>>,
    Budget: Fn() -> bool,
{
    let mut waiting = VecDeque::from(candidates);
    let mut pending = FuturesUnordered::new();
    let mut attempts = 0;
    let mut advanced = 0;
    let mut first_error = None;
    loop {
        while first_error.is_none()
            && attempts < config.max_pages
            && pending.len() < config.parallelism
            && !waiting.is_empty()
            && has_time()
        {
            let candidate = waiting.pop_front().expect("queue was checked above");
            let future = operation(candidate);
            pending.push(async move { (candidate, future.await) });
            attempts += 1;
        }
        let Some((candidate, outcome)) = pending.next().await else {
            break;
        };
        match outcome {
            Ok(PageOutcome::Advanced) => {
                advanced += 1;
                waiting.push_back(candidate);
            }
            Ok(PageOutcome::Sealed | PageOutcome::CaughtUp) => advanced += 1,
            Ok(PageOutcome::Deferred) => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(advanced),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            HashMap,
            HashSet,
        },
        sync::{
            Arc,
            Mutex,
            atomic::{
                AtomicBool,
                AtomicUsize,
                Ordering,
            },
        },
        time::Duration,
    };

    use tokio::sync::{
        Notify,
        Semaphore,
    };

    use super::*;

    fn config(max_pages: usize, parallelism: usize) -> AggregationConfig {
        AggregationConfig {
            max_pages,
            time_budget: Duration::from_secs(1),
            parallelism,
        }
    }

    #[tokio::test]
    async fn ready_shard_finishes_multiple_pages_in_one_pass() {
        let visits = AtomicUsize::new(0);
        let advanced = drain(
            vec![0],
            &config(8, 4),
            || true,
            |_| async {
                Ok(if visits.fetch_add(1, Ordering::SeqCst) == 0 {
                    PageOutcome::Advanced
                } else {
                    PageOutcome::Sealed
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(advanced, 2);
        assert_eq!(visits.load(Ordering::SeqCst), 2, "sealing ends revisits");
    }

    #[tokio::test]
    async fn first_visits_precede_revisits_and_attempt_budget_is_exact() {
        let visits = Mutex::new(Vec::new());
        let recorded = &visits;
        let advanced = drain(
            vec![0, 1, 2],
            &config(5, 2),
            || true,
            |id| async move {
                recorded.lock().unwrap().push(id);
                Ok(PageOutcome::Advanced)
            },
        )
        .await
        .unwrap();
        assert_eq!(advanced, 5);
        let visits = visits.into_inner().unwrap();
        assert_eq!(&visits[..3], &[0, 1, 2]);
        assert_eq!(visits.len(), 5);
    }

    #[tokio::test]
    async fn caught_up_shards_publish_progress_without_an_empty_followup_page() {
        let visits = AtomicUsize::new(0);
        let advanced = drain(
            vec![0],
            &config(8, 4),
            || true,
            |_| async {
                visits.fetch_add(1, Ordering::SeqCst);
                Ok(PageOutcome::CaughtUp)
            },
        )
        .await
        .unwrap();
        assert_eq!(advanced, 1);
        assert_eq!(visits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deferred_shards_do_not_consume_retry_budget() {
        let visits = Mutex::new(HashMap::<usize, usize>::new());
        let recorded = &visits;
        let advanced = drain(
            vec![0, 1, 2],
            &config(8, 2),
            || true,
            |id| async move {
                let mut visits = recorded.lock().unwrap();
                let count = visits.entry(id).or_default();
                *count += 1;
                Ok(if id == 0 {
                    PageOutcome::Deferred
                } else if *count == 1 {
                    PageOutcome::Advanced
                } else {
                    PageOutcome::Sealed
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(advanced, 4);
        assert_eq!(
            visits.into_inner().unwrap(),
            HashMap::from([(0, 1), (1, 2), (2, 2)])
        );
    }

    #[tokio::test]
    async fn empty_or_expired_pass_starts_no_pages() {
        for (candidates, available) in [(Vec::new(), true), (vec![0, 1], false)] {
            let advanced = drain(
                candidates,
                &config(8, 2),
                || available,
                |_| async { anyhow::bail!("empty or expired pass must not start a page") },
            )
            .await
            .unwrap();
            assert_eq!(advanced, 0);
        }
    }

    #[tokio::test]
    async fn concurrency_is_bounded_without_waiting_for_a_slow_shard() {
        let entered = Arc::new(Notify::new());
        let release_slow = Arc::new(Semaphore::new(0));
        let active = Arc::new(Mutex::new(HashSet::new()));
        let peak = Arc::new(AtomicUsize::new(0));
        let later_finished = Arc::new(AtomicBool::new(false));
        let pass = {
            let entered = entered.clone();
            let release_slow = release_slow.clone();
            let active = active.clone();
            let peak = peak.clone();
            let later_finished = later_finished.clone();
            async move {
                drain(
                    vec![0, 1, 2, 3],
                    &config(4, 2),
                    || true,
                    |id| {
                        let entered = entered.clone();
                        let release_slow = release_slow.clone();
                        let active = active.clone();
                        let peak = peak.clone();
                        let later_finished = later_finished.clone();
                        async move {
                            {
                                let mut active = active.lock().unwrap();
                                assert!(active.insert(id), "a shard must never own two pages");
                                peak.fetch_max(active.len(), Ordering::SeqCst);
                            }
                            if id == 0 {
                                release_slow.acquire().await.unwrap().forget();
                            } else if id == 3 {
                                later_finished.store(true, Ordering::SeqCst);
                                entered.notify_one();
                            }
                            active.lock().unwrap().remove(&id);
                            Ok(PageOutcome::Sealed)
                        }
                    },
                )
                .await
            }
        };
        let release = async {
            entered.notified().await;
            assert!(later_finished.load(Ordering::SeqCst));
            assert_eq!(active.lock().unwrap().len(), 1);
            release_slow.add_permits(1);
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(pass, release)
        })
        .await
        .expect("independent shards must finish before the slow shard is released");
        assert_eq!(result.unwrap(), 4);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert!(active.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_the_pass_drops_every_active_page_future() {
        struct ActivePage<'a>(&'a AtomicUsize);
        impl Drop for ActivePage<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let active = AtomicUsize::new(0);
        let started = Notify::new();
        {
            let config = config(8, 2);
            let pass = drain(
                vec![0, 1, 2],
                &config,
                || true,
                |_| async {
                    let _page = ActivePage(&active);
                    if active.fetch_add(1, Ordering::SeqCst) == 1 {
                        started.notify_one();
                    }
                    std::future::pending::<anyhow::Result<PageOutcome>>().await
                },
            );
            tokio::pin!(pass);
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    result = &mut pass => panic!("blocked pages unexpectedly finished: {result:?}"),
                    () = started.notified() => assert_eq!(active.load(Ordering::SeqCst), 2),
                }
            })
            .await
            .expect("both independent pages must start");
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn exhausted_time_budget_drains_started_pages_without_revisits() {
        let has_time = AtomicBool::new(true);
        let finished = AtomicUsize::new(0);
        let advanced = drain(
            vec![0, 1, 2],
            &config(8, 2),
            || has_time.load(Ordering::SeqCst),
            |_| async {
                has_time.store(false, Ordering::SeqCst);
                finished.fetch_add(1, Ordering::SeqCst);
                Ok(PageOutcome::Advanced)
            },
        )
        .await
        .unwrap();
        assert_eq!(advanced, 2);
        assert_eq!(finished.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn operation_error_drains_started_pages_and_stops_admission() {
        let finished = AtomicUsize::new(0);
        let observed = &finished;
        let result = drain(
            vec![0, 1, 2],
            &config(8, 2),
            || true,
            |id| async move {
                observed.fetch_add(1, Ordering::SeqCst);
                if id == 0 {
                    anyhow::bail!("rotation failed");
                }
                Ok(PageOutcome::Advanced)
            },
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "rotation failed");
        assert_eq!(finished.load(Ordering::SeqCst), 2);
    }
}
