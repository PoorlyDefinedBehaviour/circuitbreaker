use std::{
  fmt::Debug,
  future::Future,
  sync::atomic::{AtomicI8, Ordering},
};
use thiserror::Error;
use tracing::{error, instrument};

/// The circuit breaker states.
///
/// The request from the application is route to the to the operation
const CLOSED: i8 = 0;
/// The request fails immediately
const OPEN: i8 = 1;
/// A limit number of requests are allowed to pass through. If the requests
/// are successful, it's assumed that the fault that waqs causing the failure has
/// been fixed and the circuit breaker switches to the `Open` state
const HALFOPEN: i8 = 2;

pub struct CircuitBreaker {
  /// The current circuit breaker state, it may be CLOSED, OPEN or HALFOPEN.
  state: AtomicI8,
}

#[derive(Debug, Error, PartialEq)]
pub enum CircuitBreakerError<E> {
  #[error("the circuit is open")]
  CircuitOpen,
  #[error("operation failed: {0}")]
  OperationError(E),
}

impl CircuitBreaker {
  /// Creates a circuit breaker in the closed state.
  #[instrument(skip_all)]
  pub fn new() -> Self {
    Self {
      state: AtomicI8::new(0),
    }
  }

  /// Returns `true` when the circuit is closed.
  #[instrument(skip_all)]
  fn is_closed(&self) -> bool {
    self.state.load(Ordering::Acquire) == 0
  }

  #[instrument(skip_all)]
  pub async fn call<F, T, E, Fut>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Debug,
  {
    match self.state.load(Ordering::Acquire) {
      CLOSED => match f().await {
        Err(err) => {
          error!(?err);
          self.state.store(OPEN, Ordering::Release);
          Err(CircuitBreakerError::OperationError(err))
        }
        Ok(value) => Ok(value),
      },
      HALFOPEN => match f().await {
        Err(err) => {
          error!(?err);
          self.state.store(OPEN, Ordering::Relaxed);
          Err(CircuitBreakerError::OperationError(err))
        }
        Ok(value) => {
          self.state.store(CLOSED, Ordering::Release);

          Ok(value)
        }
      },
      OPEN => {
        self.state.store(HALFOPEN, Ordering::Relaxed);
        Err(CircuitBreakerError::CircuitOpen)
      }
      _ => unreachable!(),
    }
  }
}

#[cfg(test)]
mod tests {

  use super::*;

  #[tokio::test]
  async fn simple_closed_circuit() {
    // Given
    let cb = CircuitBreaker::new();

    // When
    let x = cb.call(|| async { Result::<i32, i32>::Ok(1) }).await;

    // Then
    assert_eq!(Result::<i32, CircuitBreakerError<_>>::Ok(1), x);
  }

  #[tokio::test]
  async fn simple_open_circuit() {
    // Given
    let cb = CircuitBreaker::new();
    let _ = cb.call(|| async { Result::<i32, i32>::Err(1) }).await;

    // When
    assert!(!cb.is_closed());

    // Then
    assert_eq!(
      Err(CircuitBreakerError::CircuitOpen),
      cb.call(|| async { Result::<i32, i32>::Ok(1) }).await
    );
  }

  #[tokio::test]
  async fn smoke() {
    let cb = CircuitBreaker::new();

    // Circuit is open.
    assert_eq!(Ok(()), cb.call(|| async { Result::<(), ()>::Ok(()) }).await);

    // Circuit opens.
    assert_eq!(
      Err(CircuitBreakerError::OperationError(())),
      cb.call(|| async { Result::<(), ()>::Err(()) }).await
    );

    // Circuit is open and transitions to halfopen.
    assert_eq!(
      Err(CircuitBreakerError::CircuitOpen),
      cb.call(|| async { Result::<(), ()>::Ok(()) }).await
    );

    // Circuit is halfopen and transitions to open since operation fails.
    assert_eq!(
      Err(CircuitBreakerError::OperationError(())),
      cb.call(|| async { Result::<(), ()>::Err(()) }).await
    );

    // Circuit is open and transitions to halfopen.
    assert_eq!(
      Err(CircuitBreakerError::CircuitOpen),
      cb.call(|| async { Result::<(), ()>::Err(()) }).await
    );

    // Circuit is open and transitions to closed since operation succeeds.
    assert_eq!(Ok(()), cb.call(|| async { Result::<(), ()>::Ok(()) }).await);

    // Circuit is closed and stays closed since operation succeeds.
    assert_eq!(Ok(()), cb.call(|| async { Result::<(), ()>::Ok(()) }).await);

    // Circuit is closed and transitions to open since operation fails.
    assert_eq!(
      Err(CircuitBreakerError::OperationError(())),
      cb.call(|| async { Result::<(), ()>::Err(()) }).await
    );

    // Circuit is open.
    assert_eq!(
      Err(CircuitBreakerError::CircuitOpen),
      cb.call(|| async { Result::<(), ()>::Ok(()) }).await
    );
  }
}
