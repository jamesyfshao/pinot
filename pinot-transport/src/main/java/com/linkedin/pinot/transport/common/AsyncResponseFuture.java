/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AsyncResponseFuture<K, T> implements Callback<T>, KeyedFuture<K, T> {
  protected static Logger LOG = LoggerFactory.getLogger(AsyncResponseFuture.class);

  private Cancellable _cancellable;

  // Id for this future
  private final K _key;

  // Lock for mutex
  private final Lock _futureLock = new ReentrantLock();
  // Condition variable to wait for the response
  private final Condition _finished = _futureLock.newCondition();

  /**
   *  Delayed response.
   *  If the future is cancelled or in case of error, this will be null.
   *  In that case, clients will need to use specific APIs to distinguish
   *  between cancel and errors.
   */
  private volatile T _delayedResponse;
  // Exception in case of error
  private volatile Throwable _error;
  // List of runnables that needs to be executed on completion
  private final List<Runnable> _pendingRunnable = new ArrayList<Runnable>();
  //List of executors that needs to run the runnables.
  private final List<Executor> _pendingRunnableExecutors = new ArrayList<Executor>();

  // Cached response/errors
  private volatile Map<K, T> _responseMap;
  private volatile Map<K, Throwable> _errorMap;

  // For  debug
  private final String _ctxt;

  /**
   * Response Future State
   */
  public enum State {
    PENDING,
    CANCELLED,
    DONE;

    public boolean isCompleted() {
      return this != PENDING;
    }
  }

  // State of the future
  private State _state;

  public AsyncResponseFuture(K key, String ctxt) {
    _key = key;
    _state = State.PENDING;
    _cancellable = new NoopCancellable();
    _ctxt = ctxt;
  }

  public AsyncResponseFuture(K key, Throwable t, String ctxt) {
    _key = key;
    _state = State.DONE;
    _error = t;
    _ctxt = ctxt;
  }

  public void setCancellable(Cancellable cancellable) {
    _cancellable = cancellable;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean isCancelled = false;
    try {
      _futureLock.lock();
      if (_state.isCompleted()) {
        LOG.info("{} Request is no longer pending. Cannot cancel !!", _ctxt);
        return false;
      }
      isCancelled = _cancellable.cancel();
      if (isCancelled) {
        setDone(State.CANCELLED);
      }
    } finally {
      _futureLock.unlock();
    }
    return isCancelled;
  }

  @Override
  public void onSuccess(T result) {
    try {
      _futureLock.lock();
      if (_state.isCompleted()) {
        LOG.debug("{} Request has already been completed. Discarding this response !!", _ctxt, result);
        return;
      }
      _delayedResponse = result;
      setDone(State.DONE);
    } finally {
      _futureLock.unlock();
    }
  }

  /**
   * Set Exception and let the future listener get notified.
   * @param t throwable
   */
  @Override
  public void onError(Throwable t) {
    try {
      _futureLock.lock();
      if (_state.isCompleted()) {
        LOG.debug("{} Request has already been completed. Discarding error message !!", _ctxt, t);
        return;
      }
      _error = t;
      setDone(State.DONE);
    } finally {
      _futureLock.unlock();
    }
  }

  @Override
  public boolean isCancelled() {
    return _state == State.CANCELLED;
  }

  @Override
  public boolean isDone() {
    return _state.isCompleted();
  }

  @Override
  public Map<K, T> get() throws InterruptedException, ExecutionException {
    try {
      _futureLock.lock();
      while (!_state.isCompleted()) {
        _finished.await();
      }
      if (null == _responseMap) {
        setResponseMap();
      }
    } finally {
      _futureLock.unlock();
    }
    return _responseMap;
  }

  @Override
  public T getOne() throws InterruptedException, ExecutionException {
    try {
      _futureLock.lock();
      while (!_state.isCompleted()) {
        _finished.await();
      }
    } finally {
      _futureLock.unlock();
    }
    return _delayedResponse;
  }

  @Override
  public T getOne(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    try {
      _futureLock.lock();
      while (!_state.isCompleted()) {
        boolean notElapsed = _finished.await(timeout,unit);
        if (!notElapsed)
          throw new TimeoutException("Timedout waiting for async result for key " + _key);
      }
    } finally {
      _futureLock.unlock();
    }
    return _delayedResponse;
  }

  @Override
  public Map<K, Throwable> getError() {
    if ((null == _errorMap) && (null != _error)) {
      try {
        _futureLock.lock();
        if ((null == _errorMap) && (null != _error)) {
          _errorMap = new HashMap<K, Throwable>();
          _errorMap.put(_key, _error);
        }
      } finally {
        _futureLock.unlock();
      }
    }
    return _errorMap;
  }

  private void setResponseMap() {
    if (null != _delayedResponse) {
      _responseMap = new HashMap<K, T>();
      _responseMap.put(_key, _delayedResponse);
    }
  }

  @Override
  public Map<K, T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    try {
      _futureLock.lock();
      while (!_state.isCompleted()) {
        boolean notElapsed = _finished.await(timeout, unit);
        if (!notElapsed) {
          throw new TimeoutException("Timeout awaiting response !!");
        }
        if (null == _responseMap) {
          setResponseMap();
        }
      }
    } finally {
      _futureLock.unlock();
    }
    return _responseMap;
  }

  /**
   * Mark complete and notify threads waiting for this condition
   */
  private void setDone(State state) {
    LOG.debug("{} Setting state to : {}, Current State : {}", _ctxt, state, _state);
    try {
      _futureLock.lock();
      _state = state;
      _finished.signalAll();
    } finally {
      _futureLock.unlock();
    }
    for (int i = 0; i < _pendingRunnable.size(); i++) {
      LOG.debug("{} Running pending runnable :" + i, _ctxt);
      Executor e = _pendingRunnableExecutors.get(i);
      if (null != e) {
        e.execute(_pendingRunnable.get(i));
      } else {
        _pendingRunnable.get(i).run(); // run in the current thread.
      }
    }
    _pendingRunnable.clear();
    _pendingRunnableExecutors.clear();
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    boolean processed = false;
    try {
      _futureLock.lock();
      if (!_state.isCompleted()) {
        _pendingRunnable.add(listener);
        _pendingRunnableExecutors.add(executor);
        processed = true;
      }
    } finally {
      _futureLock.unlock();
    }

    if (!processed) {
      LOG.debug("{} Executing the listener as the future event is already done !!", _ctxt);
      if (null != executor) {
        executor.execute(listener);
      } else {
        listener.run(); // run in the same thread
      }
    }
  }

  public static class NoopCancellable implements Cancellable {
    @Override
    public boolean cancel() {
      return true;
    }
  }

  @Override
  public String getName() {
    return "Future for (" + _key + ")";
  }

  public State getState() {
    return _state;
  }
}
