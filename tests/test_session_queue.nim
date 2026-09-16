# SPDX-License-Identifier: AGPL-3.0-only
import std/[unittest, sequtils]
include ../src/auth

let
  tweets = ApiReq(cookie: ApiUrl(endpoint: "tweets"), oauth: ApiUrl(endpoint: "oauth-tweets"))
  users = ApiReq(cookie: ApiUrl(endpoint: "users"), oauth: ApiUrl(endpoint: "oauth-users"))

proc newSession(): Session =
  Session(kind: SessionKind.cookie, authToken: "test", ct0: "test")

suite "session request queue":
  setup:
    sessionPool = @[newSession()]
    sessionWaiters.setLen(0)
    setMaxConcurrentReqs(2)
    setSessionQueueLimits(100, 1000)

  test "the configured concurrency limit is exact":
    let first = waitFor getSession(tweets)
    let second = waitFor getSession(tweets)
    let third = getSession(tweets)
    check first.pending == 2
    check not third.finished
    release(first)
    let acquired = waitFor third
    check acquired == second
    check acquired.pending == 2
    release(second)
    release(acquired)
    check acquired.pending == 0

  test "fifteen simultaneous requests drain in order within the limit":
    var order: seq[int]
    var peak = 0
    proc fetch(i: int) {.async.} =
      let session = await getSession(tweets)
      try:
        order.add(i)
        peak = max(peak, session.pending)
        await sleepAsync(2)
      finally:
        release(session)
    var requests: seq[Future[void]]
    for i in 0 ..< 15: requests.add(fetch(i))
    waitFor all(requests)
    check peak == 2
    check order == toSeq(0 ..< 15)
    check sessionWaiters.len == 0
    check sessionPool[0].pending == 0

  test "every session is considered even when another is busy or limited":
    for i in 0 ..< 30:
      let busy = newSession()
      busy.pending = 2
      let limited = newSession()
      limited.setRateLimit(tweets, 0, epochTime().int + 60, 100)
      let ready = newSession()
      sessionPool = @[busy, limited, ready]
      let acquired = waitFor getSession(tweets)
      check acquired == ready
      release(acquired)

  test "a full queue rejects excess work without losing queued requests":
    setMaxConcurrentReqs(1)
    setSessionQueueLimits(1, 1000)
    let active = waitFor getSession(tweets)
    let queued = getSession(tweets)
    expect SessionBusyError:
      discard waitFor getSession(tweets)
    check sessionWaiters.len == 1
    release(active)
    release(waitFor queued)
    check sessionPool[0].pending == 0

  test "waiting can be disabled by either queue setting":
    setMaxConcurrentReqs(1)
    let active = waitFor getSession(tweets)
    for limits in [(0, 1000), (100, 0), (-1, -1)]:
      setSessionQueueLimits(limits[0], limits[1])
      expect SessionBusyError:
        discard waitFor getSession(tweets)
      check sessionWaiters.len == 0
    release(active)

  test "timed out requests are removed and cannot consume a released slot":
    setMaxConcurrentReqs(1)
    setSessionQueueLimits(10, 5)
    let active = waitFor getSession(tweets)
    expect SessionBusyError:
      discard waitFor getSession(tweets)
    check sessionWaiters.len == 0
    release(active)
    let next = waitFor getSession(tweets)
    check next.pending == 1
    release(next)

  test "release cannot grant a slot after the wait deadline":
    setMaxConcurrentReqs(1)
    setSessionQueueLimits(10, 5)
    let active = waitFor getSession(tweets)
    let queued = getSession(tweets)
    sleep(15)
    release(active)
    expect SessionBusyError:
      discard waitFor queued
    check sessionPool[0].pending == 0
    check sessionWaiters.len == 0

  test "no sessions and genuine rate limits fail without queueing":
    sessionPool.setLen(0)
    expect NoSessionsError:
      discard waitFor getSession(tweets)
    sessionPool = @[newSession()]
    sessionPool[0].setRateLimit(tweets, 0, epochTime().int + 60, 100)
    expect NoSessionsError:
      discard waitFor getSession(tweets)
    check sessionWaiters.len == 0
    release(waitFor getSession(users))

  test "rate limits discovered in flight fail affected waiters only":
    setMaxConcurrentReqs(1)
    let active = waitFor getSession(tweets)
    let queuedTweets = getSession(tweets)
    let queuedUsers = getSession(users)
    active.setRateLimit(tweets, 0, epochTime().int + 60, 100)
    release(active)
    expect NoSessionsError:
      discard waitFor queuedTweets
    release(waitFor queuedUsers)
    check sessionWaiters.len == 0
    check sessionPool[0].pending == 0

  test "invalidating the last session wakes waiters with an auth error":
    setMaxConcurrentReqs(1)
    var active = waitFor getSession(tweets)
    let queued = getSession(tweets)
    invalidate(active)
    expect NoSessionsError:
      discard waitFor queued
    check sessionWaiters.len == 0
    check sessionPool.len == 0

  test "an upstream failure releases capacity for the next request":
    setMaxConcurrentReqs(1)
    let active = waitFor getSession(tweets)
    let queued = getSession(tweets)
    expect IOError:
      try:
        raise newException(IOError, "upstream disconnected")
      finally:
        release(active)
    release(waitFor queued)
    check sessionPool[0].pending == 0

  test "OAuth sessions use their own endpoint limits and queued slots":
    sessionPool = @[Session(kind: SessionKind.oauth,
      oauthToken: "test", oauthSecret: "test")]
    setMaxConcurrentReqs(1)
    let active = waitFor getSession(tweets)
    let queued = getSession(tweets)
    check not queued.finished
    release(active)
    let acquired = waitFor queued
    acquired.setRateLimit(tweets, 0, epochTime().int + 60, 100)
    release(acquired)
    expect NoSessionsError:
      discard waitFor getSession(tweets)
    release(waitFor getSession(users))
    check sessionPool[0].pending == 0
