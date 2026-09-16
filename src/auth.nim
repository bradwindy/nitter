#SPDX-License-Identifier: AGPL-3.0-only
import std/[asyncdispatch, times, json, random, strutils, tables, packedsets, os, monotimes]
import types, consts
import experimental/parser/session

type SessionWaiter = object
  req: ApiReq
  future: Future[Session]
  deadline: MonoTime

const hourInSeconds = 60 * 60

var
  sessionPool: seq[Session]
  enableLogging = false
  # max requests at a time per session to avoid race conditions
  maxConcurrentReqs = 2
  maxPendingReqs = 100
  sessionWaitMs = 10000
  sessionWaiters: seq[SessionWaiter]

proc setMaxConcurrentReqs*(reqs: int) =
  if reqs > 0:
    maxConcurrentReqs = reqs

proc setSessionQueueLimits*(maxPending, waitMs: int) =
  maxPendingReqs = max(0, maxPending)
  sessionWaitMs = max(0, waitMs)

template log(str: varargs[string, `$`]) =
  echo "[sessions] ", str.join("")

proc endpoint*(req: ApiReq; session: Session): string =
  case session.kind
  of oauth: req.oauth.endpoint
  of cookie: req.cookie.endpoint

proc pretty*(session: Session): string =
  if session.isNil:
    return "<null>"

  if session.id > 0 and session.username.len > 0:
    result = $session.id & " (" & session.username & ")"
  elif session.username.len > 0:
    result = session.username
  elif session.id > 0:
    result = $session.id
  else:
    result = "<unknown>"
  result = $session.kind & " " & result

proc snowflakeToEpoch(flake: int64): int64 =
  int64(((flake shr 22) + 1288834974657) div 1000)

proc getSessionPoolHealth*(): JsonNode =
  let now = epochTime().int

  var
    totalReqs = 0
    limited: PackedSet[int64]
    reqsPerApi: Table[string, int]
    oldest = now.int64
    newest = 0'i64
    average = 0'i64
    oauthTotal, cookieTotal = 0
    oauthLimited, cookieLimited = 0

  for session in sessionPool:
    let created = snowflakeToEpoch(session.id)
    if created > newest:
      newest = created
    if created < oldest:
      oldest = created
    average += created

    case session.kind
    of oauth: inc oauthTotal
    of cookie: inc cookieTotal

    if session.limited:
      limited.incl session.id
      case session.kind
      of oauth: inc oauthLimited
      of cookie: inc cookieLimited

    for api in session.apis.keys:
      let
        apiStatus = session.apis[api]
        reqs = apiStatus.limit - apiStatus.remaining

      # no requests made with this session and endpoint since the limit reset
      if apiStatus.reset < now:
        continue

      reqsPerApi.mgetOrPut($api, 0).inc reqs
      totalReqs.inc reqs

  if sessionPool.len > 0:
    average = average div sessionPool.len
  else:
    oldest = 0
    average = 0

  return %*{
    "sessions": %*{
      "total": sessionPool.len,
      "limited": limited.card,
      "oauth": %*{"total": oauthTotal, "limited": oauthLimited},
      "cookie": %*{"total": cookieTotal, "limited": cookieLimited},
      "oldest": $fromUnix(oldest),
      "newest": $fromUnix(newest),
      "average": $fromUnix(average)
    },
    "requests": %*{
      "total": totalReqs,
      "apis": reqsPerApi
    }
  }

proc getSessionPoolDebug*(): JsonNode =
  let now = epochTime().int
  var list = newJObject()

  for session in sessionPool:
    let sessionJson = %*{
      "kind": $session.kind,
      "apis": newJObject(),
      "pending": session.pending,
    }

    if session.limited:
      sessionJson["limited"] = %true

    for api in session.apis.keys:
      let
        apiStatus = session.apis[api]
        obj = %*{}

      if apiStatus.reset > now.int:
        obj["remaining"] = %apiStatus.remaining
        obj["reset"] = %apiStatus.reset

      if "remaining" notin obj:
        continue

      sessionJson{"apis", $api} = obj
      list[$session.id] = sessionJson

  return %list

proc rateLimitError*(): ref RateLimitError =
  newException(RateLimitError, "rate limited")

proc noSessionsError*(): ref NoSessionsError =
  newException(NoSessionsError, "no sessions available")

proc isLimited(session: Session; req: ApiReq): bool =
  if session.isNil:
    return true

  let api = req.endpoint(session)
  if session.limited and api != graphUserTweetsV2:
    if (epochTime().int - session.limitedAt) > hourInSeconds:
      session.limited = false
      log "resetting limit: ", session.pretty
      return false
    else:
      return true

  if api in session.apis:
    let limit = session.apis[api]
    return limit.remaining <= 10 and limit.reset > epochTime().int
  else:
    return false

proc availableSession(req: ApiReq): tuple[session: Session, busy: bool] =
  if sessionPool.len == 0: return
  let start = rand(sessionPool.high)
  for offset in 0 ..< sessionPool.len:
    let session = sessionPool[(start + offset) mod sessionPool.len]
    if session.isLimited(req): continue
    if session.pending < maxConcurrentReqs:
      return (session, false)
    result.busy = true

proc busyError(): ref SessionBusyError =
  newException(SessionBusyError, "session queue is full or timed out")

proc processSessionWaiters() =
  var i = 0
  while i < sessionWaiters.len:
    let waiter = sessionWaiters[i]
    let available = availableSession(waiter.req)
    if getMonoTime() >= waiter.deadline:
      sessionWaiters.delete(i)
      waiter.future.fail(busyError())
    elif not available.session.isNil:
      sessionWaiters.delete(i)
      inc available.session.pending
      waiter.future.complete(available.session)
    elif not available.busy:
      sessionWaiters.delete(i)
      waiter.future.fail(noSessionsError())
    else:
      inc i

proc invalidate*(session: var Session) =
  if session.isNil: return
  log "invalidating: ", session.pretty

  let idx = sessionPool.find(session)
  if idx > -1: sessionPool.delete(idx)
  session = nil
  processSessionWaiters()

proc release*(session: Session) =
  if session.isNil: return
  dec session.pending
  processSessionWaiters()

proc getSession*(req: ApiReq): Future[Session] {.async.} =
  let available = availableSession(req)
  if not available.session.isNil:
    inc available.session.pending
    return available.session
  if not available.busy:
    log "no sessions available for API: ", req.cookie.endpoint
    raise noSessionsError()
  if sessionWaiters.len >= maxPendingReqs or sessionWaitMs == 0:
    raise busyError()

  let future = newFuture[Session]("getSession.wait")
  sessionWaiters.add SessionWaiter(req: req, future: future,
    deadline: getMonoTime() + initDuration(milliseconds=sessionWaitMs))
  try:
    if not await withTimeout(future, sessionWaitMs):
      raise busyError()
    return await future
  finally:
    for i in 0 ..< sessionWaiters.len:
      if sessionWaiters[i].future == future:
        sessionWaiters.delete(i)
        break

proc setLimited*(session: Session; req: ApiReq) =
  let api = req.endpoint(session)
  session.limited = true
  session.limitedAt = epochTime().int
  log "rate limited by api: ", api, ", reqs left: ", session.apis[api].remaining, ", ", session.pretty

proc setRateLimit*(session: Session; req: ApiReq; remaining, reset, limit: int) =
  # avoid undefined behavior in race conditions
  let api = req.endpoint(session)
  if api in session.apis:
    let rateLimit = session.apis[api]
    if rateLimit.reset >= reset and rateLimit.remaining < remaining:
      return
    if rateLimit.reset == reset and rateLimit.remaining >= remaining:
      session.apis[api].remaining = remaining
      return

  session.apis[api] = RateLimit(limit: limit, remaining: remaining, reset: reset)

proc initSessionPool*(cfg: Config; path: string) =
  enableLogging = cfg.enableDebug

  if path.endsWith(".json"):
    log "ERROR: .json is not supported, the file must be a valid JSONL file ending in .jsonl"
    quit 1

  if not fileExists(path):
    log "ERROR: ", path, " not found. This file is required to authenticate API requests."
    quit 1

  log "parsing JSONL account sessions file: ", path
  for line in path.lines:
    sessionPool.add parseSession(line)

  log "successfully added ", sessionPool.len, " valid account sessions"
