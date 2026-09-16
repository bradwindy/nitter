# SPDX-License-Identifier: AGPL-3.0-only
import std/[asyncdispatch, tables]
import types

var rssRefreshes: Table[string, Future[Rss]]

proc shareRssRefresh*(key: string; fetch: proc(): Future[Rss] {.closure.}): Future[Rss] {.async.} =
  if key in rssRefreshes:
    return await rssRefreshes[key]

  let future = newFuture[Rss]("shareRssRefresh")
  rssRefreshes[key] = future
  try:
    future.complete(await fetch())
  except CatchableError as e:
    future.fail(e)
  finally:
    rssRefreshes.del(key)
  return await future
