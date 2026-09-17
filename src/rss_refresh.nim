# SPDX-License-Identifier: AGPL-3.0-only
import std/[asyncdispatch, tables]
import types

var rssRefreshes: Table[string, Future[Rss]]

proc shareRssRefresh*(key: string; fetch: proc(): Future[Rss] {.closure.}): Future[Rss] {.async.} =
  # concurrent callers for the same key share one in-flight fetch
  if key notin rssRefreshes:
    rssRefreshes[key] = fetch()
  let future = rssRefreshes[key]
  try:
    return await future
  finally:
    if rssRefreshes.getOrDefault(key) == future:
      rssRefreshes.del(key)
