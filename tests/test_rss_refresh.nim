# SPDX-License-Identifier: AGPL-3.0-only
import std/[asyncdispatch, unittest]
import ../src/[rss_refresh, types]

discard getGlobalDispatcher()

suite "shared RSS refreshes":
  test "simultaneous readers share one refresh and receive identical content":
    let gate = newFuture[void]("refresh gate")
    var calls = 0
    proc fetch(): Future[Rss] {.async.} =
      inc calls
      await gate
      return Rss(feed: "fresh feed", cursor: "next")
    var readers: seq[Future[Rss]]
    for i in 0 ..< 30:
      readers.add shareRssRefresh("twitter:user", fetch)
    check calls == 1
    gate.complete()
    for rss in waitFor all(readers):
      check rss == Rss(feed: "fresh feed", cursor: "next")
    discard waitFor shareRssRefresh("twitter:user", fetch)
    check calls == 2

  test "different feeds, tabs, search queries and cursors stay independent":
    let gate = newFuture[void]("refresh gate")
    var calls = 0
    proc fetch(): Future[Rss] {.async.} =
      inc calls
      await gate
      return Rss(feed: "feed", cursor: "next")
    var readers: seq[Future[Rss]]
    for key in ["twitter:a", "twitter:b", "media:a", "twitter:a:cursor",
                "search:query1", "search:query2", "lists:123"]:
      readers.add shareRssRefresh(key, fetch)
    check calls == 7
    gate.complete()
    discard waitFor all(readers)

  test "upstream errors reach all readers and the next refresh can retry":
    let gate = newFuture[void]("refresh gate")
    var calls = 0
    proc fetch(): Future[Rss] {.async.} =
      inc calls
      if calls == 1:
        await gate
        raise newException(SessionBusyError, "busy")
      return Rss(feed: "recovered", cursor: "next")
    let first = shareRssRefresh("twitter:failure", fetch)
    let second = shareRssRefresh("twitter:failure", fetch)
    gate.complete()
    expect SessionBusyError:
      discard waitFor first
    expect SessionBusyError:
      discard waitFor second
    check calls == 1
    check (waitFor shareRssRefresh("twitter:failure", fetch)).feed == "recovered"
    check calls == 2

  test "a synchronous callback failure does not leave a stuck refresh":
    proc broken(): Future[Rss] =
      raise newException(IOError, "failed before returning a future")
    proc recovered(): Future[Rss] {.async.} =
      return Rss(feed: "recovered", cursor: "next")
    expect IOError:
      discard waitFor shareRssRefresh("twitter:sync", broken)
    check (waitFor shareRssRefresh("twitter:sync", recovered)).feed == "recovered"

  test "empty and suspended results are passed through unchanged":
    proc empty(): Future[Rss] {.async.} = return Rss()
    proc suspended(): Future[Rss] {.async.} =
      return Rss(feed: "user", cursor: "suspended")
    check (waitFor shareRssRefresh("twitter:empty", empty)) == Rss()
    check (waitFor shareRssRefresh("twitter:suspended", suspended)).cursor == "suspended"
