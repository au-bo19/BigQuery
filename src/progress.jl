# A progress indicator on one line of stderr.
#
# The worker tasks only change counters. A timer draws the line a few times each
# second. Thus the line appears on time and keeps moving also when no rows arrive:
# while the query runs, and while a set of large REST pages downloads in parallel.
# For those pages, the bytes that have arrived so far count as an estimate of rows.

const PROGRESS_DELAY = 1.0        # show nothing for fast operations
const PROGRESS_INTERVAL = 0.25
const PROGRESS_WIDTH = 60

mutable struct Progress
    enabled::Bool
    total::Int                            # -1 while the query runs
    done::Threads.Atomic{Int}             # the rows that are complete
    inflight::Threads.Atomic{Int}         # the bytes received for pages that are not complete
    sample_rows::Threads.Atomic{Int}      # the rows and bytes of the complete pages: they
    sample_bytes::Threads.Atomic{Int}     # give the number of rows in the `inflight` bytes
    lock::ReentrantLock
    started::Float64                      # the start of the query, or of the download
    shown::Int                            # the largest count on the line; it never goes back
    printed::Bool
    closed::Bool
    timer::Union{Nothing,Timer}
end

"""
    Progress(enabled, total; tick = enabled)

A progress line for `total` rows. A `total` of -1 means that the query still runs.
With `tick = false` there is no timer, and only [`draw!`](@ref) prints (for tests).
"""
function Progress(enabled::Bool, total::Integer; tick::Bool = enabled)
    p = Progress(enabled, Int(total), Threads.Atomic{Int}(0), Threads.Atomic{Int}(0),
                 Threads.Atomic{Int}(0), Threads.Atomic{Int}(0), ReentrantLock(), time(),
                 0, false, false, nothing)
    if enabled && tick
        p.timer = Timer(_ -> draw!(p), PROGRESS_DELAY; interval = PROGRESS_INTERVAL)
    end
    return p
end

function format_count(n::Integer)
    n >= 1_000_000 && return string(round(n / 1_000_000; digits = 2), "M")
    n >= 10_000 && return string(round(n / 1_000; digits = 1), "k")
    return string(n)
end

"A byte count in decimal units with three significant digits, for example \"2.35 GB\"."
function format_bytes(n::Integer)
    units = ("B", "kB", "MB", "GB", "TB", "PB", "EB")
    x = Float64(n)
    i = 1
    while i < length(units) && round(x; sigdigits = 3) >= 1000
        x /= 1000
        i += 1
    end
    s = string(i == 1 ? Int(x) : round(x; sigdigits = 3))
    endswith(s, ".0") && (s = chop(s; tail = 2))
    return string(s, " ", units[i])
end

advance!(p::Progress, n::Integer) = (Threads.atomic_add!(p.done, Int(n)); nothing)

"The rows to show: the complete rows, and an estimate of the rows in the bytes in flight."
function shown_count(p::Progress)
    done = p.done[]
    bytes = p.sample_bytes[]
    extra = bytes > 0 ? round(Int, p.inflight[] * (p.sample_rows[] / bytes)) : 0
    estimate = min(done + max(extra, 0), max(p.total, done))
    p.shown = max(p.shown, estimate)
    return p.shown
end

function status_line(p::Progress, now::Float64)
    elapsed = now - p.started
    p.total < 0 && return string("BigQuery: running query (", round(Int, elapsed), "s)")
    n = shown_count(p)
    rate = n / max(elapsed, 1.0e-3)
    return string("BigQuery: ", format_count(n), " / ", format_count(p.total),
                  " rows (", format_count(round(Int, rate)), " rows/s)")
end

"Print the line again. The timer calls this function."
function draw!(p::Progress)
    p.enabled || return nothing
    try
        lock(p.lock) do
            p.closed && return
            p.printed = true
            print(stderr, "\r", rpad(status_line(p, time()), PROGRESS_WIDTH))
        end
    catch
        # A progress line must never stop a download.
    end
    return nothing
end

function erase_line(p::Progress)
    p.printed && print(stderr, "\r", " "^PROGRESS_WIDTH, "\r")
    p.printed = false
    return nothing
end

"Remove the line, for example before a log message. The timer prints it again."
clear!(p::Progress) = (lock(() -> erase_line(p), p.lock); nothing)

"The query has finished: from now on, the line counts the `total` rows."
function set_total!(p::Progress, total::Integer)
    lock(p.lock) do
        p.total = Int(total)
        p.started = time()
    end
    return nothing
end

"Count from zero again, for a second download (after a fallback to REST)."
function restart!(p::Progress)
    lock(p.lock) do
        p.done[] = 0
        p.inflight[] = 0
        p.shown = 0
        p.started = time()
    end
    return nothing
end

function finish!(p::Progress)
    lock(p.lock) do
        p.closed = true
        p.timer === nothing || close(p.timer)
        erase_line(p)
    end
    return nothing
end

"""
Counts the bytes of one HTTP response while they arrive. It is a `progress` callback
for `Downloads.request`. If the counter has a progress line, the bytes count as rows
in flight until [`sample!`](@ref).
"""
mutable struct ByteCounter <: Function
    bar::Union{Nothing,Progress}
    received::Int      # the bytes of all the attempts
    last::Int          # the count of the current attempt at the previous call
end

# The arguments: download total, download now, upload total, upload now.
function (c::ByteCounter)(_, dl_now::Integer, _, _)
    dl_now < c.last && (c.last = 0)       # a new attempt of the request
    delta = Int(dl_now) - c.last
    delta > 0 || return nothing
    c.last = Int(dl_now)
    c.received += delta
    c.bar === nothing || Threads.atomic_add!(c.bar.inflight, delta)
    return nothing
end

"""
A byte counter for one request, or `nothing` if `p` shows nothing. With
`inflight = false`, the bytes do not count on the line until the request is complete
(for responses that may not contain rows, such as query polls).
"""
byte_counter(p::Progress; inflight::Bool = true) =
    p.enabled ? ByteCounter(inflight ? p : nothing, 0, 0) : nothing

"A response with `rows` rows is complete. Its bytes improve the estimate of rows per byte."
function sample!(p::Progress, c::Union{Nothing,ByteCounter}, rows::Integer)
    c === nothing && return nothing
    c.bar === nothing || Threads.atomic_sub!(p.inflight, c.received)
    if c.received > 0 && rows > 0
        Threads.atomic_add!(p.sample_rows, Int(rows))
        Threads.atomic_add!(p.sample_bytes, c.received)
    end
    return nothing
end

"A page with `rows` rows is complete: count the rows (first, thus the line does not go back)."
function settle!(p::Progress, c::Union{Nothing,ByteCounter}, rows::Integer)
    advance!(p, rows)
    sample!(p, c, rows)
    return nothing
end
