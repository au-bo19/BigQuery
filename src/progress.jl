# A progress indicator on one line of stderr.

mutable struct Progress
    enabled::Bool
    total::Int
    done::Threads.Atomic{Int}
    lock::ReentrantLock
    started::Float64
    last::Float64
    printed::Bool
end

Progress(enabled::Bool, total::Integer) =
    Progress(enabled, Int(total), Threads.Atomic{Int}(0), ReentrantLock(), time(), 0.0, false)

function format_count(n::Integer)
    n >= 1_000_000 && return string(round(n / 1_000_000; digits = 2), "M")
    n >= 10_000 && return string(round(n / 1_000; digits = 1), "k")
    return string(n)
end

function advance!(p::Progress, n::Integer)
    done = Threads.atomic_add!(p.done, Int(n)) + n
    p.enabled || return nothing
    now = time()
    # Show nothing for fast downloads. After that, update the line a few times each second.
    (now - p.started > 1.0 && now - p.last > 0.25) || return nothing
    if trylock(p.lock)
        try
            p.last = now
            p.printed = true
            rate = done / max(now - p.started, 1.0e-3)
            line = string("BigQuery: ", format_count(done), " / ", format_count(p.total),
                          " rows (", format_count(round(Int, rate)), " rows/s)")
            print(stderr, "\r", rpad(line, 60))
        finally
            unlock(p.lock)
        end
    end
    return nothing
end

function finish!(p::Progress)
    if p.printed
        print(stderr, "\r", " "^60, "\r")
        p.printed = false
    end
    return nothing
end
