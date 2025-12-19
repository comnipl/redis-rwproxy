#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Master,
    Replica,
    Both,
}

/// Extremely conservative whitelist of commands that are safe to route to a read replica.
///
/// Policy: **default master, explicit allow-list only**.
pub fn is_replica_read_whitelisted(cmd_upper: &str) -> bool {
    matches!(
        cmd_upper,
        // connection / healthcheck
        "PING" |
        // scan family (cursor-based iterators)
        "SCAN" | "SSCAN" | "HSCAN" | "ZSCAN" |
        // strings
        "GET" | "MGET" | "GETRANGE" | "STRLEN" |
        // hashes
        "HGET" | "HMGET" | "HGETALL" | "HEXISTS" | "HLEN" | "HSTRLEN" | "HKEYS" | "HVALS" |
        // lists
        "LINDEX" | "LLEN" | "LRANGE" |
        // sets
        "SCARD" | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS" | "SRANDMEMBER" |
        // sorted sets
        "ZCARD" | "ZCOUNT" | "ZRANGE" | "ZRANGEBYSCORE" | "ZREVRANGE" | "ZREVRANGEBYSCORE" |
        "ZRANK" | "ZREVRANK" | "ZSCORE" | "ZMSCORE" |
        // generic
        "EXISTS" | "TYPE" | "TTL" | "PTTL"
    )
}

/// Commands (or subcommands) that should always be forwarded to both master and replica.
///
/// *Response is always the master's response.*
pub fn is_dual_forward(cmd_upper: &str, first_arg_upper: Option<&str>) -> bool {
    match cmd_upper {
        "SELECT" | "READONLY" | "READWRITE" => true,
        "CLIENT" => matches!(
            first_arg_upper.unwrap_or(""),
            "SETNAME" | "SETINFO" | "TRACKING" | "CACHING" | "REPLY"
        ),
        // HELLO is treated specially (we don't forward client AUTH to backends), but protocol / setname state
        // must be applied on both connections.
        "HELLO" => true,
        _ => false,
    }
}

/// Commands that are always routed to the master regardless of whitelist.
///
/// This includes scripting and other constructs where reads/writes can be mixed, or where semantics depend on connection state.
pub fn is_always_master(cmd_upper: &str) -> bool {
    matches!(
        cmd_upper,
        "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WATCH"
            | "UNWATCH"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "SCRIPT"
            | "FUNCTION"
            | "FCALL"
            | "FCALL_RO"
            | "MONITOR"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "SSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "UNSUBSCRIBE"
            | "SUNSUBSCRIBE"
    )
}
