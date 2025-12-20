#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Master,
    Replica,
    Both,
}

pub fn route_cmd(cmd_upper: &str, first_arg_upper: Option<&str>) -> Route {
    match (cmd_upper, first_arg_upper) {
        ("HELLO", _) => Route::Both,
        ("SELECT" | "READONLY" | "READWRITE", _) => Route::Both,
        ("CLIENT", Some("SETNAME" | "SETINFO" | "TRACKING" | "CACHING" | "REPLY")) => Route::Both,
        _ if is_always_master(cmd_upper) => Route::Master,
        _ if is_replica_read(cmd_upper) => Route::Replica,
        _ => Route::Master,
    }
}

/// Extremely conservative whitelist of commands that are safe to route to a read replica.
///
/// Policy: **default master, explicit allow-list only**.
fn is_replica_read(cmd_upper: &str) -> bool {
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
