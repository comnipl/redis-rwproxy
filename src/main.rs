mod command;
mod config;
mod proxy;
mod resp;
mod routing;
mod stats;

use clap::Parser;
use config::{Config, ProxyAuth, RedisEndpoint};
use stats::Stats;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command(
    name = "redis-rwproxy",
    version,
    about = "Transparent Redis master/replica proxy (RESP3-capable)"
)]
struct Args {
    /// Listen address, e.g. 0.0.0.0:8080
    listen: SocketAddr,

    /// Redis master URL, e.g. redis://user:pass@host:6379/0
    master_url: String,

    /// Redis replica URL, e.g. redis://user:pass@host:6380/0
    replica_url: String,

    /// Username required from clients (proxy-level AUTH). If omitted, defaults to "default".
    #[arg(long)]
    username: Option<String>,

    /// Password required from clients (proxy-level AUTH). If omitted, proxy does not enforce authentication.
    #[arg(long)]
    password: Option<String>,

    /// Backend connect timeout in milliseconds.
    #[arg(long, default_value_t = 3000)]
    connect_timeout_ms: u64,

    /// How long to wait for replica replies (including drain replies for dual-forward commands).
    /// On timeout, replica is disabled for that client and reads fall back to master.
    #[arg(long, default_value_t = 5000)]
    replica_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let master = RedisEndpoint::from_redis_url(&args.master_url)?;
    let replica = RedisEndpoint::from_redis_url(&args.replica_url)?;

    let proxy_auth = match args.password {
        Some(pw) => ProxyAuth {
            enabled: true,
            username: args.username.unwrap_or_else(|| "default".to_string()),
            password: pw,
        },
        None => ProxyAuth::disabled(),
    };

    let cfg = Arc::new(Config {
        listen: args.listen,
        master,
        replica,
        proxy_auth,
        connect_timeout: Duration::from_millis(args.connect_timeout_ms),
        replica_timeout: Duration::from_millis(args.replica_timeout_ms),
    });

    let stats = Arc::new(Stats::new());

    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "redis-rwproxy listening");

    tokio::select! {
        res = accept_loop(listener, cfg, stats.clone()) => {
            res?;
        }
        _ = shutdown_signal() => {
            tracing::info!("shutdown requested");
        }
    }

    // Print summary on exit.
    for line in stats.render_summary_lines() {
        println!("{line}");
    }

    Ok(())
}

async fn accept_loop(
    listener: TcpListener,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::info!(client = %addr, "accepted connection");
        let cfg = cfg.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            proxy::handle_client(socket, cfg, stats).await;
        });
    }
}

async fn shutdown_signal() {
    // Ctrl+C everywhere.
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}
