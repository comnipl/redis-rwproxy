use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::command::{HelloRequest, ParsedCommand, Request, parse_request};
use crate::config::{Config, ProxyAuth, RedisEndpoint};
use crate::resp::{Frame, RespStream, RespVersion, encode_command, encode_command_str};
use crate::routing::{Route, is_always_master, is_dual_forward, is_replica_read_whitelisted};
use crate::stats::Stats;

#[derive(Debug, Clone, Copy)]
struct ConnState {
    in_multi: bool,
    watch_active: bool,
}

pub async fn handle_client(socket: TcpStream, cfg: Arc<Config>, stats: Arc<Stats>) {
    if let Err(e) = handle_client_inner(socket, cfg, stats).await {
        tracing::debug!(error = ?e, "connection terminated");
    }
}

async fn handle_client_inner(
    client_sock: TcpStream,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
) -> Result<()> {
    client_sock.set_nodelay(true)?;
    let mut client = RespStream::new(client_sock, RespVersion::Resp2);

    let mut master = connect_and_handshake(&cfg.master, cfg.connect_timeout).await?;
    let mut replica = match connect_and_handshake(&cfg.replica, cfg.connect_timeout).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = ?e, "replica unavailable at connect; falling back to master-only");
            None
        }
    };

    let mut authenticated = !cfg.proxy_auth.enabled;
    let mut state = ConnState {
        in_multi: false,
        watch_active: false,
    };

    loop {
        let Some((frame, raw)) = client.read_frame().await? else {
            break;
        };

        let req = match parse_request(&frame) {
            Ok(r) => r,
            Err(e) => {
                // Protocol errors are usually fatal.
                let _ = client
                    .write_all(format!("-ERR Protocol error: {e}\r\n").as_bytes())
                    .await;
                return Err(e);
            }
        };

        match req {
            Request::Hello(hello) => {
                handle_hello(
                    &mut client,
                    &mut master,
                    &mut replica,
                    &mut authenticated,
                    &cfg.proxy_auth,
                    cfg.replica_timeout,
                    &stats,
                    hello,
                )
                .await?;
                continue;
            }
            Request::Command(cmd) => {
                // Auth gate.
                if !authenticated && !is_auth_exempt(&cmd) {
                    client
                        .write_all(b"-NOAUTH Authentication required.\r\n")
                        .await?;
                    continue;
                }

                // Handle a few commands locally.
                if cmd.name_upper == "AUTH" {
                    handle_auth(&mut client, &mut authenticated, &cfg.proxy_auth, &cmd).await?;
                    continue;
                }
                if cmd.name_upper == "QUIT" {
                    client.write_all(b"+OK\r\n").await?;
                    break;
                }

                // Route and forward.
                let first_arg_upper = cmd
                    .args
                    .first()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_ascii_uppercase());

                let route =
                    decide_route(&cmd, first_arg_upper.as_deref(), &state, replica.is_some());

                match route {
                    Route::Master => {
                        stats.record(Route::Master, &cmd.name_upper);
                        forward_master(&mut client, &mut master, &raw).await?;
                    }
                    Route::Replica => {
                        if let Some(rep) = replica.as_mut() {
                            stats.record(Route::Replica, &cmd.name_upper);
                            let ok = forward_replica_with_fallback(
                                &mut client,
                                &mut master,
                                rep,
                                &raw,
                                cfg.replica_timeout,
                            )
                            .await?;
                            if !ok {
                                stats.record_replica_fallback(&cmd.name_upper);
                                replica = None;
                            }
                        } else {
                            stats.record(Route::Master, &cmd.name_upper);
                            forward_master(&mut client, &mut master, &raw).await?;
                        }
                    }
                    Route::Both => {
                        if replica.is_some() {
                            stats.record(Route::Both, &cmd.name_upper);
                            forward_both(
                                &mut client,
                                &mut master,
                                &mut replica,
                                &raw,
                                cfg.replica_timeout,
                            )
                            .await?;
                        } else {
                            // If replica is absent, this effectively becomes master-only.
                            stats.record(Route::Master, &cmd.name_upper);
                            forward_master(&mut client, &mut master, &raw).await?;
                        }
                    }
                }

                update_state(&mut state, &cmd);
            }
        }
    }

    // Best-effort shutdown.
    let _ = master.shutdown().await;
    if let Some(mut rep) = replica {
        let _ = rep.shutdown().await;
    }
    let _ = client.shutdown().await;

    Ok(())
}

fn is_auth_exempt(cmd: &ParsedCommand) -> bool {
    matches!(cmd.name_upper.as_str(), "AUTH" | "HELLO" | "QUIT")
}

fn decide_route(
    cmd: &ParsedCommand,
    first_arg_upper: Option<&str>,
    state: &ConnState,
    replica_available: bool,
) -> Route {
    // Force-master contexts.
    if state.in_multi || state.watch_active {
        return Route::Master;
    }
    if is_always_master(&cmd.name_upper) {
        return Route::Master;
    }

    if is_dual_forward(&cmd.name_upper, first_arg_upper) {
        return Route::Both;
    }

    if replica_available && is_replica_read_whitelisted(&cmd.name_upper) {
        return Route::Replica;
    }

    Route::Master
}

fn update_state(state: &mut ConnState, cmd: &ParsedCommand) {
    match cmd.name_upper.as_str() {
        "MULTI" => state.in_multi = true,
        "EXEC" | "DISCARD" => {
            state.in_multi = false;
            state.watch_active = false; // EXEC/DISCARD clears WATCH.
        }
        "WATCH" => state.watch_active = true,
        "UNWATCH" => state.watch_active = false,
        _ => {}
    }
}

async fn handle_auth(
    client: &mut RespStream,
    authenticated: &mut bool,
    proxy_auth: &ProxyAuth,
    cmd: &ParsedCommand,
) -> Result<()> {
    let (user, pass) = match cmd.args.len() {
        1 => (
            "default".to_string(),
            String::from_utf8_lossy(&cmd.args[0]).to_string(),
        ),
        2 => (
            String::from_utf8_lossy(&cmd.args[0]).to_string(),
            String::from_utf8_lossy(&cmd.args[1]).to_string(),
        ),
        _ => {
            client
                .write_all(b"-ERR wrong number of arguments for 'auth' command\r\n")
                .await?;
            return Ok(());
        }
    };

    if proxy_auth.verify(&user, &pass) {
        *authenticated = true;
        client.write_all(b"+OK\r\n").await?;
    } else {
        client
            .write_all(b"-WRONGPASS invalid username-password pair\r\n")
            .await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_hello(
    client: &mut RespStream,
    master: &mut RespStream,
    replica: &mut Option<RespStream>,
    authenticated: &mut bool,
    proxy_auth: &ProxyAuth,
    replica_timeout: std::time::Duration,
    stats: &Arc<Stats>,
    hello: HelloRequest,
) -> Result<()> {
    // If proxy-level auth is required, HELLO must either already be authenticated or carry AUTH.
    if proxy_auth.enabled {
        if let Some((u, p)) = &hello.auth {
            if proxy_auth.verify(u, p) {
                *authenticated = true;
            } else {
                client
                    .write_all(b"-WRONGPASS invalid username-password pair\r\n")
                    .await?;
                return Ok(());
            }
        }
        if !*authenticated {
            client
                .write_all(b"-NOAUTH Authentication required.\r\n")
                .await?;
            return Ok(());
        }
    }

    // Determine target protocol for this connection.
    let target = hello.protover.unwrap_or(client.version());

    // Build backend HELLO command. Do NOT forward client's AUTH to backends.
    // We keep optional SETNAME for transparency.
    let mut parts: Vec<Bytes> = Vec::new();
    parts.push(Bytes::from_static(b"HELLO"));
    parts.push(Bytes::copy_from_slice(match target {
        RespVersion::Resp2 => b"2",
        RespVersion::Resp3 => b"3",
    }));

    if let Some(name) = &hello.setname {
        parts.push(Bytes::from_static(b"SETNAME"));
        parts.push(Bytes::copy_from_slice(name.as_bytes()));
    }

    let hello_cmd = encode_command(&parts);

    // Stats: HELLO is effectively BOTH when replica is available, otherwise master-only.
    if replica.is_some() {
        stats.record(Route::Both, "HELLO");
    } else {
        stats.record(Route::Master, "HELLO");
    }

    // Send to master, switch protocol before reading the response.
    master.write_all(&hello_cmd).await?;
    master.set_version(target);

    if replica.is_some() {
        // Avoid holding a mutable borrow across `.await` so we can disable the replica on failures.
        let write_result = {
            let rep = replica.as_mut().unwrap();
            rep.write_all(&hello_cmd).await
        };

        match write_result {
            Ok(()) => {
                if let Some(rep) = replica.as_mut() {
                    rep.set_version(target);
                }
            }
            Err(e) => {
                tracing::warn!(error=?e, "replica write failed during HELLO; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
        }
    }

    // Read master's HELLO response and forward to client.
    let (_frame, raw) = read_one_reply_from_master(master, client).await?;
    client.set_version(target);
    client.write_all(&raw).await?;

    // Discard replica's HELLO response.
    if replica.is_some() {
        // Drain one reply from replica to keep stream aligned. If it fails/times out, disable replica.
        let drain_result = {
            let rep = replica.as_mut().unwrap();
            timeout(replica_timeout, rep.read_frame()).await
        };

        match drain_result {
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => {
                tracing::warn!("replica closed during HELLO reply drain; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error=?e, "replica read failed during HELLO reply drain; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
            Err(_) => {
                tracing::warn!("replica timeout during HELLO reply drain; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
        }
    }

    Ok(())
}

async fn forward_master(
    client: &mut RespStream,
    master: &mut RespStream,
    raw: &bytes::Bytes,
) -> Result<()> {
    master.write_all(raw.as_ref()).await?;
    let (_frame, reply_raw) = read_one_reply_from_master(master, client).await?;
    client.write_all(reply_raw.as_ref()).await?;
    Ok(())
}

async fn forward_both(
    client: &mut RespStream,
    master: &mut RespStream,
    replica: &mut Option<RespStream>,
    raw: &bytes::Bytes,
    replica_timeout: std::time::Duration,
) -> Result<()> {
    master.write_all(raw.as_ref()).await?;
    if replica.is_some() {
        let write_result = {
            let rep = replica.as_mut().unwrap();
            rep.write_all(raw.as_ref()).await
        };
        if let Err(e) = write_result {
            tracing::warn!(error=?e, "replica write failed; disabling replica");
            if let Some(mut rep) = replica.take() {
                let _ = rep.shutdown().await;
            }
        }
    }

    let (_frame, reply_raw) = read_one_reply_from_master(master, client).await?;
    client.write_all(reply_raw.as_ref()).await?;

    if replica.is_some() {
        let drain_result = {
            let rep = replica.as_mut().unwrap();
            timeout(replica_timeout, rep.read_frame()).await
        };

        match drain_result {
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => {
                tracing::warn!("replica closed while draining reply; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error=?e, "replica read failed while draining reply; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
            Err(_) => {
                tracing::warn!("replica read timeout while draining reply; disabling replica");
                if let Some(mut rep) = replica.take() {
                    let _ = rep.shutdown().await;
                }
            }
        }
    }

    Ok(())
}

/// Forward a whitelisted read to replica. If replica errors or times out, resend to master.
///
/// Returns `Ok(true)` if replica remains usable, `Ok(false)` if replica should be disabled.
async fn forward_replica_with_fallback(
    client: &mut RespStream,
    master: &mut RespStream,
    replica: &mut RespStream,
    raw: &bytes::Bytes,
    replica_timeout: std::time::Duration,
) -> Result<bool> {
    if let Err(e) = replica.write_all(raw.as_ref()).await {
        tracing::warn!(error=?e, "replica write failed; falling back to master");
        forward_master(client, master, raw).await?;
        return Ok(false);
    }

    match timeout(replica_timeout, replica.read_frame()).await {
        Ok(Ok(Some((_frame, reply_raw)))) => {
            client.write_all(reply_raw.as_ref()).await?;
            Ok(true)
        }
        Ok(Ok(None)) => {
            tracing::warn!("replica closed; falling back to master");
            forward_master(client, master, raw).await?;
            Ok(false)
        }
        Ok(Err(e)) => {
            tracing::warn!(error=?e, "replica read failed; falling back to master");
            forward_master(client, master, raw).await?;
            Ok(false)
        }
        Err(_) => {
            tracing::warn!("replica read timeout; falling back to master");
            forward_master(client, master, raw).await?;
            Ok(false)
        }
    }
}

async fn read_one_reply_from_master(
    master: &mut RespStream,
    client: &mut RespStream,
) -> Result<(Frame, bytes::Bytes)> {
    loop {
        let Some((frame, raw)) = master.read_frame().await? else {
            // Master went away => close client per spec.
            return Err(anyhow!("master connection closed"));
        };

        if let (Frame::Resp3(f), RespVersion::Resp3) = (&frame, master.version())
            && let crate::resp::Resp3Frame::Push { .. } = f
        {
            // Forward out-of-band push messages as-is.
            client.write_all(raw.as_ref()).await?;
            continue;
        }

        return Ok((frame, raw));
    }
}

async fn connect_and_handshake(
    endpoint: &RedisEndpoint,
    connect_timeout: std::time::Duration,
) -> Result<RespStream> {
    let addr = (&endpoint.host[..], endpoint.port);
    let sock = timeout(connect_timeout, TcpStream::connect(addr))
        .await
        .context("connect timeout")??;
    sock.set_nodelay(true)?;

    let mut stream = RespStream::new(sock, RespVersion::Resp2);

    // Backend AUTH
    if let Some(pass) = &endpoint.password {
        let user = endpoint.username.as_deref().unwrap_or("default");
        let cmd = if endpoint.username.is_some() {
            encode_command(&[
                Bytes::from_static(b"AUTH"),
                Bytes::copy_from_slice(user.as_bytes()),
                Bytes::copy_from_slice(pass.as_bytes()),
            ])
        } else {
            // Password-only AUTH is valid and implies the default user.
            encode_command(&[
                Bytes::from_static(b"AUTH"),
                Bytes::copy_from_slice(pass.as_bytes()),
            ])
        };

        stream.write_all(&cmd).await?;
        let Some((frame, raw)) = stream.read_frame().await? else {
            return Err(anyhow!("backend closed during AUTH"));
        };
        if is_error_reply(&frame) {
            return Err(anyhow!(
                "backend AUTH failed: {}",
                String::from_utf8_lossy(&raw)
            ));
        }
    }

    // Backend SELECT
    if let Some(db) = endpoint.db {
        let cmd = encode_command_str(&["SELECT", &db.to_string()]);
        stream.write_all(&cmd).await?;
        let Some((frame, raw)) = stream.read_frame().await? else {
            return Err(anyhow!("backend closed during SELECT"));
        };
        if is_error_reply(&frame) {
            return Err(anyhow!(
                "backend SELECT failed: {}",
                String::from_utf8_lossy(&raw)
            ));
        }
    }

    Ok(stream)
}

fn is_error_reply(frame: &Frame) -> bool {
    match frame {
        Frame::Resp2(f) => matches!(f, crate::resp::Resp2Frame::Error(_)),
        Frame::Resp3(f) => matches!(
            f,
            crate::resp::Resp3Frame::SimpleError { .. } | crate::resp::Resp3Frame::BlobError { .. }
        ),
    }
}
