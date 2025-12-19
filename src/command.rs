use anyhow::{Result, anyhow};
use bytes::Bytes;

use crate::resp::{Frame, Resp2Frame, Resp3Frame, RespVersion};

#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub name_upper: String,
    pub args: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct HelloRequest {
    pub protover: Option<RespVersion>,
    pub auth: Option<(String, String)>,
    pub setname: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Request {
    Command(ParsedCommand),
    Hello(HelloRequest),
}

pub fn parse_request(frame: &Frame) -> Result<Request> {
    match frame {
        Frame::Resp2(f) => parse_resp2(f),
        Frame::Resp3(f) => parse_resp3(f),
    }
}

fn parse_resp2(frame: &Resp2Frame) -> Result<Request> {
    let (name, args) = parse_resp2_command_parts(frame)?;
    if name == "HELLO" {
        let hello = parse_hello_args(RespVersion::Resp2, &args)?;
        Ok(Request::Hello(hello))
    } else {
        Ok(Request::Command(ParsedCommand {
            name_upper: name,
            args,
        }))
    }
}

fn parse_resp3(frame: &Resp3Frame) -> Result<Request> {
    match frame {
        // When the connection is in RESP3 mode, redis-protocol can decode HELLO requests into a dedicated variant.
        Resp3Frame::Hello {
            version,
            auth,
            setname,
        } => {
            let protover = match version {
                redis_protocol::resp3::types::RespVersion::RESP2 => RespVersion::Resp2,
                redis_protocol::resp3::types::RespVersion::RESP3 => RespVersion::Resp3,
            };

            let auth = auth.as_ref().map(|(u, p)| (u.to_string(), p.to_string()));
            let setname = setname.as_ref().map(|s| s.to_string());

            Ok(Request::Hello(HelloRequest {
                protover: Some(protover),
                auth,
                setname,
            }))
        }
        _ => {
            let (name, args) = parse_resp3_command_parts(frame)?;
            if name == "HELLO" {
                let hello = parse_hello_args(RespVersion::Resp3, &args)?;
                Ok(Request::Hello(hello))
            } else {
                Ok(Request::Command(ParsedCommand {
                    name_upper: name,
                    args,
                }))
            }
        }
    }
}

fn parse_resp2_command_parts(frame: &Resp2Frame) -> Result<(String, Vec<Bytes>)> {
    let Resp2Frame::Array(items) = frame else {
        return Err(anyhow!("Expected Array frame for Redis request"));
    };
    if items.is_empty() {
        return Err(anyhow!("Empty request array"));
    }

    let cmd =
        resp2_item_to_bytes(&items[0]).ok_or_else(|| anyhow!("Invalid command name frame"))?;
    let name = ascii_upper(&cmd);

    let mut args = Vec::with_capacity(items.len().saturating_sub(1));
    for it in items.iter().skip(1) {
        let b = resp2_item_to_bytes(it).ok_or_else(|| anyhow!("Invalid argument frame"))?;
        args.push(b);
    }

    Ok((name, args))
}

fn resp2_item_to_bytes(frame: &Resp2Frame) -> Option<Bytes> {
    match frame {
        Resp2Frame::BulkString(b) => Some(b.clone()),
        Resp2Frame::SimpleString(b) => Some(b.clone()),
        // Be permissive: some clients might send integers as ints.
        Resp2Frame::Integer(i) => Some(Bytes::copy_from_slice(i.to_string().as_bytes())),
        _ => None,
    }
}

fn parse_resp3_command_parts(frame: &Resp3Frame) -> Result<(String, Vec<Bytes>)> {
    let Resp3Frame::Array { data, .. } = frame else {
        return Err(anyhow!("Expected Array frame for Redis request"));
    };
    if data.is_empty() {
        return Err(anyhow!("Empty request array"));
    }

    let cmd = resp3_item_to_bytes(&data[0]).ok_or_else(|| anyhow!("Invalid command name frame"))?;
    let name = ascii_upper(&cmd);

    let mut args = Vec::with_capacity(data.len().saturating_sub(1));
    for it in data.iter().skip(1) {
        let b = resp3_item_to_bytes(it).ok_or_else(|| anyhow!("Invalid argument frame"))?;
        args.push(b);
    }

    Ok((name, args))
}

fn resp3_item_to_bytes(frame: &Resp3Frame) -> Option<Bytes> {
    match frame {
        Resp3Frame::BlobString { data, .. } => Some(data.clone()),
        Resp3Frame::SimpleString { data, .. } => Some(data.clone()),
        Resp3Frame::Number { data, .. } => {
            Some(Bytes::copy_from_slice(data.to_string().as_bytes()))
        }
        Resp3Frame::BigNumber { data, .. } => Some(data.clone()),
        Resp3Frame::VerbatimString { data, .. } => Some(data.clone()),
        _ => None,
    }
}

fn parse_hello_args(current: RespVersion, args: &[Bytes]) -> Result<HelloRequest> {
    // HELLO [protover] [AUTH username password] [SETNAME name]
    let mut idx = 0;
    let mut protover = None;

    if let Some(first) = args.first()
        && let Ok(s) = std::str::from_utf8(first)
    {
        if s == "2" {
            protover = Some(RespVersion::Resp2);
            idx = 1;
        } else if s == "3" {
            protover = Some(RespVersion::Resp3);
            idx = 1;
        }
    }

    let mut auth: Option<(String, String)> = None;
    let mut setname: Option<String> = None;

    while idx < args.len() {
        let token = args.get(idx).ok_or_else(|| anyhow!("HELLO parse error"))?;
        let token_upper = ascii_upper(token);
        idx += 1;

        match token_upper.as_str() {
            "AUTH" => {
                let u = args
                    .get(idx)
                    .ok_or_else(|| anyhow!("HELLO AUTH missing username"))?;
                let p = args
                    .get(idx + 1)
                    .ok_or_else(|| anyhow!("HELLO AUTH missing password"))?;
                idx += 2;
                let user = String::from_utf8_lossy(u).to_string();
                let pass = String::from_utf8_lossy(p).to_string();
                auth = Some((user, pass));
            }
            "SETNAME" => {
                let n = args
                    .get(idx)
                    .ok_or_else(|| anyhow!("HELLO SETNAME missing name"))?;
                idx += 1;
                setname = Some(String::from_utf8_lossy(n).to_string());
            }
            other => {
                return Err(anyhow!("Unsupported HELLO option: {other}"));
            }
        }
    }

    // If no protover was given, keep current.
    Ok(HelloRequest {
        protover: protover.or(Some(current)),
        auth,
        setname,
    })
}

fn ascii_upper(bytes: &Bytes) -> String {
    bytes
        .iter()
        .map(|b| b.to_ascii_uppercase() as char)
        .collect()
}
