use anyhow::{Context, Result, anyhow};
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: std::net::SocketAddr,
    pub master: RedisEndpoint,
    pub replica: RedisEndpoint,
    pub proxy_auth: ProxyAuth,
    pub connect_timeout: Duration,
    pub replica_timeout: Duration,
    pub force_eval_readonly: bool,
    pub force_evalsha_readonly: bool,
}

#[derive(Clone, Debug)]
pub struct ProxyAuth {
    pub enabled: bool,
    pub username: String,
    pub password: String,
}

impl ProxyAuth {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            username: "default".to_string(),
            password: "".to_string(),
        }
    }

    pub fn verify(&self, username: &str, password: &str) -> bool {
        if !self.enabled {
            return true;
        }
        self.username == username && self.password == password
    }
}

#[derive(Clone, Debug)]
pub struct RedisEndpoint {
    #[allow(unused)]
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db: Option<u32>,
}

impl RedisEndpoint {
    pub fn from_redis_url(input: &str) -> Result<Self> {
        let url = Url::parse(input).with_context(|| format!("Invalid Redis URL: {input}"))?;
        let scheme = url.scheme().to_string();
        if scheme != "redis" {
            return Err(anyhow!(
                "Unsupported scheme '{scheme}' in URL '{input}'. Use redis://"
            ));
        }

        let host = url
            .host_str()
            .ok_or_else(|| anyhow!("Missing host in URL '{input}'"))?
            .to_string();

        let port = url.port().unwrap_or(6379);

        let username = {
            let u = url.username();
            if u.is_empty() {
                None
            } else {
                Some(u.to_string())
            }
        };

        let password = url.password().map(|p| p.to_string());

        let db = {
            let path = url.path().trim();
            if path.is_empty() || path == "/" {
                None
            } else {
                let p = path.strip_prefix('/').unwrap_or(path);
                if p.is_empty() {
                    None
                } else {
                    Some(
                        p.parse::<u32>()
                            .with_context(|| format!("Invalid db index in URL path: '{path}'"))?,
                    )
                }
            }
        };

        Ok(Self {
            scheme,
            host,
            port,
            username,
            password,
            db,
        })
    }
}
