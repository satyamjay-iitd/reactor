//! Where a node listens, and who may use its HTTP API.
//!
//! The HTTP API is the node's control plane: it compiles and loads code, and starts and stops
//! actors, so anyone who can call it can run code on the machine. It is called by the dashboard
//! and the job manager; nodes never call each other's API. Actors of different nodes talk over
//! the data plane instead: one TCP port per actor, bound to the same address as the API.

use std::net::{IpAddr, Ipv4Addr};

use axum::http::{HeaderValue, Uri};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Port of the HTTP API.
    pub port: u16,
    /// Address the HTTP API and the actors' data ports listen on. Loopback (the default) keeps
    /// the node private to this machine; a node of a multi-machine cluster needs an address the
    /// other machines can reach, e.g. `0.0.0.0`.
    pub bind: IpAddr,
    /// When set, every API request must carry `Authorization: Bearer <token>`. Required unless
    /// the node listens on loopback only.
    pub auth_token: Option<String>,
    /// Browser origins (e.g. `https://dashboard.example.com`) allowed to call the API, besides
    /// pages served from this machine (`http://localhost:*`, `http://127.0.0.1:*`).
    pub allowed_origins: Vec<String>,
}

impl NodeConfig {
    /// A node private to this machine: listens on loopback, without a token.
    pub fn local(port: u16) -> Self {
        NodeConfig {
            port,
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            auth_token: None,
            allowed_origins: Vec::new(),
        }
    }

    /// Rejects configurations that would expose the API to the network unauthenticated.
    pub fn validate(&self) -> Result<(), String> {
        if self.auth_token.as_deref() == Some("") {
            return Err("the auth token is empty".to_string());
        }
        if !self.bind.is_loopback() && self.auth_token.is_none() {
            return Err(format!(
                "refusing to listen on {} without an auth token: anyone who can reach the node \
                 could run code on it. Set a token, or listen on 127.0.0.1",
                self.bind
            ));
        }
        for origin in &self.allowed_origins {
            if HeaderValue::from_str(origin).is_err() || origin.ends_with('/') {
                return Err(format!(
                    "invalid allowed origin {origin:?}: expected scheme://host[:port]"
                ));
            }
        }
        Ok(())
    }

    /// Whether the request's `Authorization` header carries the token (or no token is needed).
    pub(crate) fn authorized(&self, authorization: Option<&HeaderValue>) -> bool {
        let Some(token) = &self.auth_token else {
            return true;
        };
        authorization
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|given| constant_time_eq(given.as_bytes(), token.as_bytes()))
    }

    /// Whether a browser page from `origin` may call the API.
    pub(crate) fn origin_allowed(&self, origin: &HeaderValue) -> bool {
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        is_loopback_origin(origin) || self.allowed_origins.iter().any(|allowed| allowed == origin)
    }
}

fn is_loopback_origin(origin: &str) -> bool {
    let Ok(uri) = origin.parse::<Uri>() else {
        return false;
    };
    matches!(uri.scheme_str(), Some("http" | "https"))
        && matches!(uri.host(), Some("localhost" | "127.0.0.1" | "[::1]"))
}

/// Compares without revealing, through timing, how much of the token was guessed right.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Command-line flags of a node binary: `#[command(flatten)] node: NodeArgs`.
#[cfg(feature = "cli")]
#[derive(clap::Args, Clone, Debug)]
pub struct NodeArgs {
    /// Port of the HTTP API
    #[arg(short, long, default_value = "8080")]
    pub port: u16,

    /// Address the HTTP API and the actors listen on. Use one the other machines can reach
    /// (e.g. 0.0.0.0) for a multi-machine cluster; that requires --auth-token.
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: IpAddr,

    /// Token API clients must send as `Authorization: Bearer <token>`
    #[arg(long, env = "REACTOR_AUTH_TOKEN", hide_env_values = true)]
    pub auth_token: Option<String>,

    /// Browser origin allowed to call the API, besides localhost (repeatable)
    #[arg(long = "allow-origin", value_name = "ORIGIN")]
    pub allowed_origins: Vec<String>,
}

#[cfg(feature = "cli")]
impl From<NodeArgs> for NodeConfig {
    fn from(args: NodeArgs) -> Self {
        NodeConfig {
            port: args.port,
            bind: args.bind,
            auth_token: args.auth_token,
            allowed_origins: args.allowed_origins,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_token(token: &str) -> NodeConfig {
        NodeConfig {
            auth_token: Some(token.to_string()),
            ..NodeConfig::local(8080)
        }
    }

    #[test]
    fn network_addresses_need_a_token() {
        let exposed = NodeConfig {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            ..NodeConfig::local(8080)
        };
        assert!(exposed.validate().is_err());
        assert!(
            NodeConfig {
                auth_token: Some("s3cret".into()),
                ..exposed.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            NodeConfig {
                auth_token: Some("".into()),
                ..exposed
            }
            .validate()
            .is_err()
        );
        assert!(NodeConfig::local(8080).validate().is_ok());
    }

    #[test]
    fn checks_the_bearer_token() {
        let header = |s: &'static str| HeaderValue::from_static(s);
        let config = with_token("s3cret");
        assert!(config.authorized(Some(&header("Bearer s3cret"))));
        assert!(!config.authorized(Some(&header("Bearer s3cre"))));
        assert!(!config.authorized(Some(&header("s3cret"))));
        assert!(!config.authorized(None));
        assert!(NodeConfig::local(8080).authorized(None));
    }

    #[test]
    fn allows_local_and_listed_origins() {
        let config = NodeConfig {
            allowed_origins: vec!["https://dash.example.com".into()],
            ..NodeConfig::local(8080)
        };
        let allowed = |s: &'static str| config.origin_allowed(&HeaderValue::from_static(s));
        assert!(allowed("http://localhost:5173"));
        assert!(allowed("http://127.0.0.1:3000"));
        assert!(allowed("http://[::1]:5173"));
        assert!(allowed("https://dash.example.com"));
        assert!(!allowed("https://evil.example.com"));
        assert!(!allowed("http://localhost.evil.com"));
        assert!(!allowed("null"));
        let bad = NodeConfig {
            allowed_origins: vec!["https://x.com/".into()],
            ..NodeConfig::local(1)
        };
        assert!(bad.validate().is_err());
    }
}
