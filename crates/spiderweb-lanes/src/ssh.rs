//! SSH lane — bus traffic carried through a system-`ssh` port-forward, so every
//! byte is encrypted and authenticated with ZERO crypto dependencies.
//!
//! SSH's key exchange + symmetric encryption + MAC is a **real wall** for a
//! zero-dep crate (the same crypto wall the edge crate's honest-scope note
//! refuses to reimplement). So instead of cracking it, this lane **composes**
//! the sealed OpenSSH client through its exposed surface — its CLI: it builds an
//! `ssh -N -L`/`-R` argv, spawns + supervises the child, and chains the
//! UNCHANGED edge transport through the forwarded loopback port:
//! `Edge over (ssh-forwarded localhost) over network`.
//!
//! Mechanism lives here (argv, spawn, supervise, hand off the address); policy
//! (which host/user/key, when to restart on `is_up()==false`) lives in the
//! orchestrator. The [`SshTunnel`] is an RAII guard: dropping it kills the ssh
//! child (mirrors `EdgeListener`'s teardown). It rides the lane's `guard` slot.

use spiderweb_edge::{Edge, EdgeListener, EdgeSender};
use std::ffi::{OsStr, OsString};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Which way the forward points: `-L` (local, egress) or `-R` (remote, ingress).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Local,
    Remote,
}

/// Everything needed to render an `ssh` argv. Pure data — no process is spawned.
#[derive(Clone, Debug)]
pub struct SshForward {
    pub direction: Direction,
    pub bind_host: String,
    pub bind_port: u16,
    pub dest_host: String,
    pub dest_port: u16,
    pub user: Option<String>,
    pub host: String,
    pub ssh_port: Option<u16>,
    pub identity: Option<PathBuf>,
    pub server_alive_interval: u32,
    pub server_alive_count_max: u32,
    /// `false` => add `StrictHostKeyChecking=accept-new` (CI/first-run); `true`
    /// => leave ssh's default (prod, requires a known_hosts entry).
    pub strict_host_key: bool,
    pub extra_opts: Vec<String>,
}

impl SshForward {
    fn base(direction: Direction, host: impl Into<String>, bind_port: u16, dest_port: u16) -> Self {
        SshForward {
            direction,
            bind_host: "127.0.0.1".into(),
            bind_port,
            dest_host: "127.0.0.1".into(),
            dest_port,
            user: None,
            host: host.into(),
            ssh_port: None,
            identity: None,
            server_alive_interval: 15,
            server_alive_count_max: 3,
            strict_host_key: false,
            extra_opts: Vec::new(),
        }
    }

    /// A local (`-L`) forward: `127.0.0.1:local_port` → (via ssh host) →
    /// `127.0.0.1:dest_port`.
    pub fn local(host: impl Into<String>, local_port: u16, dest_port: u16) -> Self {
        Self::base(Direction::Local, host, local_port, dest_port)
    }

    /// A reverse (`-R`) forward: the ssh server's `127.0.0.1:remote_port` →
    /// (back through the tunnel) → our `127.0.0.1:dest_port`.
    pub fn reverse(host: impl Into<String>, remote_port: u16, dest_port: u16) -> Self {
        Self::base(Direction::Remote, host, remote_port, dest_port)
    }

    pub fn user(mut self, u: impl Into<String>) -> Self {
        self.user = Some(u.into());
        self
    }
    pub fn identity(mut self, p: impl Into<PathBuf>) -> Self {
        self.identity = Some(p.into());
        self
    }
    pub fn ssh_port(mut self, p: u16) -> Self {
        self.ssh_port = Some(p);
        self
    }
    /// `true` accepts a new host key automatically (adds
    /// `StrictHostKeyChecking=accept-new`); `false` uses ssh's strict default.
    pub fn allow_new_host_key(mut self, yes: bool) -> Self {
        self.strict_host_key = !yes;
        self
    }

    /// Render the full argv (element 0 is `"ssh"`). PURE — the unit-test heart.
    pub fn to_argv(&self) -> Vec<String> {
        let mut a: Vec<String> = vec!["ssh".into(), "-N".into()];
        a.push("-o".into());
        a.push("ExitOnForwardFailure=yes".into());
        a.push("-o".into());
        a.push(format!(
            "ServerAliveInterval={}",
            self.server_alive_interval
        ));
        a.push("-o".into());
        a.push(format!(
            "ServerAliveCountMax={}",
            self.server_alive_count_max
        ));
        if !self.strict_host_key {
            a.push("-o".into());
            a.push("StrictHostKeyChecking=accept-new".into());
        }
        if let Some(p) = self.ssh_port {
            a.push("-p".into());
            a.push(p.to_string());
        }
        if let Some(id) = &self.identity {
            a.push("-i".into());
            a.push(id.to_string_lossy().into_owned());
        }
        a.push(match self.direction {
            Direction::Local => "-L".into(),
            Direction::Remote => "-R".into(),
        });
        a.push(format!(
            "{}:{}:{}:{}",
            self.bind_host, self.bind_port, self.dest_host, self.dest_port
        ));
        for o in &self.extra_opts {
            a.push("-o".into());
            a.push(o.clone());
        }
        // `--` terminates option parsing so a host/user beginning with `-` can't
        // be smuggled in as an ssh option (e.g. `-oProxyCommand=…`).
        a.push("--".into());
        a.push(match &self.user {
            Some(u) => format!("{u}@{}", self.host),
            None => self.host.clone(),
        });
        a
    }
}

/// Why opening a tunnel failed.
#[derive(Debug)]
pub enum SshLaneError {
    /// The `ssh` binary was not found on PATH.
    SshNotFound,
    /// Spawning the child failed for another reason.
    SpawnFailed(std::io::Error),
    /// The forward never became ready before the deadline.
    ForwardNotReady,
    /// The ssh child exited during the readiness wait (bad auth / bind fail).
    SshExited(Option<i32>),
    /// Pre-picking a free local port failed.
    Bind(std::io::Error),
    /// The edge transport failed to bind the forwarded port.
    Edge(std::io::Error),
}

impl std::fmt::Display for SshLaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SshLaneError::SshNotFound => write!(f, "ssh binary not found on PATH"),
            SshLaneError::SpawnFailed(e) => write!(f, "spawning ssh failed: {e}"),
            SshLaneError::ForwardNotReady => write!(f, "ssh forward did not become ready"),
            SshLaneError::SshExited(c) => write!(f, "ssh exited during setup (code {c:?})"),
            SshLaneError::Bind(e) => write!(f, "pre-picking a local port failed: {e}"),
            SshLaneError::Edge(e) => write!(f, "edge bind on the forwarded port failed: {e}"),
        }
    }
}
impl std::error::Error for SshLaneError {}

/// Wraps the ssh `Child`, killing+reaping it on drop (RAII).
struct SshChild {
    child: Child,
}

impl Drop for SshChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A live, supervised ssh port-forward. While this guard is alive the tunnel is
/// up; dropping it kills the ssh child.
pub struct SshTunnel {
    child: SshChild,
    local_addr: SocketAddr,
}

impl SshTunnel {
    /// Build argv from `fwd`, spawn `ssh -N`, and block until ready (the
    /// forwarded local port accepts a connection, for `-L`) or the deadline.
    ///
    /// Readiness contract differs by direction. For `-L` it is strong: the local
    /// bind port is probed until it accepts. For `-R` there is no local port to
    /// probe, so readiness is "the child is still alive at the deadline" — a
    /// weaker signal that leans on `ExitOnForwardFailure=yes` (in the argv) to
    /// make ssh *exit* if the remote bind fails, which is caught here as
    /// `SshExited`. A remote forward that ssh reports as succeeded but that is
    /// unreachable at the app layer is not detected; verify with an app-level
    /// handshake if you need certainty.
    pub fn open(fwd: SshForward, ready_timeout: Duration) -> Result<Self, SshLaneError> {
        Self::open_with(fwd, ready_timeout, OsStr::new("ssh"))
    }

    /// As [`SshTunnel::open`], but with an explicit ssh program (lets a test
    /// inject a fake/absent binary).
    pub fn open_with(
        fwd: SshForward,
        ready_timeout: Duration,
        ssh_program: &OsStr,
    ) -> Result<Self, SshLaneError> {
        let argv = fwd.to_argv();
        let args: Vec<OsString> = argv[1..].iter().map(OsString::from).collect();
        let child = Command::new(ssh_program)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => SshLaneError::SshNotFound,
                _ => SshLaneError::SpawnFailed(e),
            })?;
        let mut child = SshChild { child };

        // For a -L forward the local bind port is probeable; for -R there is no
        // local port to probe (the edge listener is already bound), so we grace
        // the child and trust it.
        let probe: Option<SocketAddr> = match fwd.direction {
            Direction::Local => Some(SocketAddr::from(([127, 0, 0, 1], fwd.bind_port))),
            Direction::Remote => None,
        };
        let local_addr = match fwd.direction {
            Direction::Local => SocketAddr::from(([127, 0, 0, 1], fwd.bind_port)),
            Direction::Remote => SocketAddr::from(([127, 0, 0, 1], fwd.dest_port)),
        };

        let deadline = Instant::now() + ready_timeout;
        loop {
            match child.child.try_wait() {
                Ok(Some(status)) => return Err(SshLaneError::SshExited(status.code())),
                Ok(None) => {}
                Err(e) => return Err(SshLaneError::SpawnFailed(e)),
            }
            if let Some(addr) = probe {
                if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                    break;
                }
            }
            if Instant::now() >= deadline {
                // -L never opened the port -> not ready; -R has no probe, so a
                // child that is still alive at the deadline is taken as ready.
                if probe.is_some() {
                    return Err(SshLaneError::ForwardNotReady);
                }
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }

        Ok(SshTunnel { child, local_addr })
    }

    /// The loopback address the edge transport talks to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Liveness probe for a supervising strand/spider: `false` once ssh exits.
    pub fn is_up(&mut self) -> bool {
        matches!(self.child.child.try_wait(), Ok(None))
    }
}

/// One-call builders that compose an ssh tunnel with the edge transport.
pub struct SshLane;

impl SshLane {
    /// EGRESS: pre-pick a free local port, open a `-L` tunnel to it, then build
    /// an `EdgeSender` aimed at the forwarded `127.0.0.1:<port>`. Returns the
    /// strand to register plus the tunnel guard to keep alive.
    pub fn sender(
        edge: Edge,
        name: &'static str,
        fwd: SshForward,
        ready_timeout: Duration,
    ) -> Result<(EdgeSender, SshTunnel), SshLaneError> {
        // Pre-pick a free local port, then drop the listener so ssh can take it.
        let picked = {
            let l = TcpListener::bind("127.0.0.1:0").map_err(SshLaneError::Bind)?;
            l.local_addr().map_err(SshLaneError::Bind)?.port()
        };
        let mut fwd = fwd;
        fwd.direction = Direction::Local;
        fwd.bind_port = picked;
        let tunnel = SshTunnel::open(fwd, ready_timeout)?;
        let sender = edge.sender(name, format!("127.0.0.1:{picked}"));
        Ok((sender, tunnel))
    }

    /// INGRESS: bind the edge listener locally, then open a `-R` tunnel so the
    /// ssh server's `127.0.0.1:<remote_port>` reaches it. Returns the listener
    /// strand plus the tunnel guard.
    pub fn listener(
        edge: Edge,
        name: &'static str,
        fwd: SshForward,
        ready_timeout: Duration,
    ) -> Result<(EdgeListener, SshTunnel), SshLaneError> {
        let listener = edge
            .listener(name, "127.0.0.1:0")
            .map_err(SshLaneError::Edge)?;
        let edge_port = listener.local_addr().port();
        let mut fwd = fwd;
        fwd.direction = Direction::Remote;
        fwd.dest_port = edge_port;
        let tunnel = SshTunnel::open(fwd, ready_timeout)?;
        Ok((listener, tunnel))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_local_forward_argv_exactly() {
        let fwd = SshForward::local("jump.example.com", 49000, 7000)
            .user("bus")
            .ssh_port(2222)
            .allow_new_host_key(true);
        let argv = fwd.to_argv();
        assert_eq!(argv[0], "ssh");
        assert!(argv.iter().any(|a| a == "-N"));
        let i = argv.iter().position(|a| a == "-L").expect("has -L");
        assert_eq!(argv[i + 1], "127.0.0.1:49000:127.0.0.1:7000");
        assert!(argv
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "ExitOnForwardFailure=yes"));
        assert!(argv
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "ServerAliveInterval=15"));
        assert!(argv
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "StrictHostKeyChecking=accept-new"));
        assert!(argv.windows(2).any(|w| w[0] == "-p" && w[1] == "2222"));
        assert_eq!(argv.last().unwrap(), "bus@jump.example.com");
    }

    #[test]
    fn reverse_forward_uses_dash_r_not_dash_l() {
        let argv = SshForward::reverse("host", 8000, 7000).to_argv();
        assert!(argv.iter().any(|a| a == "-R") && !argv.iter().any(|a| a == "-L"));
    }

    /// A `--` must precede the host token so a dash-leading host/user can't be
    /// parsed as an ssh option (argument-injection guard).
    #[test]
    fn host_is_separated_by_double_dash() {
        let argv = SshForward::local("-oProxyCommand=evil", 1, 2).to_argv();
        let host_i = argv
            .iter()
            .position(|a| a == "-oProxyCommand=evil")
            .expect("host present");
        assert_eq!(argv[host_i - 1], "--", "host must be preceded by `--`");
        assert_eq!(argv.last().unwrap(), "-oProxyCommand=evil");
    }

    #[test]
    fn strict_host_key_default_omits_accept_new() {
        let argv = SshForward::local("h", 1, 2).to_argv(); // allow_new_host_key defaults to true (strict=false)
        assert!(argv
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "StrictHostKeyChecking=accept-new"));
        let strict = SshForward::local("h", 1, 2)
            .allow_new_host_key(false)
            .to_argv();
        assert!(!strict
            .iter()
            .any(|a| a == "StrictHostKeyChecking=accept-new"));
    }

    /// An absent ssh binary degrades to a typed error, never a panic.
    #[test]
    fn absent_ssh_degrades_to_typed_error_not_panic() {
        let fwd = SshForward::local("h", 49555, 7000);
        let err = SshTunnel::open_with(
            fwd,
            Duration::from_millis(200),
            OsStr::new("ssh-does-not-exist-xyz123"),
        )
        .err();
        assert!(
            matches!(err, Some(SshLaneError::SshNotFound)),
            "got {err:?}"
        );
    }
}
