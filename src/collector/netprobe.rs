//! ICMP / TCP 探活。ICMP 走原始套接字，需要 `CAP_NET_RAW`；打不开时把错误写进结果，
//! 不把「没权限」记成成功。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

const ICMP_COUNT: u32 = 5;
const ICMP_GAP: Duration = Duration::from_millis(200);
const ICMP_TIMEOUT: Duration = Duration::from_millis(800);
const TCP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
pub struct IcmpResult {
    pub ok: bool,
    pub rtt_ms: f64,
    pub loss: f64,
    pub sent: u32,
    pub received: u32,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct TcpResult {
    pub port: u32,
    pub ok: bool,
    pub rtt_ms: f64,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct Destination {
    pub host: String,
    pub ipv4: bool,
}

pub fn destinations(
    ipv4: &str,
    ipv6: &str,
    hostname: &str,
    enable_ipv4: bool,
    enable_ipv6: bool,
) -> Vec<Destination> {
    let (enable_ipv4, enable_ipv6) = if !enable_ipv4 && !enable_ipv6 {
        (true, true)
    } else {
        (enable_ipv4, enable_ipv6)
    };
    let mut dests = Vec::new();
    if enable_ipv4 {
        let host = if ipv4.trim().is_empty() {
            hostname.trim()
        } else {
            ipv4.trim()
        };
        if !host.is_empty() {
            dests.push(Destination {
                host: host.to_string(),
                ipv4: true,
            });
        }
    }
    if enable_ipv6 {
        let host = if ipv6.trim().is_empty() {
            hostname.trim()
        } else {
            ipv6.trim()
        };
        if !host.is_empty() {
            dests.push(Destination {
                host: host.to_string(),
                ipv4: false,
            });
        }
    }
    dests
}

pub fn merge_icmp(results: &[IcmpResult]) -> IcmpResult {
    let Some(first) = results.first() else {
        return IcmpResult {
            ok: false,
            rtt_ms: 0.0,
            loss: 100.0,
            sent: 0,
            received: 0,
            error: "empty host".into(),
        };
    };
    let mut best = first.clone();
    for item in results.iter().skip(1) {
        if item.ok
            && (!best.ok
                || (item.rtt_ms > 0.0 && (best.rtt_ms == 0.0 || item.rtt_ms < best.rtt_ms)))
        {
            best = item.clone();
        }
    }
    best
}

pub fn merge_tcp_by_port(results: Vec<TcpResult>) -> Vec<TcpResult> {
    let mut merged: Vec<TcpResult> = Vec::new();
    for item in results {
        if let Some(prev) = merged.iter_mut().find(|prev| prev.port == item.port) {
            if item.ok
                && (!prev.ok
                    || (item.rtt_ms > 0.0 && (prev.rtt_ms == 0.0 || item.rtt_ms < prev.rtt_ms)))
            {
                *prev = item;
            }
        } else {
            merged.push(item);
        }
    }
    merged
}

pub fn display_error(icmp: Option<&IcmpResult>, tcp: &[TcpResult]) -> String {
    if icmp.is_some_and(|item| item.ok) || tcp.iter().any(|item| item.ok) {
        return String::new();
    }
    if let Some(error) = icmp
        .map(|item| item.error.trim())
        .filter(|error| !error.is_empty())
    {
        return error.to_string();
    }
    for item in tcp {
        if !item.error.trim().is_empty() {
            return format!("tcp:{} {}", item.port, item.error.trim());
        }
    }
    "unreachable".to_string()
}

pub async fn icmp_ping(host: &str, ipv4: bool) -> IcmpResult {
    let Some(ip) = resolve_host(host, ipv4).await else {
        return IcmpResult {
            ok: false,
            rtt_ms: 0.0,
            loss: 100.0,
            sent: 0,
            received: 0,
            error: format!("resolve {host}"),
        };
    };
    let ping = move || match ip {
        IpAddr::V4(ip) => icmp_v4(ip),
        IpAddr::V6(ip) => icmp_v6(ip),
    };
    match tokio::task::spawn_blocking(ping).await {
        Ok(result) => result,
        Err(err) => IcmpResult {
            ok: false,
            rtt_ms: 0.0,
            loss: 100.0,
            sent: 0,
            received: 0,
            error: err.to_string(),
        },
    }
}

pub async fn tcp_probe(host: &str, ipv4: bool, port: u32) -> TcpResult {
    if host.trim().is_empty() || port == 0 || port > 65535 {
        return TcpResult {
            port,
            ok: false,
            rtt_ms: 0.0,
            error: "invalid tcp target".into(),
        };
    }
    let Some(ip) = resolve_host(host, ipv4).await else {
        return TcpResult {
            port,
            ok: false,
            rtt_ms: 0.0,
            error: format!("resolve {host}"),
        };
    };
    let addr = SocketAddr::new(ip, port as u16);
    let started = Instant::now();
    match tokio::time::timeout(TCP_TIMEOUT, tokio::net::TcpStream::connect(addr)).await {
        Ok(Ok(_stream)) => TcpResult {
            port,
            ok: true,
            rtt_ms: started.elapsed().as_secs_f64() * 1000.0,
            error: String::new(),
        },
        Ok(Err(err)) => TcpResult {
            port,
            ok: false,
            rtt_ms: 0.0,
            error: err.to_string(),
        },
        Err(_) => TcpResult {
            port,
            ok: false,
            rtt_ms: 0.0,
            error: "tcp timeout".into(),
        },
    }
}

async fn resolve_host(host: &str, ipv4: bool) -> Option<IpAddr> {
    let host = host.trim();
    if let Ok(ip) = host.parse::<IpAddr>() {
        return match (ip, ipv4) {
            (IpAddr::V4(_), true) | (IpAddr::V6(_), false) => Some(ip),
            _ => None,
        };
    }
    let mut addrs = tokio::net::lookup_host((host, 0u16)).await.ok()?;
    addrs
        .find(|addr| addr.is_ipv4() == ipv4)
        .map(|addr| addr.ip())
}

fn icmp_v4(ip: Ipv4Addr) -> IcmpResult {
    let socket = match Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4)) {
        Ok(socket) => socket,
        Err(err) => return icmp_failure(err.to_string()),
    };
    if let Err(err) = socket.set_read_timeout(Some(ICMP_TIMEOUT)) {
        return icmp_failure(err.to_string());
    }
    let id = std::process::id() as u16;
    let dest = SockAddr::from(SocketAddr::from((ip, 0)));
    ping_loop(&socket, &dest, id, true)
}

fn icmp_v6(ip: Ipv6Addr) -> IcmpResult {
    let socket = match Socket::new(Domain::IPV6, Type::RAW, Some(Protocol::ICMPV6)) {
        Ok(socket) => socket,
        Err(err) => return icmp_failure(err.to_string()),
    };
    if let Err(err) = socket.set_read_timeout(Some(ICMP_TIMEOUT)) {
        return icmp_failure(err.to_string());
    }
    let id = std::process::id() as u16;
    let dest = SockAddr::from(SocketAddr::from((ip, 0)));
    ping_loop(&socket, &dest, id, false)
}

fn ping_loop(socket: &Socket, dest: &SockAddr, id: u16, ipv4: bool) -> IcmpResult {
    let mut received = 0u32;
    let mut rtt_sum = 0.0;
    for seq in 0..ICMP_COUNT {
        let packet = echo_request(id, seq as u16, !ipv4);
        let started = Instant::now();
        if socket.send_to(&packet, dest).is_err() {
            std::thread::sleep(ICMP_GAP);
            continue;
        }
        let mut buf = [std::mem::MaybeUninit::<u8>::uninit(); 1500];
        let deadline = Instant::now() + ICMP_TIMEOUT;
        while Instant::now() < deadline {
            match socket.recv_from(&mut buf) {
                Ok((len, _)) if echo_matches(init_prefix(&buf, len), id, seq as u16, ipv4) => {
                    received += 1;
                    rtt_sum += started.elapsed().as_secs_f64() * 1000.0;
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        if seq + 1 < ICMP_COUNT {
            std::thread::sleep(ICMP_GAP);
        }
    }
    let loss = if ICMP_COUNT == 0 {
        100.0
    } else {
        f64::from(ICMP_COUNT - received) / f64::from(ICMP_COUNT) * 100.0
    };
    IcmpResult {
        ok: received > 0,
        rtt_ms: if received == 0 {
            0.0
        } else {
            rtt_sum / f64::from(received)
        },
        loss,
        sent: ICMP_COUNT,
        received,
        error: if received == 0 {
            "icmp timeout".into()
        } else {
            String::new()
        },
    }
}

fn echo_request(id: u16, seq: u16, ipv6: bool) -> [u8; 16] {
    let mut packet = [0u8; 16];
    packet[0] = if ipv6 { 128 } else { 8 };
    packet[4] = (id >> 8) as u8;
    packet[5] = id as u8;
    packet[6] = (seq >> 8) as u8;
    packet[7] = seq as u8;
    let sum = icmp_checksum(&packet);
    packet[2] = (sum >> 8) as u8;
    packet[3] = sum as u8;
    packet
}

/// `recv_from` 写入的前 `len` 字节已初始化。
fn init_prefix(buf: &[std::mem::MaybeUninit<u8>], len: usize) -> &[u8] {
    let len = len.min(buf.len());
    // SAFETY: socket2::recv_from 保证前 len 个字节已由内核写入。
    unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), len) }
}

fn echo_matches(buf: &[u8], id: u16, seq: u16, ipv4: bool) -> bool {
    let icmp = if ipv4 {
        let ihl = usize::from(buf.first().copied().unwrap_or(0) & 0x0f) * 4;
        buf.get(ihl..).unwrap_or(&[])
    } else {
        buf
    };
    if icmp.len() < 8 {
        return false;
    }
    let reply = if ipv4 { 0 } else { 129 };
    icmp[0] == reply
        && u16::from_be_bytes([icmp[4], icmp[5]]) == id
        && u16::from_be_bytes([icmp[6], icmp[7]]) == seq
}

fn icmp_checksum(data: &[u8]) -> u16 {
    let mut sum = 0u32;
    let (chunks, remainder) = data.as_chunks::<2>();
    for chunk in chunks {
        sum += u16::from_be_bytes(*chunk) as u32;
    }
    if let Some(&byte) = remainder.first() {
        sum += u32::from(byte) << 8;
    }
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn icmp_failure(error: String) -> IcmpResult {
    IcmpResult {
        ok: false,
        rtt_ms: 0.0,
        loss: 100.0,
        sent: 0,
        received: 0,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_probe_reaches_local_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let result = tcp_probe("127.0.0.1", true, u32::from(port)).await;
        assert!(result.ok, "{}", result.error);
        assert!(result.rtt_ms >= 0.0);
    }

    #[test]
    fn display_error_prefers_success() {
        let icmp = IcmpResult {
            ok: false,
            rtt_ms: 0.0,
            loss: 100.0,
            sent: 1,
            received: 0,
            error: "icmp timeout".into(),
        };
        let tcp = vec![TcpResult {
            port: 443,
            ok: true,
            rtt_ms: 1.0,
            error: String::new(),
        }];
        assert!(display_error(Some(&icmp), &tcp).is_empty());
    }
}
