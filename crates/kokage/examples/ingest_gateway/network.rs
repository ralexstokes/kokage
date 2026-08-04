use std::{fmt, io, net::SocketAddr, sync::Arc, time::Duration};

use kokage::{
    ActorRef, DynamicScopeRef, ExitResult, OneShotTaskSpec, RestartPolicy, SendErrorKind, Shutdown,
    TaskContext, TaskSpec,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
};

use crate::model::{Evidence, IngressOutcome, MalformedKind, TelemetryEvent};

const MAX_FRAME_BYTES: usize = 4 * 1024;

#[derive(Debug)]
enum ReadEvent {
    CleanEof,
    Event(TelemetryEvent),
}

#[derive(Debug)]
enum ProtocolError {
    PartialHeader { received: usize },
    TruncatedBody { expected: usize, received: usize },
    OversizedLength { declared: usize, maximum: usize },
    InvalidJson(serde_json::Error),
}

impl ProtocolError {
    fn kind(&self) -> MalformedKind {
        match self {
            Self::PartialHeader { .. } => MalformedKind::PartialHeader,
            Self::TruncatedBody { .. } => MalformedKind::TruncatedBody,
            Self::OversizedLength { .. } => MalformedKind::OversizedLength,
            Self::InvalidJson(_) => MalformedKind::InvalidJson,
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialHeader { received } => {
                write!(
                    formatter,
                    "partial frame header: received {received} of 4 bytes"
                )
            }
            Self::TruncatedBody { expected, received } => write!(
                formatter,
                "truncated frame body: received {received} of {expected} bytes"
            ),
            Self::OversizedLength { declared, maximum } => write!(
                formatter,
                "frame length {declared} exceeds maximum {maximum}"
            ),
            Self::InvalidJson(error) => write!(formatter, "invalid JSON frame: {error}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidJson(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum ReadEventError {
    Protocol(ProtocolError),
    Transport(io::Error),
}

impl fmt::Display for ReadEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(formatter),
            Self::Transport(error) => write!(formatter, "connection transport error: {error}"),
        }
    }
}

impl std::error::Error for ReadEventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
    }
}

impl From<io::Error> for ReadEventError {
    fn from(error: io::Error) -> Self {
        Self::Transport(error)
    }
}

pub struct Connection {
    stream: TcpStream,
    intake: ActorRef<TelemetryEvent>,
    evidence: Evidence,
}

impl Connection {
    pub fn new(stream: TcpStream, intake: ActorRef<TelemetryEvent>, evidence: Evidence) -> Self {
        Self {
            stream,
            intake,
            evidence,
        }
    }

    async fn run(mut self, ctx: TaskContext) -> ExitResult {
        loop {
            tokio::select! {
                _ = ctx.shutdown_token().cancelled() => return Ok(()),
                read = read_event(&mut self.stream) => {
                    let event = match read {
                        Ok(ReadEvent::CleanEof) => {
                            self.evidence.clean_disconnect();
                            return Ok(());
                        }
                        Ok(ReadEvent::Event(event)) => event,
                        Err(ReadEventError::Protocol(error)) => {
                            self.evidence.malformed_client(error.kind());
                            return Err(Box::new(error));
                        }
                        Err(ReadEventError::Transport(error)) => return Err(error.into()),
                    };
                    match self.intake.try_send(event) {
                        Ok(()) => self.evidence.valid_frame(IngressOutcome::Accepted),
                        Err(error) if error.kind == SendErrorKind::Full => {
                            self.evidence.valid_frame(IngressOutcome::ShedFull);
                        }
                        Err(error) => {
                            self.evidence.valid_frame(IngressOutcome::Degraded);
                            return Err(error.into_boxed());
                        }
                    }
                }
            }
        }
    }
}

pub fn listener(
    connections: DynamicScopeRef,
    intake: ActorRef<TelemetryEvent>,
    evidence: Evidence,
    address: watch::Sender<Option<SocketAddr>>,
) -> TaskSpec {
    let next_connection = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    TaskSpec::new("listener", move |ctx| {
        let connections = connections.clone();
        let intake = intake.clone();
        let evidence = evidence.clone();
        let address = address.clone();
        let next_connection = next_connection.clone();
        async move {
            address.send_replace(None);
            let result: ExitResult = async {
                let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
                address.send_replace(Some(listener.local_addr()?));
                ctx.mark_ready();

                loop {
                    tokio::select! {
                        _ = ctx.shutdown_token().cancelled() => return Ok(()),
                        accepted = listener.accept() => {
                            let (stream, _) = accepted?;
                            stream.set_nodelay(true)?;
                            let id = next_connection.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let start = Arc::new(Notify::new());
                            connections.spawn_once_spec(
                                OneShotTaskSpec::new(format!("connection-{id}"), {
                                    let intake = intake.clone();
                                    let evidence = evidence.clone();
                                    let start = start.clone();
                                    move |ctx| async move {
                                        start.notified().await;
                                        Connection::new(stream, intake, evidence).run(ctx).await
                                    }
                                })
                                .shutdown(Shutdown::graceful_for(Duration::from_millis(250))),
                            ).await?;

                            // Count only successful membership insertion, before the
                            // connection can publish any outcome of its own.
                            evidence.connection_accepted();
                            start.notify_one();
                        }
                    }
                }
            }
            .await;
            address.send_replace(None);
            result
        }
    })
    .manual_readiness(Duration::from_secs(2))
    .restart(RestartPolicy::on_failure().limit(3, Duration::from_secs(1)))
}

async fn read_event(stream: &mut TcpStream) -> Result<ReadEvent, ReadEventError> {
    let mut header = [0_u8; 4];
    let header_bytes = read_up_to(stream, &mut header).await?;
    if header_bytes == 0 {
        return Ok(ReadEvent::CleanEof);
    }
    if header_bytes < header.len() {
        return Err(ReadEventError::Protocol(ProtocolError::PartialHeader {
            received: header_bytes,
        }));
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(ReadEventError::Protocol(ProtocolError::OversizedLength {
            declared: length,
            maximum: MAX_FRAME_BYTES,
        }));
    }
    let mut frame = vec![0; length];
    let body_bytes = read_up_to(stream, &mut frame).await?;
    if body_bytes < frame.len() {
        return Err(ReadEventError::Protocol(ProtocolError::TruncatedBody {
            expected: frame.len(),
            received: body_bytes,
        }));
    }
    let event = serde_json::from_slice(&frame)
        .map_err(|error| ReadEventError::Protocol(ProtocolError::InvalidJson(error)))?;
    Ok(ReadEvent::Event(event))
}

async fn read_up_to(stream: &mut TcpStream, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = stream.read(&mut buffer[filled..]).await?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

pub async fn write_event(stream: &mut TcpStream, event: &TelemetryEvent) -> io::Result<()> {
    let frame = serde_json::to_vec(event).map_err(io::Error::other)?;
    write_frame(stream, &frame).await
}

pub async fn write_malformed(stream: &mut TcpStream, kind: MalformedKind) -> io::Result<()> {
    match kind {
        MalformedKind::PartialHeader => stream.write_all(&[0, 0]).await?,
        MalformedKind::TruncatedBody => {
            stream.write_all(&8_u32.to_be_bytes()).await?;
            stream.write_all(b"bad").await?;
        }
        MalformedKind::OversizedLength => {
            let oversized = u32::try_from(MAX_FRAME_BYTES + 1)
                .expect("maximum scripted frame size fits in u32");
            stream.write_all(&oversized.to_be_bytes()).await?;
        }
        MalformedKind::InvalidJson => {
            return write_frame(stream, br#"{"id": definitely-not-json}"#).await;
        }
    }
    stream.flush().await
}

async fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    let length = u32::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(frame).await?;
    stream.flush().await
}
