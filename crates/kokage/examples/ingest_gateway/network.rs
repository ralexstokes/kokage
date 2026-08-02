use std::{io, net::SocketAddr, time::Duration};

use kokage::{
    ActorRef, DynamicScopeRef, ExitResult, OneShotActorSpec, RestartPolicy, SendErrorKind,
    Shutdown, TaskSpec,
    raw::{RawActor, RawContext},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};

use crate::model::{Evidence, TelemetryEvent};

const MAX_FRAME_BYTES: usize = 4 * 1024;

pub enum ConnectionMsg {}

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
}

impl RawActor for Connection {
    type Msg = ConnectionMsg;

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        loop {
            tokio::select! {
                command = ctx.recv() => match command {
                    None => return Ok(()),
                    Some(never) => match never {},
                },
                frame = read_frame(&mut self.stream) => {
                    let Some(frame) = frame? else {
                        self.evidence.clean_disconnect();
                        return Ok(());
                    };
                    let event = serde_json::from_slice(&frame).map_err(|error| {
                        self.evidence.malformed_client();
                        io::Error::new(io::ErrorKind::InvalidData, error)
                    })?;
                    self.evidence.valid_frame();
                    match self.intake.try_send(event) {
                        Ok(()) => self.evidence.frame_accepted(),
                        Err(error) if error.kind == SendErrorKind::Full => {
                            self.evidence.frame_shed_full();
                        }
                        Err(error) => {
                            self.evidence.degraded_connection();
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
                        connections.spawn_actor_once_spec(
                            OneShotActorSpec::new(format!("connection-{id}"), {
                                let intake = intake.clone();
                                let evidence = evidence.clone();
                                move || Connection::new(stream, intake, evidence)
                            })
                            .shutdown(Shutdown::graceful_for(Duration::from_millis(250))),
                        ).await?;
                        evidence.connection_accepted();
                    }
                }
            }
        }
    })
    .manual_readiness(Duration::from_secs(2))
    .restart(RestartPolicy::on_failure().limit(3, Duration::from_secs(1)))
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {length} exceeds {MAX_FRAME_BYTES}"),
        ));
    }
    let mut frame = vec![0; length];
    stream.read_exact(&mut frame).await?;
    Ok(Some(frame))
}

pub async fn write_event(stream: &mut TcpStream, event: &TelemetryEvent) -> io::Result<()> {
    let frame = serde_json::to_vec(event).map_err(io::Error::other)?;
    write_frame(stream, &frame).await
}

pub async fn write_malformed(stream: &mut TcpStream) -> io::Result<()> {
    write_frame(stream, br#"{"id": definitely-not-json}"#).await
}

async fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    let length = u32::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(frame).await?;
    stream.flush().await
}
