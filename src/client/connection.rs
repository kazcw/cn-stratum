// copyright 2017 Kaz Wesley

//! session layer of a pool client

use crate::message::{
    ClientCommand, Credentials, ErrorReply, Job, JsonMessage, PoolCommand, PoolEvent,
    PoolReply, PoolRequest, Share, WorkerId,
};

use serde_json;

use std::convert::From;
use std::default::Default;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::time::Duration;

use failure::Fail;
use log::{debug, info, warn};
use serde_derive::{Deserialize, Serialize};

/// Result of client operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Id for matching our requests with server replies.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub struct RequestId(u32);

/// Write-end of a connection to a pool.
struct ClientWriter {
    stream: BufWriter<TcpStream>,
    next_id: RequestId,
}

impl ClientWriter {
    fn new(stream: BufWriter<TcpStream>) -> Self {
        ClientWriter {
            stream,
            next_id: RequestId(1),
        }
    }

    fn alloc_id(&mut self) -> RequestId {
        let id = self.next_id.0;
        self.next_id.0 = id.wrapping_add(1);
        RequestId(id)
    }

    fn send(&mut self, command: PoolCommand) -> Result<RequestId> {
        let id = self.alloc_id();
        serde_json::to_writer(&mut self.stream, &PoolRequest { id, command })?;
        writeln!(&mut self.stream)?;
        self.stream.flush()?;
        Ok(id)
    }
}

/// Read-end of a connection to a pool.
pub struct PoolClientReader {
    stream: BufReader<TcpStream>,
    buf: String,
}

impl PoolClientReader {
    fn new(stream: BufReader<TcpStream>) -> PoolClientReader {
        PoolClientReader {
            stream,
            buf: Default::default(),
        }
    }

    pub fn read(&mut self) -> Result<Option<PoolEvent<RequestId>>> {
        self.buf.clear();
        if let Err(e) = self.stream.read_line(&mut self.buf) {
            return match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => Ok(None),
                _ => Err(Error::from(e)),
            };
        };
        debug!("read() success: \"{}\"", &self.buf);
        if self.buf.is_empty() {
            return Err(Error::disconnected());
        }
        let msg: JsonMessage<_> = serde_json::from_str(&self.buf)?;
        Ok(msg.body)
    }
}

/// Write-end of a logged-in connection to a pool.
pub struct PoolClientWriter {
    writer: ClientWriter,
    worker_id: WorkerId,
}

impl PoolClientWriter {
    fn new(writer: ClientWriter, worker_id: WorkerId) -> Self {
        PoolClientWriter { writer, worker_id }
    }

    /// Send a keepalive message.
    pub fn keepalive(&mut self) -> Result<RequestId> {
        self.writer
            .send(PoolCommand::KeepAlived { id: self.worker_id })
    }

    /// Submit a share.
    pub fn submit(
        &mut self,
        job: &Job,
        nonce: u32,
        result: &[u8; 32],
    ) -> Result<RequestId> {
        self.writer.send(PoolCommand::Submit(Share {
            worker_id: self.worker_id,
            job_id: job.id(),
            nonce,
            result: *result,
            algo: job.algo().map(|a| a.to_owned()).unwrap_or_else(String::new),
        }))
        // 1 PoolReply::StatusReply expected
    }
}

/// synchronously login to server
pub fn connect(
    address: &str,
    login: &str,
    pass: &str,
    agent: &str,
    keepalive: Option<Duration>,
) -> Result<(PoolClientWriter, Job, PoolClientReader)> {
    let stream_r = TcpStream::connect(address)?;
    let stream_w = stream_r.try_clone()?;

    stream_w.set_nodelay(true)?;
    let stream_w = BufWriter::with_capacity(1500, stream_w);
    let mut writer = ClientWriter::new(stream_w);
    let algo = vec!["cn/1".to_owned()];
    let (login, pass, agent) = (login.to_owned(), pass.to_owned(), agent.to_owned());
    let req_id = writer.send(PoolCommand::Login(Credentials {
        login,
        pass,
        agent,
        algo,
    }))?;
    debug!("login sent: {:?}", req_id);

    stream_r.set_read_timeout(keepalive)?;
    let stream_r = BufReader::with_capacity(1500, stream_r);
    let mut reader = PoolClientReader::new(stream_r);
    let (wid, job, status) = loop {
        match reader.read()?.ok_or_else(Error::login_timed_out)? {
            PoolEvent::PoolReply {
                id,
                error: None,
                result: Some(PoolReply::Job(assignment)),
            } => {
                debug_assert_eq!(id, req_id);
                let worker_id = assignment.worker_id();
                let status = assignment.status().map(|x| x.to_owned());
                let job = assignment.into_job();
                break (worker_id, job, status);
            }
            PoolEvent::PoolReply { error: Some(e), .. } => return Err(Error(Error_::ErrorReply(e))),
            PoolEvent::ClientCommand(ClientCommand::Job(_)) => {
                warn!("ignoring job notification received during login");
                continue;
            }
            _ => return Err(Error::login_unexpected_reply()),
        };
    };
    info!("login successful: status \"{:?}\"", status);

    let writer = PoolClientWriter::new(writer, wid);
    Ok((writer, job, reader))
}

////////////////////
// errors
////////////////////

#[derive(Fail, Debug)]
#[fail(display = "{}", _0)]
pub struct Error(Error_);

#[derive(Fail, Debug)]
pub enum Error_ {
    #[fail(display = "{}", _0)]
    IoError(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    MessageError(#[cause] serde_json::Error),
    #[fail(display = "disconnected")]
    Disconnected,
    #[fail(display = "read timeout during login")]
    LoginTimedOut,
    #[fail(display = "unexpected reply during login")]
    LoginUnexpectedReply,
    #[fail(display = "server reports error: {}", _0)]
    ErrorReply(ErrorReply),
}

impl Error {
    fn disconnected() -> Self {
        Error(Error_::Disconnected)
    }
    fn login_timed_out() -> Self {
        Error(Error_::LoginTimedOut)
    }
    fn login_unexpected_reply() -> Self {
        Error(Error_::LoginUnexpectedReply)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error(Error_::IoError(error))
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error(Error_::MessageError(error))
    }
}

impl From<ErrorReply> for Error {
    fn from(error: ErrorReply) -> Self {
        Error(Error_::ErrorReply(error))
    }
}

////////////////////
// testing
////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    static EXAMPLE_LOGINREPLY_STR: &'static str = concat!(
        r#"{"id":0,"jsonrpc":"2.0","result":{"id":"0","job":"#,
        r#"{"blob":"0606de93b8d0055f149bdc720d9b8928e51399dbc2f85b069aa10142fff7b8814a296424f3659"#,
        r#"00000000019be9ee931ce265444a4d5b599d1e463f1f7fbada6517218fe65aea3a73390a406","#,
        r#""job_id":"12022","target":"b7d10000"},"status":"OK"},"error":null}"#
    );
    static EXAMPLE_JOBCOMMAND_STR: &'static str = concat!(
        r#"{"jsonrpc":"2.0","method":"job","params":"#,
        r#"{"blob":"06068795b8d0055b9272a308e09675e9c4c1510e84921e1ff0bfa13fc375eb8eec2207408205c"#,
        r#"000000000da5d4af05371b7bda75eef0d73cbbead3773006bd9117b1ca7dbcc9dacc1284d0d","#,
        r#""job_id":"12023","target":"b7d10000"}}"#
    );

    #[test]
    fn deserialize_login_reply() {
        let _: PoolEvent<u32> = serde_json::from_str(EXAMPLE_LOGINREPLY_STR).unwrap();
    }

    #[test]
    fn deserialize_job_command() {
        let _: PoolEvent<u32> = serde_json::from_str(EXAMPLE_JOBCOMMAND_STR).unwrap();
    }
}
