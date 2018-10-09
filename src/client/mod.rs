// copyright 2017 Kaz Wesley

//! Pool client implementation. Client API is synchronous because users will not normally want to
//! multiplex lots of upstreams.

mod connection;

use self::connection::PoolClientReader;
use crate::message::{ClientCommand, PoolEvent, PoolReply};

pub use self::connection::{PoolClientWriter, RequestId, Result};
pub use crate::message::{ErrorReply, Job, JobAssignment};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::*;

/// Trait for objects that can handle messages received from the pool.
pub trait MessageHandler {
    fn job_command(&mut self, job: Job);
    fn error_reply(&mut self, id: RequestId, error: ErrorReply);
    fn status_reply(&mut self, id: RequestId, status: String);
    fn job_reply(&mut self, id: RequestId, job: Box<JobAssignment>);
}

/// A synchronous stratum pool client, customized with a MessageHandler.
pub struct PoolClient<H> {
    writer: Arc<Mutex<PoolClientWriter>>,
    reader: PoolClientReader,
    handler: H,
}

impl<H: MessageHandler> PoolClient<H> {
    /// Synchronously connect to the server; pass the initial job to a MessageHandler constructor.
    pub fn connect<F>(
        address: &str,
        login: &str,
        pass: &str,
        keepalive: Option<Duration>,
        agent: &str,
        make_handler: F,
    ) -> Result<Self>
    where
        F: FnOnce(Job) -> H,
    {
        let (writer, work, reader) = connection::connect(address, login, pass, agent, keepalive)?;
        debug!("client connected, initial job: {:?}", &work);
        let writer = Arc::new(Mutex::new(writer));
        let handler = make_handler(work);
        Ok(PoolClient {
            writer,
            reader,
            handler,
        })
    }

    /// Return a new handle to the write end of the client connection.
    pub fn write_handle(&self) -> Arc<Mutex<PoolClientWriter>> {
        Arc::clone(&self.writer)
    }

    /// Borrow the message handler that was created in connect().
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Handle messages until the connection is closed.
    pub fn run(mut self) -> Result<()> {
        loop {
            let event = if let Some(event) = self.reader.read()? {
                event
            } else {
                debug!("read timeout; sending keepalive");
                self.writer.lock().unwrap().keepalive().unwrap();
                continue;
            };
            match event {
                PoolEvent::ClientCommand(ClientCommand::Job(j)) => self.handler.job_command(j),
                PoolEvent::PoolReply {
                    id,
                    error: Some(error),
                    ..
                } => self.handler.error_reply(id, error),
                PoolEvent::PoolReply {
                    id,
                    error: None,
                    result: Some(PoolReply::Status { status }),
                } => self.handler.status_reply(id, status),
                PoolEvent::PoolReply {
                    id,
                    error: None,
                    result: Some(PoolReply::Job(job)),
                } => self.handler.job_reply(id, job),
                PoolEvent::PoolReply {
                    error: None,
                    result: None,
                    ..
                } => warn!("pool reply with no content"),
            }
        }
    }
}
