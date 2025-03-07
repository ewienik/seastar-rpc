/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

#![allow(dead_code)]

use {
    anyhow::{anyhow, bail},
    bytes::{Buf, BufMut, Bytes, BytesMut},
    std::{collections::VecDeque, mem, time::Duration},
};

#[derive(Copy, Clone, Debug, PartialEq, derive_more::From, derive_more::AsRef)]
pub struct ConnectionId(u64);

#[derive(Copy, Clone, Debug)]
pub struct VerbType(u64);

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct MessageId(i64);

pub struct Data(Bytes);

#[derive(Clone, Debug, PartialEq)]
pub struct Output(Bytes);

impl Output {
    pub fn data(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub struct ClientRpc {
    state: StateClientRpc,
    outputs: VecDeque<Output>,
    responses: VecDeque<Response>,
}

enum StateClientRpc {
    Negotiation {
        buf: BytesMut,
    },
    Compressed {
        id: ConnectionId,
    },
    Uncompressed {
        id: ConnectionId,
        msg_id: MessageId,
        buf: BytesMut,
    },
    End,
}

impl ClientRpc {
    pub fn new() -> Self {
        let mut buf = BytesMut::new();
        buf.put_negotiation(|_| {});
        Self {
            state: StateClientRpc::Negotiation {
                buf: BytesMut::new(),
            },
            outputs: VecDeque::from_iter([Output(buf.freeze())]),
            responses: VecDeque::new(),
        }
    }

    pub fn handle_input(&mut self, input: &[u8]) -> anyhow::Result<()> {
        let next = match self.state {
            StateClientRpc::Negotiation { ref mut buf } => {
                buf.put_slice(input);
                dbg!(Self::handle_input_negotiation(buf)?).map(|id| {
                    buf.clear();
                    StateClientRpc::Uncompressed {
                        id,
                        msg_id: MessageId(0),
                        buf: mem::replace(buf, BytesMut::new()),
                    }
                })
            }
            StateClientRpc::Uncompressed { ref mut buf, .. } => {
                buf.put_slice(input);
                Self::handle_input_uncompressed(buf)?
                    .into_iter()
                    .for_each(|response| self.responses.push_back(response));
                None
            }
            StateClientRpc::Compressed { .. } => todo!(),
            StateClientRpc::End => bail!("connection finished"),
        };
        if let Some(next) = next {
            self.state = next;
        }
        Ok(())
    }

    fn handle_input_negotiation(buf: &mut BytesMut) -> anyhow::Result<Option<ConnectionId>> {
        let Some(frame_len) = dbg!(Negotiation::frame_len(buf)?) else {
            return Ok(None);
        };
        let mut buf = Negotiation::parse(buf.split_to(frame_len).freeze())?.data();
        let mut connection_id = None;
        while !buf.is_empty() {
            let Some(frame_len) = dbg!(NegotiationFeature::frame_len(&buf)?) else {
                bail!("Unable to parse negotiation features");
            };
            match dbg!(NegotiationFeature::parse(buf.split_to(frame_len))?) {
                NegotiationFeature::ConnectionId(feature) => {
                    connection_id.replace(feature.connection_id());
                }
                _ => {}
            }
        }
        if connection_id.is_none() {
            bail!("Missing a connection id negotiation feature");
        }
        Ok(connection_id)
    }

    fn handle_input_uncompressed(buf: &mut BytesMut) -> anyhow::Result<Vec<Response>> {
        let mut responses = Vec::new();
        while let Some(frame_len) = Response::frame_len(buf)? {
            responses.push(Response::parse(buf.split_to(frame_len).freeze())?);
        }
        Ok(responses)
    }

    pub fn handle_request(
        &mut self,
        verb_type: VerbType,
        put_data: impl FnOnce(&mut BytesMut),
    ) -> anyhow::Result<MessageId> {
        match self.state {
            StateClientRpc::Negotiation { .. } => Err(anyhow!("connection not negotiated")),
            StateClientRpc::Uncompressed { ref mut msg_id, .. } => {
                let mut buf = BytesMut::new();
                buf.put_request(verb_type, *msg_id, put_data);
                msg_id.0 += 1;
                self.outputs.push_back(Output(buf.freeze()));
                Ok(MessageId(msg_id.0 - 1))
            }
            StateClientRpc::Compressed { .. } => todo!(),
            StateClientRpc::End => Err(anyhow!("connection finished")),
        }
    }

    pub fn poll_output(&mut self) -> Option<Output> {
        self.outputs.pop_front()
    }

    pub fn poll_response(&mut self) -> Option<Response> {
        self.responses.pop_front()
    }

    pub fn connection_id(&mut self) -> anyhow::Result<ConnectionId> {
        match self.state {
            StateClientRpc::Negotiation { .. } => Err(anyhow!("connection not negotiated")),
            StateClientRpc::Uncompressed { id, .. } => Ok(id),
            StateClientRpc::Compressed { id, .. } => Ok(id),
            StateClientRpc::End => Err(anyhow!("connection finished")),
        }
    }
}

pub struct ClientStream {
    state: StateClientStream,
    outputs: VecDeque<Output>,
}

enum StateClientStream {
    Negotiation { parent_id: ConnectionId },
    Compressed { id: ConnectionId },
    Uncompressed { id: ConnectionId },
    End,
}

impl ClientStream {
    pub fn new(parent_id: ConnectionId) -> Self {
        Self {
            state: StateClientStream::Negotiation { parent_id },
            outputs: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
pub struct Server {
    state: StateServer,
    outputs: VecDeque<Output>,
}

#[derive(Debug, PartialEq)]
pub enum ConnectionType {
    Rpc,
    Stream,
}

#[derive(Debug)]
enum StateServer {
    Negotiation {
        id: ConnectionId,
        buf: BytesMut,
    },
    Rpc {
        state: StateServerRpc,
    },
    Stream {
        state: StateServerStream,
        parent_id: ConnectionId,
    },
}

impl Server {
    pub fn new(id: ConnectionId) -> Self {
        Self {
            state: StateServer::Negotiation {
                id,
                buf: BytesMut::new(),
            },
            outputs: VecDeque::new(),
        }
    }

    pub fn handle_input(&mut self, input: &[u8]) -> anyhow::Result<()> {
        let next = match self.state {
            StateServer::Negotiation { ref mut buf, id } => {
                buf.put_slice(input);
                Self::handle_input_negotiation(buf)?.map(|parent_id| {
                    buf.clear();
                    buf.put_negotiation(|buf| buf.put_negotiation_connection_id(id));
                    self.outputs.push_back(Output(buf.split().freeze()));
                    let buf = buf.split_off(0);
                    if let Some(parent_id) = parent_id {
                        StateServer::Stream {
                            state: StateServerStream::Uncompressed { buf },
                            parent_id,
                        }
                    } else {
                        StateServer::Rpc {
                            state: StateServerRpc::Uncompressed { buf },
                        }
                    }
                })
            }
            _ => bail!("connection negotiated"),
        };
        if let Some(next) = next {
            self.state = next;
        }
        Ok(())
    }

    fn handle_input_negotiation(
        buf: &mut BytesMut,
    ) -> anyhow::Result<Option<Option<ConnectionId>>> {
        let Some(frame_len) = Negotiation::frame_len(buf)? else {
            return Ok(None);
        };
        let mut buf = Negotiation::parse(buf.split_to(frame_len).freeze())?.data();
        let mut parent_id = None;
        while !buf.is_empty() {
            let Some(frame_len) = NegotiationFeature::frame_len(&buf)? else {
                bail!("Unable to parse negotiation features");
            };
            match NegotiationFeature::parse(buf.split_to(frame_len))? {
                NegotiationFeature::StreamParent(feature) => {
                    parent_id.replace(feature.connection_id());
                }
                _ => {}
            }
        }
        Ok(Some(parent_id))
    }

    pub fn poll_output(&mut self) -> Option<Output> {
        self.outputs.pop_front()
    }

    pub fn connection_type(&self) -> Option<ConnectionType> {
        if !self.outputs.is_empty() {
            return None;
        }
        match self.state {
            StateServer::Rpc { .. } => Some(ConnectionType::Rpc),
            StateServer::Stream { .. } => Some(ConnectionType::Stream),
            _ => None,
        }
    }

    pub fn into_rpc(self) -> Result<ServerRpc, Self> {
        if !self.outputs.is_empty() {
            return Err(self);
        }
        match self.state {
            StateServer::Rpc { state } => Ok(ServerRpc {
                state,
                outputs: self.outputs,
                requests: VecDeque::new(),
            }),
            _ => Err(self),
        }
    }

    pub fn into_stream(self) -> Result<(ServerStream, ConnectionId), Self> {
        if !self.outputs.is_empty() {
            return Err(self);
        }
        match self.state {
            StateServer::Stream { state, parent_id } => Ok((
                ServerStream {
                    state,
                    outputs: self.outputs,
                },
                parent_id,
            )),
            _ => Err(self),
        }
    }
}

#[derive(Debug)]
pub struct ServerRpc {
    state: StateServerRpc,
    outputs: VecDeque<Output>,
    requests: VecDeque<Request>,
}

#[derive(Debug)]
enum StateServerRpc {
    Compressed { buf: BytesMut },
    Uncompressed { buf: BytesMut },
    End,
}

impl ServerRpc {
    pub fn handle_input(&mut self, input: &[u8]) -> anyhow::Result<()> {
        let next = match self.state {
            StateServerRpc::Uncompressed { ref mut buf, .. } => {
                buf.put_slice(input);
                Self::handle_input_uncompressed(buf)?
                    .into_iter()
                    .for_each(|request| self.requests.push_back(request));
                None
            }
            StateServerRpc::Compressed { .. } => todo!(),
            StateServerRpc::End => bail!("connection finished"),
        };
        if let Some(next) = next {
            self.state = next;
        }
        Ok(())
    }

    fn handle_input_uncompressed(buf: &mut BytesMut) -> anyhow::Result<Vec<Request>> {
        let mut requests = Vec::new();
        while let Some(frame_len) = Request::frame_len(buf)? {
            requests.push(Request::parse(buf.split_to(frame_len).freeze())?);
        }
        Ok(requests)
    }

    pub fn handle_response(
        &mut self,
        msg_id: MessageId,
        put_data: impl FnOnce(&mut BytesMut),
    ) -> anyhow::Result<()> {
        match self.state {
            StateServerRpc::Uncompressed { .. } => {
                let mut buf = BytesMut::new();
                buf.put_response(msg_id, put_data);
                self.outputs.push_back(Output(buf.freeze()));
                Ok(())
            }
            StateServerRpc::Compressed { .. } => todo!(),
            StateServerRpc::End => Err(anyhow!("connection finished")),
        }
    }

    pub fn poll_output(&mut self) -> Option<Output> {
        self.outputs.pop_front()
    }

    pub fn poll_request(&mut self) -> Option<Request> {
        self.requests.pop_front()
    }
}

#[derive(Debug)]
pub struct ServerStream {
    state: StateServerStream,
    outputs: VecDeque<Output>,
}

#[derive(Debug)]
enum StateServerStream {
    Compressed { buf: BytesMut },
    Uncompressed { buf: BytesMut },
    End,
}

#[derive(Debug)]
pub struct Request {
    buf: Bytes,
}

impl Request {
    const OFFSET_VERB_TYPE: usize = 0;
    const OFFSET_MSG_ID: usize = Self::OFFSET_VERB_TYPE + mem::size_of::<u64>();
    const OFFSET_LEN: usize = Self::OFFSET_MSG_ID + mem::size_of::<i64>();
    const OFFSET_DATA: usize = Self::OFFSET_LEN + mem::size_of::<u32>();
    const HEADER_SIZE: usize = Self::OFFSET_DATA;
    const MAX_FRAME_LEN: usize = 10240;

    fn frame_len(buf: &[u8]) -> anyhow::Result<Option<usize>> {
        let buf_len = buf.remaining();
        if buf_len < Self::HEADER_SIZE {
            return Ok(None);
        }
        let frame_len = (&buf[Self::OFFSET_LEN..]).get_u32_le() as usize + Self::HEADER_SIZE;
        if frame_len > Self::MAX_FRAME_LEN {
            bail!("Too big expected response frame len: {frame_len}");
        }
        Ok((buf_len >= frame_len).then_some(frame_len))
    }

    fn parse(mut buf: Bytes) -> anyhow::Result<Self> {
        let Some(frame_len) = Self::frame_len(&buf)? else {
            bail!("not a full request frame");
        };
        buf.truncate(frame_len);
        Ok(Self { buf })
    }

    pub fn verb_type(&self) -> VerbType {
        VerbType((&self.buf.as_ref()[Self::OFFSET_VERB_TYPE..]).get_u64_le())
    }

    pub fn msg_id(&self) -> MessageId {
        MessageId((&self.buf.as_ref()[Self::OFFSET_MSG_ID..]).get_i64_le())
    }

    pub fn data(&self) -> Bytes {
        self.buf.slice(Self::OFFSET_DATA..)
    }
}

pub struct Response {
    buf: Bytes,
}

impl Response {
    const OFFSET_MSG_ID: usize = 0;
    const OFFSET_LEN: usize = Self::OFFSET_MSG_ID + mem::size_of::<i64>();
    const OFFSET_DATA: usize = Self::OFFSET_LEN + mem::size_of::<u32>();
    const HEADER_SIZE: usize = Self::OFFSET_DATA;
    const MAX_FRAME_LEN: usize = 10240;

    fn frame_len(buf: &[u8]) -> anyhow::Result<Option<usize>> {
        let buf_len = buf.remaining();
        if buf_len < Self::HEADER_SIZE {
            return Ok(None);
        }
        let frame_len = (&buf[Self::OFFSET_LEN..]).get_u32_le() as usize + Self::HEADER_SIZE;
        if frame_len > Self::MAX_FRAME_LEN {
            bail!("Too big expected response frame len: {frame_len}");
        }
        Ok((buf_len >= frame_len).then_some(frame_len))
    }

    fn parse(mut buf: Bytes) -> anyhow::Result<Self> {
        let Some(frame_len) = Self::frame_len(&buf)? else {
            bail!("not a full response frame");
        };
        buf.truncate(frame_len);
        Ok(Self { buf })
    }

    pub fn msg_id(&self) -> MessageId {
        MessageId((&self.buf.as_ref()[Self::OFFSET_MSG_ID..]).get_i64_le())
    }

    pub fn data(&self) -> Bytes {
        self.buf.slice(Self::OFFSET_DATA..)
    }
}

pub struct ResponseWithHandlerDuration {
    buf: Bytes,
}

impl ResponseWithHandlerDuration {
    const OFFSET_MSG_ID: usize = 0;
    const OFFSET_LEN: usize = Self::OFFSET_MSG_ID + mem::size_of::<i64>();
    const OFFSET_HANDLER_DURATION: usize = Self::OFFSET_LEN + mem::size_of::<u32>();
    const OFFSET_DATA: usize = Self::OFFSET_HANDLER_DURATION + mem::size_of::<u32>();
    const HEADER_SIZE: usize = Self::OFFSET_DATA;
    const MAX_FRAME_LEN: usize = 10240;
    const WRONG_DURATION: u32 = 0xffffffff;

    fn frame_len(buf: &[u8]) -> anyhow::Result<Option<usize>> {
        let buf_len = buf.remaining();
        if buf_len < Self::HEADER_SIZE {
            return Ok(None);
        }
        let frame_len = (&buf[Self::OFFSET_LEN..]).get_u32_le() as usize + Self::HEADER_SIZE;
        if frame_len > Self::MAX_FRAME_LEN {
            bail!("Too big expected response with handler duration frame len: {frame_len}");
        }
        Ok((buf_len >= frame_len).then_some(frame_len))
    }

    fn parse(mut buf: Bytes) -> anyhow::Result<Self> {
        let Some(frame_len) = Self::frame_len(&buf)? else {
            bail!("not a full response with handler duration frame");
        };
        buf.truncate(frame_len);
        Ok(Self { buf })
    }

    fn msg_id(&self) -> MessageId {
        MessageId((&self.buf.as_ref()[Self::OFFSET_MSG_ID..]).get_i64_le())
    }

    fn handler_duration(&self) -> Option<Duration> {
        let duration_ms = (&self.buf.as_ref()[Self::OFFSET_HANDLER_DURATION..]).get_u32_le();
        (duration_ms != Self::WRONG_DURATION).then_some(Duration::from_micros(duration_ms as u64))
    }

    fn data(&self) -> Bytes {
        self.buf.slice(Self::OFFSET_DATA..)
    }
}

pub enum Exception {
    User {
        msg_id: MessageId,
        data: Data,
    },
    UnknownVerb {
        msg_id: MessageId,
        verb_id: VerbType,
    },
}

pub struct Stream {
    data: Data,
}

const NEGOTIATION_COMPRESSION: u32 = 0;
const NEGOTIATION_TIMEOUT_PROPAGATION: u32 = 1;
const NEGOTIATION_CONNECTION_ID: u32 = 2;
const NEGOTIATION_STREAM_PARENT: u32 = 3;
const NEGOTIATION_ISOLATION: u32 = 4;
const NEGOTIATION_HANDLER_DURATION: u32 = 5;

struct Negotiation {
    buf: Bytes,
}

impl Negotiation {
    const MAGIC: &'static [u8] = b"SSTARRPC";
    const MAGIC_SIZE: usize = Self::MAGIC.len();
    const OFFSET_LEN: usize = Self::MAGIC_SIZE;
    const OFFSET_DATA: usize = Self::OFFSET_LEN + mem::size_of::<u32>();
    const HEADER_SIZE: usize = Self::OFFSET_DATA;
    const MAX_FRAME_LEN: usize = 10240;

    fn frame_len(buf: &[u8]) -> anyhow::Result<Option<usize>> {
        if buf.len() < Self::HEADER_SIZE {
            return Ok(None);
        }
        let frame_len = (&buf[Self::OFFSET_LEN..]).get_u32_le() as usize + Self::HEADER_SIZE;
        if frame_len > Self::MAX_FRAME_LEN {
            bail!("Too big negotiation frame len: {frame_len}");
        }
        Ok((buf.len() >= frame_len).then_some(frame_len))
    }

    fn parse(mut buf: Bytes) -> anyhow::Result<Self> {
        let Some(frame_len) = Self::frame_len(&buf)? else {
            bail!("not a full negotiation frame");
        };
        buf.truncate(frame_len);
        if &buf[..Self::MAGIC_SIZE] != Self::MAGIC {
            bail!("missing a magic in a negotiation frame");
        }
        Ok(Self { buf })
    }

    fn data(&self) -> Bytes {
        self.buf.slice(Self::OFFSET_DATA..)
    }
}

#[derive(Debug)]
enum NegotiationFeature {
    Compression(NegotiationCompression),
    TimeoutPropagation(NegotiationTimeoutPropagation),
    ConnectionId(NegotiationConnectionId),
    StreamParent(NegotiationStreamParent),
    Isolation(NegotiationIsolation),
    HandlerDuration(NegotiationHandlerDuration),
}

impl NegotiationFeature {
    const OFFSET_FEATURE_NUMBER: usize = 0;
    const OFFSET_LEN: usize = Self::OFFSET_FEATURE_NUMBER + mem::size_of::<u32>();
    const OFFSET_DATA: usize = Self::OFFSET_LEN + mem::size_of::<u32>();
    const HEADER_SIZE: usize = Self::OFFSET_DATA;
    const MAX_FRAME_LEN: usize = 10240;

    fn frame_len(buf: &[u8]) -> anyhow::Result<Option<usize>> {
        if buf.len() < Self::HEADER_SIZE {
            return Ok(None);
        }
        let frame_len = (&buf[Self::OFFSET_LEN..]).get_u32_le() as usize + Self::HEADER_SIZE;
        if frame_len > Self::MAX_FRAME_LEN {
            bail!("Too big negotiation feature frame len: {frame_len}");
        }
        Ok((buf.len() >= frame_len).then_some(frame_len))
    }

    fn parse(mut buf: Bytes) -> anyhow::Result<Self> {
        let Some(frame_len) = Self::frame_len(&buf)? else {
            bail!("not a full negotiation frame");
        };
        buf.truncate(frame_len);
        let feature_number = (&buf[Self::OFFSET_FEATURE_NUMBER..]).get_u32_le();
        match feature_number {
            NEGOTIATION_COMPRESSION => Ok(Self::Compression(NegotiationCompression { buf })),
            NEGOTIATION_TIMEOUT_PROPAGATION => {
                Ok(Self::TimeoutPropagation(NegotiationTimeoutPropagation {
                    buf,
                }))
            }
            NEGOTIATION_CONNECTION_ID => Ok(Self::ConnectionId(NegotiationConnectionId { buf })),
            NEGOTIATION_STREAM_PARENT => Ok(Self::StreamParent(NegotiationStreamParent { buf })),
            NEGOTIATION_ISOLATION => Ok(Self::Isolation(NegotiationIsolation { buf })),
            NEGOTIATION_HANDLER_DURATION => {
                Ok(Self::HandlerDuration(NegotiationHandlerDuration { buf }))
            }
            _ => Err(anyhow!("Unknown feature number: {feature_number}")),
        }
    }
}

#[derive(Debug)]
struct NegotiationCompression {
    buf: Bytes,
}

#[derive(Debug)]
struct NegotiationTimeoutPropagation {
    buf: Bytes,
}

#[derive(Debug)]
struct NegotiationConnectionId {
    buf: Bytes,
}

impl NegotiationConnectionId {
    const OFFSET_CONNECTION_ID: usize = NegotiationFeature::OFFSET_DATA;

    fn connection_id(&self) -> ConnectionId {
        ConnectionId((&self.buf.as_ref()[Self::OFFSET_CONNECTION_ID..]).get_u64_le())
    }
}

#[derive(Debug)]
struct NegotiationStreamParent {
    buf: Bytes,
}

impl NegotiationStreamParent {
    fn connection_id(&self) -> ConnectionId {
        ConnectionId(self.buf.as_ref().get_u64_le())
    }
}

#[derive(Debug)]
struct NegotiationIsolation {
    buf: Bytes,
}

#[derive(Debug)]
struct NegotiationHandlerDuration {
    buf: Bytes,
}

trait BytesMutExt {
    fn put_len_value(&mut self, put_value: impl FnOnce(&mut Self));
    fn put_negotiation(&mut self, put_data: impl FnOnce(&mut Self));
    fn put_negotiation_feature(&mut self, feature_number: u32, put_data: impl FnOnce(&mut Self));
    fn put_negotiation_compression(&mut self, data: &[u8]);
    fn put_negotiation_timeout_propagation(&mut self);
    fn put_negotiation_connection_id(&mut self, connection_id: ConnectionId);
    fn put_negotiation_stream_parent(&mut self, connection_id: ConnectionId);
    fn put_negotiation_isolation(&mut self, cookie: &[u8]);
    fn put_negotiation_handler_duration(&mut self);
    fn put_request(
        &mut self,
        verb_type: VerbType,
        msg_id: MessageId,
        put_data: impl FnOnce(&mut Self),
    );
    fn put_response(&mut self, msg_id: MessageId, put_data: impl FnOnce(&mut Self));
}

impl BytesMutExt for BytesMut {
    fn put_len_value(&mut self, put_value: impl FnOnce(&mut Self)) {
        let offset_len = self.len();
        self.put_u32_le(0);
        let offset_data = self.len();
        put_value(self);
        let data_len = self.len() - offset_data;
        (&mut self.as_mut()[offset_len..offset_data]).put_u32_le(data_len as u32);
    }

    fn put_negotiation(&mut self, put_data: impl FnOnce(&mut Self)) {
        self.put_slice(b"SSTARRPC");
        self.put_len_value(put_data);
    }

    fn put_negotiation_feature(&mut self, feature_number: u32, put_data: impl FnOnce(&mut Self)) {
        self.put_u32_le(feature_number);
        self.put_len_value(put_data);
    }

    fn put_negotiation_compression(&mut self, data: &[u8]) {
        self.put_negotiation_feature(NEGOTIATION_COMPRESSION, |buf| {
            buf.put_slice(data);
        });
    }

    fn put_negotiation_timeout_propagation(&mut self) {
        self.put_negotiation_feature(NEGOTIATION_TIMEOUT_PROPAGATION, |_| {});
    }

    fn put_negotiation_connection_id(&mut self, connection_id: ConnectionId) {
        self.put_negotiation_feature(NEGOTIATION_CONNECTION_ID, |buf| {
            buf.put_u64_le(connection_id.0);
        });
    }

    fn put_negotiation_stream_parent(&mut self, connection_id: ConnectionId) {
        self.put_negotiation_feature(NEGOTIATION_STREAM_PARENT, |buf| {
            buf.put_u64_le(connection_id.0);
        });
    }

    fn put_negotiation_isolation(&mut self, cookie: &[u8]) {
        self.put_negotiation_feature(NEGOTIATION_ISOLATION, |buf| {
            buf.put_slice(cookie);
        });
    }

    fn put_negotiation_handler_duration(&mut self) {
        self.put_negotiation_feature(NEGOTIATION_HANDLER_DURATION, |_| {});
    }

    fn put_request(
        &mut self,
        verb_type: VerbType,
        msg_id: MessageId,
        put_data: impl FnOnce(&mut Self),
    ) {
        self.put_u64_le(verb_type.0);
        self.put_i64_le(msg_id.0);
        self.put_len_value(put_data);
    }

    fn put_response(&mut self, msg_id: MessageId, put_data: impl FnOnce(&mut Self)) {
        self.put_i64_le(msg_id.0);
        self.put_len_value(put_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_server_rpc() {
        let mut server = Server::new(ConnectionId(10));
        assert_eq!(server.connection_type(), None);
        let mut client = ClientRpc::new();
        while let Some(output) = client.poll_output() {
            server.handle_input(&output.0).unwrap();
        }
        while let Some(output) = server.poll_output() {
            client.handle_input(&output.0).unwrap();
        }
        assert_eq!(client.connection_id().unwrap(), ConnectionId(10));
        assert_eq!(server.connection_type(), Some(ConnectionType::Rpc));
        let mut server = server.into_rpc().unwrap();
        client
            .handle_request(VerbType(20), |buf| buf.put_slice(b"Request1"))
            .unwrap();
        client
            .handle_request(VerbType(20), |buf| buf.put_slice(b"Request2"))
            .unwrap();
        while let Some(output) = client.poll_output() {
            server.handle_input(&output.0).unwrap();
        }
        let request1 = server.poll_request().unwrap();
        assert_eq!(request1.msg_id().0, 0);
        assert_eq!(request1.data().as_ref(), b"Request1");
        let request2 = server.poll_request().unwrap();
        assert_eq!(request2.msg_id().0, 1);
        assert_eq!(request2.data().as_ref(), b"Request2");
        server
            .handle_response(request2.msg_id(), |buf| buf.put_slice(b"Response2"))
            .unwrap();
        server
            .handle_response(request1.msg_id(), |buf| buf.put_slice(b"Response1"))
            .unwrap();
        while let Some(output) = server.poll_output() {
            client.handle_input(&output.0).unwrap();
        }
        let response1 = client.poll_response().unwrap();
        assert_eq!(response1.msg_id(), request2.msg_id());
        assert_eq!(response1.data().as_ref(), b"Response2");
        let response2 = client.poll_response().unwrap();
        assert_eq!(response2.msg_id(), request1.msg_id());
        assert_eq!(response2.data().as_ref(), b"Response1");
    }
}
