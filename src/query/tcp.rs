// SPDX-FileCopyrightText: Copyright (c) 2017-2024 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::borrow::Cow;
use std::collections::VecDeque;
use std::result::Result;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Duration, Interval};
use tokio_util::codec::Framed;

use crate::{
    codec,
    frame::{tcp::*, *},
    QueryError,
};

/// Modbus TCP query
#[derive(Debug)]
pub struct Query<'a, T> {
    unit_id: UnitId,
    delay_timer: Interval,
    queries: VecDeque<RequestAdu<'a>>,
    framed: Option<Framed<T, codec::tcp::ClientCodec>>,
}

impl<'a, T> Query<'a, T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(unit_id: UnitId, delay_time: Duration) -> Query<'a, T> {
        Query {
            unit_id,
            framed: None,
            delay_timer: time::interval(delay_time),
            queries: VecDeque::new(),
        }
    }

    pub fn new_with_transport(t: T, unit_id: UnitId, delay_time: Duration) -> Query<'a, T> {
        Query {
            unit_id,
            framed: Some(Framed::new(t, codec::tcp::ClientCodec::default())),
            delay_timer: time::interval(delay_time),
            queries: VecDeque::new(),
        }
    }

    pub fn set_transport(&mut self, t: T) {
        self.framed = Some(Framed::new(t, codec::tcp::ClientCodec::default()));
    }

    pub fn discard(&mut self) {
        self.queries.clear();
    }

    pub fn transport_ok(&mut self) -> bool {
        self.framed.is_some()
    }

    /// Read multiple coils (0x01)
    pub fn read_coils(&mut self, addr: Address, cnt: Quantity) {
        self.enqueue(Request::ReadCoils(addr, cnt));
    }

    /// Read multiple discrete inputs (0x02)
    pub fn read_discrete_inputs(&mut self, addr: Address, cnt: Quantity) {
        self.enqueue(Request::ReadDiscreteInputs(addr, cnt));
    }

    /// Read multiple holding registers (0x03)
    pub fn read_holding_registers(&mut self, addr: Address, cnt: Quantity) {
        self.enqueue(Request::ReadHoldingRegisters(addr, cnt));
    }

    /// Read multiple input registers (0x04)
    pub fn read_input_registers(&mut self, addr: Address, cnt: Quantity) {
        self.enqueue(Request::ReadInputRegisters(addr, cnt));
    }

    /// Write a single coil (0x05)
    pub fn write_single_coil(&mut self, addr: Address, coil: Coil) {
        self.enqueue(Request::WriteSingleCoil(addr, coil));
    }

    /// Write a single holding register (0x06)
    pub fn write_single_register(&mut self, addr: Address, word: Word) {
        self.enqueue(Request::WriteSingleRegister(addr, word));
    }

    /// Write multiple coils (0x0F)
    pub fn write_multiple_coils(&mut self, addr: Address, coils: &[Coil]) {
        self.enqueue(Request::WriteMultipleCoils(addr, Cow::Owned(coils.into())));
    }

    /// Write multiple holding registers (0x10)
    pub fn write_multiple_registers(&mut self, addr: Address, words: &[Word]) {
        self.enqueue(Request::WriteMultipleRegisters(
            addr,
            Cow::Owned(words.into()),
        ));
    }

    pub fn enqueue_raw(&mut self, req: Request<'a>) {
        self.enqueue(req);
    }

    fn enqueue<R>(&mut self, req: R)
    where
        R: Into<RequestPdu<'a>>,
    {
        let adu = RequestAdu {
            hdr: Header {
                transaction_id: 0,
                unit_id: self.unit_id,
            },
            pdu: req.into(),
            disconnect: false,
        };
        self.queries.push_back(adu);
    }

    pub async fn next(&mut self) -> Result<Option<Response>, QueryError> {
        match &mut self.framed {
            Some(framed) => tokio::select! {
                r = framed.next() => match r {
                    None => {
                        self.discard();
                        self.framed = None;
                        Err(QueryError::EofReached)
                    },
                    Some(Err(e)) => Err(QueryError::TransportFailed(e)),
                    Some(Ok(v)) => match v.pdu.0 {
                        Ok(v1) => {
                            self.advance().await?;
                            Ok(Some(v1))
                        },
                        Err(e) => Err(QueryError::ModbusException(e)),
                    }
                },
                _ = self.delay_timer.tick() => {
                    self.advance().await.map(|_| None)
                },
            },
            None => Err(QueryError::EofReached),
        }
    }

    async fn advance(&mut self) -> Result<(), QueryError> {
        match &mut self.framed {
            Some(framed) => {
                if let Some(q) = self.queries.pop_front() {
                    framed
                        .send(q)
                        .await
                        .map_err(|e| QueryError::TransportFailed(e))
                } else {
                    Ok(())
                }
            }
            None => Err(QueryError::EofReached),
        }
    }
}
