// SPDX-FileCopyrightText: Copyright (c) 2017-2024 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::VecDeque;
use std::result::Result;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tokio::time::{self, Interval, Duration};

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
    framed: Framed<T, codec::tcp::ClientCodec>,
}

impl<'a, T> Query<'a, T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(t: T, unit_id: UnitId, delay_time: Duration) -> Query<'a, T> {
        Query {
            unit_id,
            framed: Framed::new(t, codec::tcp::ClientCodec::default()),
            delay_timer: time::interval(delay_time),
            queries: VecDeque::new(),
        }
    }

    pub fn recover(&mut self, t: T) {
        self.framed = Framed::new(t, codec::tcp::ClientCodec::default());
    }

    /// Read multiple coils (0x01)
    pub fn read_coils(&mut self, addr: Address, cnt: Quantity) {
        self.prepare(Request::ReadCoils(addr, cnt));
    }

    /// Read multiple discrete inputs (0x02)
    pub fn read_discrete_inputs(&mut self, addr: Address, cnt: Quantity) {
        self.prepare(Request::ReadDiscreteInputs(addr, cnt));
    }

    /// Read multiple holding registers (0x03)
    pub fn read_holding_registers(&mut self, addr: Address, cnt: Quantity) {
        self.prepare(Request::ReadHoldingRegisters(addr, cnt));
    }

    /// Read multiple input registers (0x04)
    pub fn read_input_registers(&mut self, addr: Address, cnt: Quantity) {
        self.prepare(Request::ReadInputRegisters(addr, cnt));
    }

    /// Write a single coil (0x05)
    pub fn write_single_coil(&mut self, addr: Address, coil: Coil) {
        self.prepare(Request::WriteSingleCoil(addr, coil));
    }

    /// Write a single holding register (0x06)
    pub fn write_single_register(&mut self, addr: Address, word: Word) {
        self.prepare(Request::WriteSingleRegister(addr, word));
    }

    /// Write multiple coils (0x0F)
    pub fn write_multiple_coils(&mut self, addr: Address, coils: &'a [Coil]) {
        self.prepare(Request::WriteMultipleCoils(addr, std::borrow::Cow::Borrowed(coils)));
    }

    /// Write multiple holding registers (0x10)
    pub fn write_multiple_registers(&mut self, addr: Address, words: &'a [Word]) {
        self.prepare(Request::WriteMultipleRegisters(addr, std::borrow::Cow::Borrowed(words)));
    }

    fn prepare<R>(&mut self, req: R)
    where
        R: Into<RequestPdu<'a>>
    {
        let adu = RequestAdu {
            hdr: Header {
                transaction_id: 0,
                unit_id: self.unit_id
            },
            pdu: req.into(),
            disconnect: false,
        };
        self.queries.push_back(adu);
    }

    pub async fn next(&mut self) -> Result<Option<Response>, QueryError> {
        tokio::select! {
            r = self.framed.next() => match r {
                None => Err(QueryError::EofReached),
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
        }
    }

    async fn advance(&mut self) -> Result<(), QueryError> {
        if let Some(q) = self.queries.pop_front() {
            self.framed
                .send(q)
                .await
                .map_err(|e| QueryError::TransportFailed(e))
        } else {
            Ok(())
        }
    }
}
