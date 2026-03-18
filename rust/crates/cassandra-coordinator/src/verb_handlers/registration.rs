// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Verb handler registration: wires all data-path handlers to MessagingService.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.MessagingService.registerDefaultVerbs()`
//!
//! Registers handlers for Mutation, ReadData, ReadDigest, Hint,
//! BatchStore, BatchRemove, and ReadRepair verbs.

use std::sync::Arc;

use cassandra_messaging::MessagingService;
use cassandra_messaging::service::MessageHandler;
use cassandra_messaging::verb::Verb;
use tracing::info;

use super::batch_handler::{BatchRemoveVerbHandler, BatchStoreVerbHandler};
use super::hint_handler::HintVerbHandler;
use super::mutation_handler::MutationVerbHandler;
use super::read_handler::{ReadDataVerbHandler, ReadDigestVerbHandler};
use super::read_repair_handler::ReadRepairVerbHandler;

/// Register all data-path verb handlers with the messaging service.
///
/// This should be called during node startup, after the MessagingService
/// is created but before it starts accepting connections.
pub fn register_all_verb_handlers(messaging: &MessagingService) {
    // Mutation
    let mutation_handler: MessageHandler = Arc::new(|msg| MutationVerbHandler::handle(msg));
    messaging.register_handler(Verb::Mutation, mutation_handler);

    // ReadData
    let read_data_handler: MessageHandler = Arc::new(|msg| ReadDataVerbHandler::handle(msg));
    messaging.register_handler(Verb::ReadData, read_data_handler);

    // ReadDigest
    let read_digest_handler: MessageHandler = Arc::new(|msg| ReadDigestVerbHandler::handle(msg));
    messaging.register_handler(Verb::ReadDigest, read_digest_handler);

    // Hint
    let hint_handler: MessageHandler = Arc::new(|msg| HintVerbHandler::handle(msg));
    messaging.register_handler(Verb::Hint, hint_handler);

    // BatchStore
    let batch_store_handler: MessageHandler = Arc::new(|msg| BatchStoreVerbHandler::handle(msg));
    messaging.register_handler(Verb::BatchStore, batch_store_handler);

    // BatchRemove
    let batch_remove_handler: MessageHandler = Arc::new(|msg| BatchRemoveVerbHandler::handle(msg));
    messaging.register_handler(Verb::BatchRemove, batch_remove_handler);

    // ReadRepair
    let read_repair_handler: MessageHandler = Arc::new(|msg| ReadRepairVerbHandler::handle(msg));
    messaging.register_handler(Verb::ReadRepair, read_repair_handler);

    info!("Registered all data-path verb handlers");
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_messaging::frame::Message;

    #[test]
    fn register_and_dispatch_all_verbs() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);

        register_all_verb_handlers(&svc);

        // Each registered verb should dispatch successfully (not return None).
        let verbs_and_payloads = vec![
            (Verb::Mutation, make_mutation_payload()),
            (Verb::ReadData, make_read_data_payload()),
            (Verb::ReadDigest, make_read_digest_payload()),
            (Verb::Hint, make_hint_payload()),
            (Verb::BatchStore, make_batch_store_payload()),
            (Verb::BatchRemove, make_batch_remove_payload()),
            (Verb::ReadRepair, make_read_repair_payload()),
        ];

        for (verb, payload) in verbs_and_payloads {
            let msg = Message::request(verb, 1, payload);
            let result = svc.dispatch(msg);
            assert!(
                result.is_some(),
                "Handler for {verb} should return a response"
            );
            let resp = result.unwrap();
            assert!(
                resp.is_response(),
                "Response for {verb} should have RESPONSE flag"
            );
        }
    }

    #[test]
    fn unregistered_verb_returns_none() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);
        register_all_verb_handlers(&svc);

        // Ping is NOT registered by register_all_verb_handlers.
        let msg = Message::request(Verb::Ping, 1, Vec::new());
        assert!(svc.dispatch(msg).is_none());
    }

    fn make_mutation_payload() -> Vec<u8> {
        use super::super::mutation_handler::MutationRequest;
        use crate::write::CoordinatedMutation;
        let req = MutationRequest {
            mutation: CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000),
        };
        serde_json::to_vec(&req).unwrap()
    }

    fn make_read_data_payload() -> Vec<u8> {
        use super::super::read_handler::ReadDataRequest;
        serde_json::to_vec(&ReadDataRequest {
            keyspace: "ks".into(),
            table: "tbl".into(),
            partition_key: vec![1],
        })
        .unwrap()
    }

    fn make_read_digest_payload() -> Vec<u8> {
        use super::super::read_handler::ReadDigestRequest;
        serde_json::to_vec(&ReadDigestRequest {
            keyspace: "ks".into(),
            table: "tbl".into(),
            partition_key: vec![1],
        })
        .unwrap()
    }

    fn make_hint_payload() -> Vec<u8> {
        use super::super::hint_handler::HintRequest;
        use crate::write::CoordinatedMutation;
        serde_json::to_vec(&HintRequest {
            mutation: CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000),
            hint_id: 1,
            created_at: 1_700_000_000_000,
        })
        .unwrap()
    }

    fn make_batch_store_payload() -> Vec<u8> {
        use super::super::batch_handler::BatchStoreRequest;
        use crate::batch::BatchType;
        use crate::write::CoordinatedMutation;
        serde_json::to_vec(&BatchStoreRequest {
            id: uuid::Uuid::new_v4(),
            batch_type: BatchType::Logged,
            mutations: vec![CoordinatedMutation::simple(
                "ks".into(),
                "tbl".into(),
                vec![1],
                vec![],
                1000,
            )],
            created_at: 1_700_000_000_000,
        })
        .unwrap()
    }

    fn make_batch_remove_payload() -> Vec<u8> {
        use super::super::batch_handler::BatchRemoveRequest;
        serde_json::to_vec(&BatchRemoveRequest {
            id: uuid::Uuid::new_v4(),
        })
        .unwrap()
    }

    fn make_read_repair_payload() -> Vec<u8> {
        use super::super::read_repair_handler::ReadRepairRequest;
        use crate::write::CoordinatedMutation;
        serde_json::to_vec(&ReadRepairRequest {
            mutation: CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000),
        })
        .unwrap()
    }
}
