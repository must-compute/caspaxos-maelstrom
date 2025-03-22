use std::sync::{Arc, Mutex};

use crate::{
    kv_store::KeyValueStore,
    message::{Body, Message},
    node::Node,
};

struct CASPaxos {
    node: Arc<Node>,
    state_machine: Mutex<KeyValueStore<usize, usize>>,
}

impl CASPaxos {
    pub fn new() -> Self {
        Self {
            node: Arc::new(Node::new()),
            state_machine: Mutex::new(KeyValueStore::default()),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut rx = self.node.clone().run().await;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    tokio::spawn({
                        let cas_paxos = self.clone();
                        async move { cas_paxos.handle(msg).await }
                    });
                }
            };
        }
    }

    async fn handle(self: Arc<Self>, msg: Message) {
        match msg.body.inner.clone() {
            Body::Init { node_id, node_ids } => todo!(),
            Body::Read { key } => todo!(),
            Body::Write { key, value } => todo!(),
            Body::Cas { key, from, to } => todo!(),
            Body::Proxy { proxied_msg } => todo!(),
            Body::Error {
                in_reply_to,
                code,
                text,
            } => todo!(),
            Body::InitOk { .. }
            | Body::ReadOk { .. }
            | Body::WriteOk { .. }
            | Body::CasOk { .. } => todo!(),
        }
    }
}
