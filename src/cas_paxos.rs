use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::{
    kv_store::KeyValueStore,
    message::{Body, ErrorCode, Message},
    node::Node,
};

enum Role {
    Proposer { op: Message },
    Acceptor,
}

// NOTE Here, we store the entire key-value store in a single CASPaxos instance.
//      A non-toy implementatation would instead store the kv store as a set of
//      independent, labelled CASPaxos instances (where each instance label
//      corresponds to a key in the kv store. See section '2.3.3 Optimization'
//      in the CASPaxos paper.
// TODO Implement the optimization above.
struct CASPaxos {
    node: Arc<Node>,
    state_machine: Mutex<KeyValueStore<usize, usize>>,
    role: Mutex<Role>,
    highest_known_ballot_number: AtomicUsize,
}

impl CASPaxos {
    pub fn new() -> Self {
        Self {
            node: Arc::new(Node::new()),
            state_machine: Mutex::new(KeyValueStore::default()),
            role: Mutex::new(Role::Acceptor),
            highest_known_ballot_number: AtomicUsize::new(0),
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
            Body::Init { .. } => {
                // NOTE: By the time we receive this Init, its content was already used by
                //       self.node to store the node ids provided by the msg.
                //       So all we have to do here is to respond with InitOk.
                let _ = self
                    .node
                    .clone()
                    .send(
                        &msg.src,
                        Body::InitOk {
                            in_reply_to: msg.body.msg_id,
                        },
                        None,
                    )
                    .await;
            }
            Body::Read { key } => todo!(),
            Body::Write { key, value } => todo!(),
            Body::Cas { key, from, to } => todo!(),
            Body::Proxy { proxied_msg } => todo!(),
            Body::Propose { ballot_number } => {
                let highest_known_ballot_number =
                    self.highest_known_ballot_number.load(Ordering::SeqCst);

                if ballot_number < self.highest_known_ballot_number.load(Ordering::SeqCst) {
                    let text = format!(
                        "expected a ballot number greater than {highest_known_ballot_number}"
                    );
                    let body = Body::Error {
                        in_reply_to: msg.body.msg_id,
                        code: ErrorCode::PreconditionFailed,
                        text,
                    };

                    self.node.clone().send(&msg.src, body, None).await;
                    return;
                }

                let existing_ballot_number = self
                    .highest_known_ballot_number
                    .swap(ballot_number, Ordering::SeqCst);

                let body = Body::Promise {
                    ballot_number: existing_ballot_number,
                    value: self.state_machine.lock().unwrap().clone(),
                };

                self.node.clone().send(&msg.src, body, None).await;
            }
            Body::Promise { .. } => todo!(),
            Body::Error {
                in_reply_to,
                code,
                text,
            } => todo!(),
            Body::InitOk { .. }
            | Body::ReadOk { .. }
            | Body::WriteOk { .. }
            | Body::CasOk { .. } => panic!("i shouldn't receive this ack msg"),
        }
    }

    async fn propose(self: Arc<Self>, op: Message) {
        let mut role_guard = self.role.lock().unwrap();

        // switch to Proposer state, no matter what
        *role_guard = Role::Proposer { op };

        // stop tracking incoming accepts of prior proposals if any?
        // broadcast propose
        todo!()
    }

    async fn promise(self: Arc<Self>) {
        // switch to acceptor, no matter what
        todo!()
    }

    async fn accept(self: Arc<Self>) {
        // assert that i am currently an acceptor
        todo!()
    }
}
