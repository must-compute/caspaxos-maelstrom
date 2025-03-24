use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    kv_store::KeyValueStore,
    message::{Body, ErrorCode, Message},
    node::Node,
};

type BallotNumber = usize;
type NodeId = String;
type StateMachine = KeyValueStore<usize, usize>;
type PromisesInbox = Vec<(NodeId, BallotNumber, StateMachine)>;
type AcceptanceInbox = HashSet<(NodeId, BallotNumber)>;

#[derive(Clone, Debug)]
enum Role {
    Proposer {
        op: Message,
        last_accept_broadcast: BallotNumber, // ballot_number of last broadcast of Accept msgs
        promises_inbox: PromisesInbox,
        acceptance_inbox: AcceptanceInbox,
        pending_client_repsonse_body: Option<Body>,
    },
    Acceptor,
}

impl Role {
    fn add_promise_to_inbox(
        &mut self,
        node_id: &str,
        ballot_number: BallotNumber,
        state_machine: StateMachine,
    ) {
        match self {
            Role::Acceptor => panic!("got called on an Acceptor instead of a Proposer"),
            Role::Proposer {
                ref mut promises_inbox,
                ..
            } => {
                promises_inbox.push((node_id.to_string(), ballot_number, state_machine));
            }
        }
    }

    fn promises_inbox(&self) -> PromisesInbox {
        match self {
            Role::Acceptor => panic!("got called on an Acceptor instead of a Proposer"),
            Role::Proposer {
                ref promises_inbox, ..
            } => promises_inbox.clone(),
        }
    }

    fn set_last_accept_broadcast(&mut self, ballot_number: usize) {
        match self {
            Role::Acceptor => panic!("got called on an Acceptor instead of a Proposer"),
            Role::Proposer {
                ref mut last_accept_broadcast,
                ..
            } => *last_accept_broadcast = ballot_number,
        }
    }

    fn set_pending_client_response_body(&mut self, body: Body) {
        match self {
            Role::Acceptor => panic!("got called on an Acceptor instead of a Proposer"),
            Role::Proposer {
                ref mut pending_client_repsonse_body,
                ..
            } => *pending_client_repsonse_body = Some(body),
        }
    }
}

// NOTE Here, we store the entire key-value store in a single CASPaxos instance.
//      A non-toy implementatation would instead store the kv store as a set of
//      independent, labelled CASPaxos instances (where each instance label
//      corresponds to a key in the kv store. See section '2.3.3 Optimization'
//      in the CASPaxos paper.
// TODO Implement the optimization above.
pub struct CASPaxos {
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
            Body::Read { .. } | Body::Write { .. } | Body::Cas { .. } => {
                self.clone().propose(msg).await;
            }
            Body::Proxy { proxied_msg } => todo!(),
            Body::Propose { ballot_number } => {
                self.promise(&msg.src, msg.body.msg_id, ballot_number).await;
            }
            Body::Promise {
                ballot_number,
                value,
            } => {
                self.handle_promise_msg(&msg.src, msg.body.msg_id, ballot_number, value)
                    .await;
            }
            Body::Accept {
                ballot_number,
                value,
            } => {
                self.accept(&msg.src, msg.body.msg_id, ballot_number, value)
                    .await;
            }
            Body::Accepted { ballot_number } => {}
            Body::Error {
                in_reply_to,
                code,
                text,
            } => eprintln!("GOT AN ERROR - TODO"),
            Body::InitOk { .. }
            | Body::ReadOk { .. }
            | Body::WriteOk { .. }
            | Body::CasOk { .. } => panic!("i shouldn't receive this ack msg"),
        }
    }

    async fn promise(self: Arc<Self>, src: &str, src_msg_id: usize, ballot_number: usize) {
        if self.highest_known_ballot_number.load(Ordering::SeqCst) > ballot_number {
            self.clone()
                .send_reject_ballot_number(src, src_msg_id)
                .await;
            return;
        }

        self.highest_known_ballot_number
            .store(ballot_number, Ordering::SeqCst);

        let body = Body::Promise {
            ballot_number,
            value: self.state_machine.lock().unwrap().clone(),
        };

        self.node.clone().send(src, body, None).await;
    }

    async fn handle_promise_msg(
        self: Arc<Self>,
        src: &str,
        src_msg_id: usize,
        ballot_number: usize,
        value: KeyValueStore<usize, usize>,
    ) {
        let mut ballot_number_was_rejected = false;
        let mut should_broadcast_accept = false;
        {
            let mut role_guard = self.role.lock().unwrap();
            match &*role_guard {
                Role::Acceptor => (),
                Role::Proposer {
                    last_accept_broadcast,
                    op,
                    ..
                } => {
                    if self.highest_known_ballot_number.load(Ordering::SeqCst) > ballot_number {
                        ballot_number_was_rejected = true;
                    } else {
                        let last_accept_broadcast = *last_accept_broadcast;
                        let op = op.clone();
                        role_guard.add_promise_to_inbox(src, ballot_number, value);

                        let majority_is_reached_for_the_first_time =
                            role_guard.promises_inbox().len() >= self.majority_count()
                                && last_accept_broadcast < ballot_number;
                        if majority_is_reached_for_the_first_time {
                            role_guard.set_last_accept_broadcast(ballot_number);
                            should_broadcast_accept = true;

                            let mut promises = role_guard.promises_inbox();
                            // desc. sort by ballot_number, then node id as a tie breaker.
                            promises.sort_by(|b, a| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

                            let (_, _, mut state) = promises.first().unwrap().clone();
                            let body = self.clone().apply_to_state_machine(&op, &mut state);

                            *self.state_machine.lock().unwrap() = state;
                            role_guard.set_pending_client_response_body(body);
                        }
                    }
                }
            };
        } // role_guard dropped

        if ballot_number_was_rejected {
            self.send_reject_ballot_number(src, src_msg_id).await;
            return;
        }

        if should_broadcast_accept {
            let body = Body::Accept {
                ballot_number,
                value: self.state_machine.lock().unwrap().clone(),
            };
            self.node.clone().broadcast(body, None).await;
        }
    }

    async fn accept(
        self: Arc<Self>,
        src: &str,
        src_msg_id: usize,
        ballot_number: usize,
        value: KeyValueStore<usize, usize>,
    ) {
        let role = self.role.lock().unwrap().clone();
        match role {
            Role::Proposer { .. } => (),
            Role::Acceptor => {
                if self.highest_known_ballot_number.load(Ordering::SeqCst) > ballot_number {
                    self.clone()
                        .send_reject_ballot_number(src, src_msg_id)
                        .await;
                    return;
                }

                *self.state_machine.lock().unwrap() = value;

                self.node
                    .clone()
                    .send(src, Body::Accepted { ballot_number }, None)
                    .await;
            }
        }
    }

    async fn propose(self: Arc<Self>, op: Message) {
        {
            let mut role_guard = self.role.lock().unwrap();
            let last_accept_broadcast = match *role_guard {
                Role::Proposer {
                    last_accept_broadcast,
                    ..
                } => last_accept_broadcast,
                Role::Acceptor => 0,
            };

            *role_guard = Role::Proposer {
                op,
                last_accept_broadcast,
                promises_inbox: Vec::new(),
                pending_client_repsonse_body: None,
                acceptance_inbox: HashSet::new(),
            };
        }

        self.highest_known_ballot_number
            .fetch_add(1, Ordering::SeqCst);
        let ballot_number = self.highest_known_ballot_number.load(Ordering::SeqCst);
        let body = Body::Propose { ballot_number };

        self.node.clone().broadcast(body, None).await;
    }

    // TODO we should track the source of the highest known ballot number, since we might need to use
    //      node ids for tie breakers in case the incoming ballot number matches the number we've seen before.
    async fn send_reject_ballot_number(self: Arc<Self>, dest: &str, in_reply_to: usize) {
        let body = Body::Error {
            in_reply_to,
            code: ErrorCode::PreconditionFailed,
            text: String::from("exepcted a greater ballot number"),
        };

        self.node.clone().send(dest, body, None).await;
    }

    fn majority_count(&self) -> usize {
        let all_nodes_count = self.node.other_node_ids.get().unwrap().len() + 1;
        (all_nodes_count / 2) + 1
    }

    fn apply_to_state_machine(
        self: Arc<Self>,
        msg: &Message,
        state_machine: &mut KeyValueStore<usize, usize>,
    ) -> Body {
        match msg.body.inner {
            Body::Read { key } => {
                let result = state_machine.read(&key);

                match result {
                    Some(value) => Body::ReadOk {
                        in_reply_to: msg.body.msg_id,
                        value: *value,
                    },
                    None => {
                        let err = ErrorCode::KeyDoesNotExist;
                        Body::Error {
                            in_reply_to: msg.body.msg_id,
                            code: err.clone(),
                            text: err.to_string(),
                        }
                    }
                }
            }
            Body::Write { key, value } => {
                state_machine.write(key, value);
                Body::WriteOk {
                    in_reply_to: msg.body.msg_id,
                }
            }
            Body::Cas { key, from, to } => {
                let result = state_machine.cas(key, from, to);

                match result {
                    Ok(()) => Body::CasOk {
                        in_reply_to: msg.body.msg_id,
                    },
                    Err(e) => match e.downcast_ref::<ErrorCode>() {
                        Some(e @ ErrorCode::PreconditionFailed)
                        | Some(e @ ErrorCode::KeyDoesNotExist) => Body::Error {
                            in_reply_to: msg.body.msg_id,
                            code: e.clone(),
                            text: e.to_string(),
                        },
                        _ => panic!("encountered an unexpected error while processing Cas request"),
                    },
                }
            }
            _ => unreachable!(),
        }
    }
}
