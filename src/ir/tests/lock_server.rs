use crate::{
    discovery::InMemoryShardDirectory,
    ChannelRegistry, ChannelTransport, IrClient, IrClientId, IrMembership, IrMembershipSize,
    IrOpId, IrRecordView, IrReplica, IrReplicaUpcalls, Transport,
};
use crate::mvcc::disk::disk_io::OpenFlags;
use crate::mvcc::disk::memory_io::MemoryIo;
use crate::unified::ir::ir_record_store::{PersistentIrRecordStore, PersistentPayload};
use rand::{seq::IteratorRandom, Rng, SeedableRng};
use rand::rngs::StdRng;

fn test_rng(seed: u64) -> crate::Rng {
    crate::Rng::from_seed(seed)
}
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

#[tokio::test(start_paused = true)]
async fn lock_server_1() {
    timeout_lock_server(1).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_2() {
    timeout_lock_server(2).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_3() {
    timeout_lock_server(3).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_4() {
    timeout_lock_server(4).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_5() {
    timeout_lock_server(5).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_7() {
    timeout_lock_server(7).await;
}

#[tokio::test(start_paused = true)]
async fn lock_server_9() {
    timeout_lock_server(9).await;
}

#[ignore]
#[tokio::test(start_paused = true)]
async fn lock_server_loop() {
    loop {
        timeout_lock_server(3).await;
    }
}

async fn timeout_lock_server(num_replicas: usize) {
    tokio::time::timeout(
        Duration::from_secs((num_replicas as u64 + 10) * 10),
        lock_server(num_replicas),
    )
    .await
    .unwrap();
}

async fn lock_server(num_replicas: usize) {
    let mut rng = StdRng::seed_from_u64(num_replicas as u64);
    println!("testing lock server with {num_replicas} replicas");

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    struct Lock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    struct Unlock(IrClientId);

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
    enum LockResult {
        Ok,
        No,
    }

    #[derive(Serialize, Deserialize)]
    struct Upcalls {
        locked: Option<IrClientId>,
    }

    type RecordStore = PersistentIrRecordStore<Unlock, Lock, LockResult, MemoryIo>;

    impl IrReplicaUpcalls for Upcalls {
        type UO = ();
        type UR = ();
        type IO = Unlock;
        type IR = ();
        type CO = Lock;
        type CR = LockResult;
        type Payload = PersistentPayload<Self::IO, Self::CO, Self::CR>;

        fn exec_unlogged(&self, _op: Self::UO) -> Self::UR {
            unreachable!();
        }

        fn exec_inconsistent(&mut self, _op_id: &IrOpId, op: &Self::IO) -> Option<Self::IR> {
            if Some(op.0) == self.locked {
                self.locked = None;
            }
            None
        }

        fn exec_consensus(&mut self, _op_id: &IrOpId, op: &Self::CO) -> Self::CR {
            if self.locked.is_none() || self.locked == Some(op.0) {
                self.locked = Some(op.0);
                LockResult::Ok
            } else {
                LockResult::No
            }
        }

        fn sync<L: IrRecordView<IO = Self::IO, CO = Self::CO, CR = Self::CR>, R: IrRecordView<IO = Self::IO, CO = Self::CO, CR = Self::CR>>(&mut self, _: &L, record: &R) {
            self.locked = None;

            let mut locked = BTreeSet::<IrClientId>::new();
            let mut unlocked = BTreeSet::<IrClientId>::new();
            for (_, entry) in record.inconsistent_entries() {
                unlocked.insert(entry.op.0);
            }
            for (_, entry) in record.consensus_entries() {
                if matches!(entry.result, LockResult::Ok) {
                    locked.insert(entry.op.0);
                }
            }

            for client_id in locked {
                if !unlocked.contains(&client_id) {
                    if self.locked.is_some() {
                        panic!();
                    }
                    self.locked = Some(client_id);
                }
            }
        }

        // Lock server is in-memory only: no durable storage to flush.
        fn flush(&mut self) {}

        fn merge(
            &mut self,
            d: BTreeMap<IrOpId, (Self::CO, Self::CR)>,
            u: Vec<(IrOpId, Self::CO, Self::CR)>,
        ) -> BTreeMap<IrOpId, Self::CR> {
            let mut results = BTreeMap::<IrOpId, Self::CR>::new();

            for (op_id, (request, reply)) in &d {
                let successful = matches!(reply, LockResult::Ok);

                results.insert(
                    *op_id,
                    if successful && (self.locked.is_none() || self.locked == Some(request.0)) {
                        self.locked = Some(request.0);
                        LockResult::Ok
                    } else {
                        LockResult::No
                    },
                );
            }

            for (op_id, op, _) in &u {
                results.insert(*op_id, self.exec_consensus(op_id, op));
            }

            results
        }
    }

    let registry = ChannelRegistry::default();
    let dir = Arc::new(InMemoryShardDirectory::new());
    let membership = IrMembership::new((0..num_replicas).collect::<Vec<_>>());
    let mut lib_rng = test_rng(num_replicas as u64);

    fn create_replica(
        rng: &mut crate::Rng,
        registry: &ChannelRegistry<Upcalls>,
        dir: &Arc<InMemoryShardDirectory<usize>>,
        membership: &IrMembership<usize>,
    ) -> Arc<IrReplica<Upcalls, ChannelTransport<Upcalls>, RecordStore>> {
        Arc::new_cyclic(
            |weak: &std::sync::Weak<IrReplica<Upcalls, ChannelTransport<Upcalls>, RecordStore>>| {
                let weak = weak.clone();
                let channel =
                    registry.channel(move |from, message| weak.upgrade()?.receive(from, message), Arc::clone(dir));
                let upcalls = Upcalls { locked: None };
                let io_flags = OpenFlags { create: true, direct: false };
                let record_store = RecordStore::open(
                    &MemoryIo::temp_path(),
                    io_flags,
                ).unwrap();
                IrReplica::new(rng.fork(), membership.clone(), upcalls, channel, None, record_store)
            },
        )
    }

    let mut replicas = (0..num_replicas)
        .map(|_| create_replica(&mut lib_rng, &registry, &dir, &membership))
        .collect::<Vec<_>>();

    fn create_client(
        rng: &mut crate::Rng,
        registry: &ChannelRegistry<Upcalls>,
        dir: &Arc<InMemoryShardDirectory<usize>>,
        membership: &IrMembership<usize>,
    ) -> Arc<IrClient<Upcalls, ChannelTransport<Upcalls>>> {
        let channel = registry.channel(move |_, _| unreachable!(), Arc::clone(dir));
        Arc::new(IrClient::new(rng.fork(), membership.clone(), channel))
    }

    let clients = (0..2)
        .map(|_| create_client(&mut lib_rng, &registry, &dir, &membership))
        .collect::<Vec<_>>();

    let decide_lock = |results: BTreeMap<LockResult, usize>, membership: IrMembershipSize| {
        //println!("deciding {ok} of {} : {results:?}", results.len());
        if results.get(&LockResult::Ok).copied().unwrap_or_default() >= membership.f_plus_one() {
            LockResult::Ok
        } else {
            LockResult::No
        }
    };

    fn add_replica(
        rng: &mut crate::Rng,
        replicas: &mut Vec<Arc<IrReplica<Upcalls, ChannelTransport<Upcalls>, RecordStore>>>,
        registry: &ChannelRegistry<Upcalls>,
        dir: &Arc<InMemoryShardDirectory<usize>>,
        membership: &IrMembership<usize>,
    ) {
        let new = create_replica(rng, registry, dir, membership);
        for d in &*replicas {
            new.transport().do_send(
                d.address(),
                crate::ir::AddMember {
                    address: new.address(),
                },
            );
        }
        replicas.push(new);
    }

    for i in 0..8 {
        assert_eq!(
            clients[0]
                .invoke_consensus(Lock(clients[0].id()), &decide_lock)
                .await,
            LockResult::Ok,
            "{i}"
        );

        assert_eq!(
            clients[1]
                .invoke_consensus(Lock(clients[1].id()), &decide_lock)
                .await,
            LockResult::No,
            "{i}"
        );

        for _ in 0..2 {
            if rng.r#gen() {
                let to_remove = replicas
                    .iter()
                    .map(|r| r.address())
                    .choose(&mut rng)
                    .unwrap();
                for r in replicas.iter() {
                    clients[0]
                        .transport()
                        .do_send(r.address(), crate::ir::RemoveMember { address: to_remove });
                }
            }
            if rng.r#gen() {
                add_replica(&mut lib_rng, &mut replicas, &registry, &dir, &membership);
            }
        }
    }

    clients[0]
        .invoke_inconsistent(Unlock(clients[0].id()))
        .await;

    for _ in 0..(replicas.len() + 1) * 20 {
        ChannelTransport::<Upcalls>::sleep(Duration::from_secs(5)).await;

        eprintln!("@@@@@ INVOKE {replicas:?}");
        if clients[1]
            .invoke_consensus(Lock(clients[1].id()), &decide_lock)
            .await
            == LockResult::Ok
        {
            return;
        }
    }

    panic!();
}
