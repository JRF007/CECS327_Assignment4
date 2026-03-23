from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Set
import heapq
import os
import sys
import random

class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()
Timestamp = Tuple[int, int]  # (lamport_clock, replica_id)

@dataclass(order=True)
class QueueItem:
    sort_key: Timestamp = field(init=False)
    ts: Timestamp
    update_id: str
    op: Tuple[Any, ...]
    origin_id: int

    def __post_init__(self):
        self.sort_key = self.ts

@dataclass
class TOBCAST:
    update_id: str
    op: Tuple[Any, ...]
    ts: Timestamp
    sender_id: int   # original sender of the update

@dataclass
class ACK:
    update_id: str
    original_ts: Timestamp     # timestamp of the update being acknowledged
    progress_ts: Timestamp     # current timestamp of the ack sender
    sender_id: int             # replica sending the ACK

class Network:
    def __init__(self):
        self.replicas: Dict[int, "Replica"] = {}
        self.queue: List[Tuple[int, object]] = []

    def register(self, replica: "Replica") -> None:
        self.replicas[replica.replica_id] = replica

    def send(self, dest_id: int, msg: object) -> None:
        self.queue.append((dest_id, msg))

    def multicast(self, msg: object) -> None:
        for rid in sorted(self.replicas.keys()):
            self.send(rid, msg)

    def run(self) -> None:
        while self.queue:
            dest_id, msg = self.queue.pop(0)
            self.replicas[dest_id].on_receive(msg)


class Replica:
    def __init__(self, replica_id: int, replica_ids: List[int], network: Network):
        self.replica_id = replica_id
        self.replica_ids = sorted(replica_ids)
        self.network = network
        self.clock = 0
        # Holdback queue ordered by (ts.clock, ts.replica_id)
        self.holdback: List[QueueItem] = []
        self.known_updates: Dict[str, QueueItem] = {}
        self.delivered: Set[str] = set()
        # max_seen[k] = largest progress timestamp seen from replica k
        self.max_seen: Dict[int, Timestamp] = {
            rid: (-1, -1) for rid in self.replica_ids
        }
        self.store: Dict[str, Any] = {}
        self.delivery_log: List[str] = []

    def _bump_clock(self) -> Timestamp:
        self.clock += 1
        return (self.clock, self.replica_id)

    def _update_clock_on_receive(self, incoming_ts: Timestamp) -> None:
        self.clock = max(self.clock, incoming_ts[0]) + 1

    @staticmethod
    def _max_ts(a: Timestamp, b: Timestamp) -> Timestamp:
        return a if a > b else b

    def _enqueue_if_new(self, update_id: str, op: Tuple[Any, ...], ts: Timestamp, origin_id: int) -> None:
        if update_id in self.known_updates:
            return
        item = QueueItem(ts=ts, update_id=update_id, op=op, origin_id=origin_id)
        self.known_updates[update_id] = item
        heapq.heappush(self.holdback, item)

    def client_update(self, update_id: str, op: Tuple[Any, ...]) -> None:
        ts = self._bump_clock()
        # Add locally
        self._enqueue_if_new(update_id, op, ts, self.replica_id)
        # Local progress
        self.max_seen[self.replica_id] = self._max_ts(self.max_seen[self.replica_id], ts)
        # Multicast update
        msg = TOBCAST(update_id=update_id, op=op, ts=ts, sender_id=self.replica_id)
        self.network.multicast(msg)
        self.try_deliver()
        print(f"Replica {self.replica_id} client update {update_id} assigned ts={ts}")

    def on_receive(self, msg: object) -> None:
        if isinstance(msg, TOBCAST):
            self.on_receive_tobcast(msg)
        elif isinstance(msg, ACK):
            self.on_receive_ack(msg)
        else:
            raise ValueError(f"Unknown message type: {type(msg)}")

    def on_receive_tobcast(self, msg: TOBCAST) -> None:
        # Lamport receive rule using original update timestamp
        self._update_clock_on_receive(msg.ts)
        # Queue the update
        self._enqueue_if_new(msg.update_id, msg.op, msg.ts, msg.sender_id)
        # We have seen progress from the original sender up to msg.ts
        self.max_seen[msg.sender_id] = self._max_ts(self.max_seen[msg.sender_id], msg.ts)
        progress_ts = self._bump_clock()
        self.max_seen[self.replica_id] = self._max_ts(self.max_seen[self.replica_id], progress_ts)
        ack = ACK(
            update_id=msg.update_id,
            original_ts=msg.ts,
            progress_ts=progress_ts,
            sender_id=self.replica_id,
        )
        self.network.multicast(ack)
        self.try_deliver()
        print(f"Replica {self.replica_id} received TOBCAST {msg.update_id} ts={msg.ts} from R{msg.sender_id}")

    def on_receive_ack(self, ack: ACK) -> None:
        # Lamport receive rule using the ack sender's progress timestamp
        self._update_clock_on_receive(ack.progress_ts)
        # ACK tells us the sender has progressed to progress_ts
        self.max_seen[ack.sender_id] = self._max_ts(
            self.max_seen[ack.sender_id], ack.progress_ts
        )
        self.try_deliver()

    def can_deliver_head(self) -> bool:
        if not self.holdback:
            return False
        head = self.holdback[0]
        # head is safe only if every replica has progressed beyond head.ts
        for rid in self.replica_ids:
            if self.max_seen[rid] <= head.ts:
                return False
        return True

    def try_deliver(self) -> None:
        while self.holdback and self.can_deliver_head():
            head = heapq.heappop(self.holdback)
            if head.update_id in self.delivered:
                continue
            self.apply(head.op)
            self.delivered.add(head.update_id)
            self.delivery_log.append(head.update_id)
            print(f"Replica {self.replica_id} delivered {head.update_id} ts={head.ts}")

    def apply(self, op: Tuple[Any, ...]) -> None:
        kind = op[0]
        if kind == "put":
            _, key, value = op
            self.store[key] = value
        elif kind == "append":
            _, key, suffix = op
            current = self.store.get(key, "")
            self.store[key] = str(current) + str(suffix)
        elif kind == "incr":
            _, key = op
            current = self.store.get(key, 0)
            self.store[key] = current + 1
        else:
            raise ValueError(f"Unsupported operation: {op}")

    def summary(self) -> str:
        return (
            f"Replica {self.replica_id} | "
            f"clock={self.clock} | "
            f"delivered={self.delivery_log} | "
            f"store={self.store}"
        )


def demo():
    network = Network()
    replica_ids = [1, 2, 3]
    replicas: Dict[int, Replica] = {}
    for rid in replica_ids:
        r = Replica(rid, replica_ids, network)
        replicas[rid] = r
        network.register(r)
    # Client updates sent to different replicas
    replicas[1].client_update("u1", ("append", "x", "A"))
    replicas[2].client_update("u2", ("append", "x", "B"))
    replicas[3].client_update("u3", ("incr", "counter"))
    replicas[1].client_update("u4", ("put", "done", True))
    # Run the simulated network
    network.run()
    print("Final states:")
    for rid in replica_ids:
        print(replicas[rid].summary())
    first_log = replicas[1].delivery_log
    first_store = replicas[1].store
    same_order = all(replicas[rid].delivery_log == first_log for rid in replica_ids)
    same_state = all(replicas[rid].store == first_store for rid in replica_ids)
    print("\nConsistency checks:")
    print("Same delivery order across replicas:", same_order)
    print("Same final store across replicas:", same_state)


def Part_B():
    network = Network()
    replica_ids = [1, 2, 3]
    replicas = {}

    for rid in replica_ids:
        replica = Replica(rid, replica_ids, network)
        replicas[rid] = replica
        network.register(replica)

    counter = 1
    print("1. Concurrent Conflicting Updates\n")
    for i in range(10):
        rid = random.choice(replica_ids)

        # FORCE conflict: same key "x"
        op_type = random.choice(["put", "append"])

        if op_type == "put":
            op = ("put", "x", random.randint(1, 10))
        else:
            op = ("append", "x", random.choice(["A", "B"]))

        replicas[rid].client_update(f"u{counter}", op)
        counter += 1

    print("2. High Contention\n")
    for i in range(25):
        rid = random.choice(replica_ids)
        op = ("append", "x", random.choice(["A", "B"]))

        replicas[rid].client_update(f"u{counter}", op)
        counter += 1

    print("3. Non-conflicting Updates")
    keys = ["X", "Y", "Z"]

    for i in range(10):
        rid = random.choice(replica_ids)
        key = random.choice(keys)
        op = ("put", key, random.randint(1, 10))

        replicas[rid].client_update(f"u{counter}", op)
        counter += 1

    network.run()

    print("Final States:")
    for rid in replica_ids:
        print(replicas[rid].summary())

    first_log = replicas[1].delivery_log
    first_store = replicas[1].store

    same_order = all(replicas[rid].delivery_log == first_log for rid in replica_ids)
    same_state = all(replicas[rid].store == first_store for rid in replica_ids)

    print("\nConsistency checks:")
    print("Same delivery order across replicas:", same_order)
    print("Same final store across replicas:", same_state)



if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", "experiment1.txt")
    with open(log_path, "w", encoding="utf-8") as log_file:
        original_stdout = sys.stdout
        sys.stdout = Tee(sys.stdout, log_file)
        try:
            Part_B()
        finally:
            sys.stdout = original_stdout
    print(f"Run complete. Log written to {log_path}")
