# Part A — Total-Order Multicast with Lamport Clocks

## Overview
This project implements a simulator for total-order multicast in a replicated key–value store.
The goal is to ensure that when clients send update operations to different replicas, all replicas still deliver and apply updates in the same total order, even if the updates are concurrent.

The implementation uses:
- Lamport logical clocks
- deterministic tie-breaking by replica ID
- a holdback queue** ordered by (timestamp, replica_id)
- ACK/progress messages
- a delivery rule based on max_seen[k] > head.ts for all replicas

## Files
- Part_A.py — main simulator for Part A
- README.md — instructions and explanation
- logs/ — captured terminal output for required experiments

## How to Run
### Requirements
Python 3.x

### Run command
python3 Part_A.py

# Part C - Short Written Questions 
## 1. 
Replication needs total ordering because conflicting operations can produce different results if they are applied in different orders. For example, if two replicas receive concurrent updates append(x, "A") and append(x, "B"), one replica might produce "AB" while another produces "BA". A global total order ensures all replicas apply updates in the same sequence, so they remain consistent.
## 2. 
Lamport clocks guarantee a consistent ordering of events that respects causality. If event A happened before event B, then a timestamp(A) < timestamp(B). However, they do not reflect real-time ordering, so two concurrent events may be ordered arbitrarily. By themselves, Lamport clocks provide only a partial order, but when combined with a tie-breaker (like replica ID), they can be used to define a total order.
## 3.
If our FIFO communication isn’t managed correctly and is unreliable, messages can get lost during transmissions and total-order is lost. Messages being lost or not properly sent is going to prevent replicas from receiving and applying the same updates as others. This is going to have replicas end up with differing states, which breaks system consistency and overall correctness of our program.
## 4.
The coordination happens mainly within the middleware layer of our program. Specifically, the implementation of our Replica class is going to manage Lamport clocks, a holdback queue, ACK tracking and the delivery rules that each replica follows. These mechanisms all work to ensure that all replicas agree on a global total order before applying any updates. The application logic is only used for applying updates after they are delivered but doesn’t enforce any of the coordination itself. 
