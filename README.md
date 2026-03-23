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
```bash
python3 Part_A.py
