# NativeLog 

NativeLog is a home-grown distributed log which adopts lots concepts from Apache Kafka, Pulsar etc systems, but implemented 
in a very light-weight by compromising fewer features. 

# Develop Guideline 

NativeLog shall not depend on application related code of `proton` in general but can depend on its third party libraries 
and `base`, `Common`, `IO` libs. For `base`, `Common` and `IO`, we will NOT create any wrapper layer for now due to complexity and performance concerns.
However since these components are relatively standalone, in future if we like to spin out NativeLog and make it buildable as standalone application,
it is still doable.

The design chooses to returning error code or setting up thread local error code `err = XXX` for a function instead of throw exception in low level code. 
Exception can still be used in constructors, very high level code like env setup, application argument parsing, main entry point etc code. We are still 
using Proton's ErrorCodes, logger etc low facility to make error handling, logging and propagation consistent.

# Goals
- Simple instance simple.
- Distributed env, replicated.
- Can horizontally scale out for one shard 
- Write availability over read availability 
- Throughput over low latency, but tuned.
- Tail latency (99p) shall be within 100 ~ 500 milliseconds
- Zero serde for Records
- Can be disaggregated or converged
- No external dependency

# Concepts
**Record** : 

**Record Batch** : 

**Segment** :

**Shard** :

**Topic** :

**Index** :

**TimeIndex** :

**Namespace** :

**Sequencer** :

**MetaStore** :

# APIs

## Record

## Client APIs

# On Disk Layout

# Milestone Checklists
- [ ] Singe instance
    - [ ] Single instance sequencer 
    - [ ] Single instance metastore
    - [ ] Single instance InmemoryLoglet
    - [ ] Single instance NativeLoglet
    
- [ ] Cluster
    - [ ] Multiple shards 
    - [ ] Shard replication
