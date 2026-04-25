#import "@preview/starter-journal-article:0.5.1": article, author-meta

#show: article.with(
  title: [
    A Dynamo-Style Key-Value Store with
    Probabilistically Bounded Staleness Evaluation
  ],
  authors: (
    "Yulin Ding": author-meta(
      "NYU",
      email: "yd3442@nyu.edu",
    ),
  ),
  affiliations: (
    "NYU": "Courant Institute of Mathematical Sciences, New York University, 251 Mercer St, New York, NY 10012, USA",
  ),
  abstract: [
    // TODO: ~150-200 words. One sentence per bullet.
    // - problem: highly-available KV stores trade strict consistency for
    //   availability; quantifying the resulting staleness is hard.
    // - contribution 1: build a Dynamo-style KV store in Go implementing
    //   consistent hashing, vector-clock siblings, sloppy quorums, read
    //   repair, hinted handoff, and Merkle-tree anti-entropy.
    // - contribution 2: instrument the system with a PBS (Bailis et al.,
    //   2012) evaluation framework producing #box[$Delta$]-P and k-P curves,
    //   comparing empirical measurements against a Monte-Carlo WARS
    //   predictor.
    // - contribution 3: use the instrumentation to find and fix two
    //   non-trivial consistency bugs in read-repair and anti-entropy.
    // - contribution 4: characterise the effect of $(N,R,W)$, the network
    //   reliability regime, and each convergence mechanism on observed
    //   staleness.
    #lorem(120)
  ],
  keywords: (
    "Distributed Systems",
    "Dynamo",
    "Eventual Consistency",
    "Probabilistically Bounded Staleness",
    "Vector Clocks",
    "Quorum Replication",
  )
)

// =============================================================================
= Introduction
// =============================================================================

// TODO: 1-1.5 pages.
// 1. Motivate: AP systems (Dynamo, Cassandra, Riak) give up linearizability
//    for availability; how stale is a "typical" read? Operators need
//    data-driven answers, not asymptotic ones.
// 2. Gap: most course-level Dynamo clones stop at "implements quorums and
//    hinted handoff"; few quantify the resulting staleness or validate
//    convergence mechanisms empirically.
// 3. Contributions (enumerate 4, mirror the abstract bullets).
// 4. Roadmap paragraph: "Section 2 ... Section 3 ... etc."

#lorem(60) @dynamo2007 @pbs2012

== Contributions

// TODO: 3-4 bulleted contributions, each one sentence, concrete.
- A from-scratch Go implementation of a Dynamo-style key-value store
  covering consistent hashing with virtual nodes, vector-clock sibling
  reconciliation, sloppy quorums, hinted handoff, read repair, and
  Merkle-tree-based anti-entropy, on top of the MIT 6.5840 `labrpc`
  simulation fabric.
- An open-source PBS evaluation framework that samples W/A/R/S message
  latencies at runtime, fits empirical distributions, and drives a
  Monte-Carlo predictor whose output is plotted alongside measured
  $Delta$-P and k-P curves.
- A pair of case-studies where the PBS harness surfaced subtle
  consistency bugs in the repair path that unit tests had missed.
- An empirical study sweeping replication degree, quorum sizes, network
  reliability, and the four convergence mechanisms
  (baseline / read repair / anti-entropy / hinted handoff).

// =============================================================================
= Background and Related Work
// =============================================================================

== Dynamo-Style Key-Value Stores

// TODO: ~0.5 page. Summarise the key Dynamo design points you actually
// implemented and their origins:
// - consistent hashing with virtual nodes (Karger et al. @karger1997)
// - sloppy quorum with $W + R > N$ as a "configurable" knob
// - vector clocks for causality (Fidge @fidge1988, Mattern @mattern1988)
// - hinted handoff for short-term failures
// - Merkle trees for anti-entropy
// - application-level reconciliation versus last-write-wins
#lorem(40) @dynamo2007 @cassandra2010

== Probabilistically Bounded Staleness

// TODO: ~0.5 page.
// - Bailis et al. @pbs2012 introduce two families of consistency metrics:
//     * $Delta$-regularity: probability that a read at time $t$ sees a value
//       written at least $Delta$ time ago.
//     * $k$-regularity: probability that a read returns one of the k most
//       recent committed values.
// - The WARS model samples four latency classes (Write request, Ack, Read
//   request, Response) and Monte-Carlo-simulates per-read staleness.
// - Clarify the "eventually consistent vs. always stale" distinction.
#lorem(50) @pbs2012

== Convergence Mechanisms

// TODO: briefly compare the three mechanisms you implemented.
- *Read repair* piggybacks on reads: after any Get completes, the
  coordinator asynchronously pushes the merged sibling set to replicas
  whose responses diverged from it.
- *Anti-entropy* uses Merkle trees over key sectors so nodes can gossip
  only the buckets whose sub-tree hashes disagree.
- *Hinted handoff* stores writes destined for a temporarily unreachable
  replica at a surrogate node, replaying them once the target heals.

// =============================================================================
= System Design
// =============================================================================

// TODO: ~2-3 pages total. Include at least one architecture diagram
// (preference list + coordinator) and one Merkle-tree sketch.

== Architecture Overview

// TODO: describe request flow. Figure: "Coordinator routes a Put to the
// preference list of the key; first W acks commit the write; background
// traffic reaches the remaining replicas."
#lorem(60)

== Consistent Hashing and the Preference List

// TODO: describe how you implemented the `chr` package:
// - `numSectors`, `bucketsPerSector`, virtual-node placement
// - `GetCoordinator(key)` and `GetPreferenceList(key)`
// - why virtual nodes matter for load balance and failure handling
// Include a tiny worked example: N=3, sectors=8, show a key's path.
#lorem(70) @karger1997

== Versioning and Sibling Reconciliation

// TODO: ~0.75 page. Crucial section.
// - Context = (vector clock, wall-clock timestamp).
// - Comparison returns {Before, Equal, After, Concurrent}.
// - On write: `CanBeAddedTo(siblings)` gates admission; concurrent writes
//   become siblings.
// - Default client policy: last-writer-wins by timestamp (LWW) as a
//   convenience wrapper, but the full sibling list is exposed so
//   applications can supply their own reconciler (Dynamo-style).
// - Briefly mention the shopping-cart example as motivation for keeping
//   siblings rather than silently LWW-merging.
#lorem(80) @fidge1988 @mattern1988

== Quorum Configuration

// TODO:
// - $N$: replication factor (size of preference list)
// - $W$: number of ack-ing replicas required to commit a write
// - $R$: number of replicas required to complete a read
// - strict quorum: $W + R > N$ implies the read-write set intersects.
// - sloppy quorum: $W + R <= N$, used in our PBS experiments.
#lorem(50)

== Read Repair

// TODO: describe the exact algorithm you implemented, including the
// subtle bug you fixed. Include pseudo-code block.
// Algorithm:
// 1. Coordinator forwards Get to all N replicas.
// 2. Returns to client after first R responses.
// 3. Background goroutine gathers remaining responses, computes
//    canonicalSiblings via causal merge.
// 4. Diverging replicas receive RepairPut(canonicalSiblings).
// 5. Replica merges incoming siblings via `mergeObjects`, using
//    `CanBeAddedTo` to guarantee causal monotonicity (never rollback).
// KEY CASE STUDY: the original implementation used installObjects
// (unconditional overwrite) which corrupted replicas under concurrent
// writes. See Section 5.2.
#lorem(80)

== Anti-Entropy via Merkle Trees

// TODO: describe how each replica maintains one Merkle root per sector,
// refreshed after every write; how two replicas compare roots over
// gossip and walk the tree to find divergent buckets; then call
// RepairPut for each differing key. Include a small figure.
#lorem(80)

== Hinted Handoff

// TODO: explain how the coordinator picks a handoff node when a
// preference-list member is "dead" per the failure detector, and how
// the hint is eventually replayed by a per-node replay loop.
#lorem(60)

== Membership and Failure Detection

// TODO: describe the gossip-based phi-style accrual-ish detector:
// heartbeat counter, `failureTimeout`, `cleanupTimeout`, Suspect/Dead
// states, and the `numNeighbors` gossip fan-out.
#lorem(50) @swim2002

// =============================================================================
= Evaluation Framework
// =============================================================================

== WARS Latency Sampling

// TODO: describe `PBSCollector` and the four latency classes captured
// on each ReplicaPut and ReplicaGet:
// - $W$ = coordinator-to-replica request time (ArrivedAt - SentAt)
// - $A$ = replica-to-coordinator ack time (ReceivedAt - RespondedAt)
// - $R$ = coordinator-to-replica read request time
// - $S$ = replica-to-coordinator response time
// These four empirical distributions feed the Monte-Carlo predictor.
#lorem(60) @pbs2012

== Monte-Carlo Predictor

// TODO: explain how `EvaluateDeltaPSweep` Monte-Carlo-samples N WARS
// tuples per trial, determines whether a read at offset $Delta$ "sees"
// the latest write under strict-quorum semantics, and averages over
// `Iterations` trials to get $Pr[consistent | Delta]$.
#lorem(50)

== Empirical Observers

// TODO:
// - $Delta$-P: for each read, check whether its returned sibling set is
//   causally $>=$ some write that committed within $[t_"read start" -
//   Delta, t_"read start"]$; specifically see `isDeltaRegular` in
//   `observer.go`. Average across all reads.
// - k-P: similar, but against the set of k latest committed writes at
//   read-start time, implemented in `isKRegular`.
#lorem(50)

== Comparison Protocol

// TODO: describe each scenario you compare:
// - `observe_baseline`: pure quorum read/write, no repair.
// - `observe_read_repair`: same as baseline, read repair enabled.
// - `observe_anti_entropy`: anti-entropy gossip running in background.
// - `observe_hinted_handoff`: one replica killed before workload;
//   writes go through hinted handoff.
// Same seed; single key; 8 writer + 32 reader goroutines.
#lorem(40)

// =============================================================================
= Case Studies: Bugs Surfaced by the Evaluation
// =============================================================================

// TODO: This section is a strong differentiator — write it carefully.

== Read Repair Overwrites Fresh Writes

// TODO: tell the story.
// 1. Observation: on the $(N=3, R=W=1)$ Delta-P curve, read repair was
//    a factor of 3-4x worse than baseline at small $Delta$ — the
//    opposite of its purpose.
// 2. Hypothesis: the repair path must be rolling replicas backward
//    under concurrent writes.
// 3. Root cause: `RepairPut` called `installObjects`, which
//    unconditionally replaced the replica's sibling list with a
//    snapshot computed when the Get was issued. Any ReplicaPut that
//    landed between the Get snapshot and RepairPut execution was
//    clobbered.
// 4. Fix: `mergeObjects` uses `CanBeAddedTo` + `AddObject` under the
//    replica's mutex, dropping stale repairs and keeping causal
//    monotonicity.
// 5. Result: curve converges to within statistical noise of baseline.
//
// Include the before / after plot as Figure.
#lorem(100)

== Merge Early-Return Mis-detected "Unchanged"

// TODO: tell the second bug.
// 1. After the first fix, read repair still trailed baseline by ~0.03
//    at $Delta = 0$.
// 2. Root cause: `mergeObjects` short-circuited when
//    `len(merged) == len(existing)`, but `AddObject` can drop a
//    dominated sibling and add the new one in the same step, keeping
//    length constant while swapping the content. One-for-one
//    replacements silently dropped.
// 3. Fix: replace length check with a `changed` flag set inside the
//    loop after `AddObject`.
// 4. Result: read repair curve matches or slightly exceeds baseline
//    for $Delta > 0$, as expected.
#lorem(100)

// =============================================================================
= Experimental Results
// =============================================================================

// TODO: ~3-4 pages. At least 4 figures.

== Setup

// TODO: specify hardware, Go version, labrpc network settings, seed,
// workload (1 key, 8 writers, 32 readers, 300 writes per writer,
// ProbeReadsPerWrite=0, SleepBetweenOps=4ms, ReadSleep=2ms, NumNodes=10).
// List which parameters are swept vs. fixed in each experiment.
#lorem(80)

== Reliable Network Baseline

// TODO: Figure 1: Delta-P curve, N=3, R=W=1, reliable network; show
// all 4 observed scenarios + predict_baseline.
// Figure 2: k-P for same config.
// Discuss: under reliable network the transition region is < 5 ms;
// predict_baseline sits systematically below observed — why? (LW
// coupling; the WARS model assumes per-replica independence.)
#lorem(120)

== Quorum Sensitivity

// TODO: Figure 3: Delta-P curves for
//   (W, R) in {(1,1), (1,2), (2,1), (2,2), (1,3), (3,1)} at N=3.
// Fit PBS prediction against empirical. Call out strict-quorum
// ${W+R > N}$ curves that hit P=1 almost instantly.
#lorem(120) @pbs2012

== Unreliable Network

// TODO: Figure 4: same as Figure 1 but with `-unreliable`. Show
// widened transition region (0-15 ms). Highlight that anti-entropy
// under-performs baseline here because its 500 ms period cannot keep
// up with drop-induced divergence.
// Optional: Figure 5 with `-long-reordering` to show extreme tails.
#lorem(100)

== Churn

// TODO: (optional but strong) Figure 6: introduce a node failure at
// $t = 5 s$ and a rejoin at $t = 10 s$. Show Delta-P as a function of
// wall-clock time during the experiment. Quantify how long each
// mechanism takes to recover.
#lorem(80)

== Cost of Convergence Mechanisms

// TODO: Table 1: per-scenario extra RPC counts, CPU time, and
// `write_err_version` / `write_quorum_retry` counters. Highlight
// that read repair is ~2x the background traffic of baseline but
// buys a tighter $Delta = 0$ point; anti-entropy is cheap per cycle
// but has a latency floor of `antiEntropyInterval`.
#lorem(80)

// =============================================================================
= Discussion
// =============================================================================

== Predict vs. Observe Gap

// TODO: why is predict_baseline systematically below observed? Two
// candidates:
// 1. WARS assumes per-replica latency independence; our implementation
//    processes RPCs through a shared `endCh` goroutine, introducing
//    positive correlation.
// 2. The predictor does not model the coordinator reading its local
//    replica first, which enjoys zero network latency.
// Propose a corrected model.
#lorem(100)

== LWW vs. Application Reconciliation

// TODO: discuss the design choice made in `client.go`:
// - Default `Clerk.Get` applies LWW.
// - `Clerk.GetSiblings` exposes the full set so applications can
//   run their own merge (union for shopping carts, max for counters).
// Argue why, for a general KV store, surfacing siblings is more
// defensible even though LWW is the easier API.
#lorem(80)

== Limitations

// TODO: be honest.
// - Simulated network via labrpc, not a real cluster.
// - No persistence; crashes lose state.
// - Single-key PBS workload; multi-key effects not studied.
// - Sibling pruning / version-vector truncation is not implemented.
// - The failure detector is threshold-based, not $phi$-accrual.
#lorem(60)

// =============================================================================
= Conclusion and Future Work
// =============================================================================

// TODO: one paragraph restating contributions, one paragraph pointing
// to realistic extensions (persistence, real TCP, multi-DC, proper
// sibling pruning, linearizability-based correctness tests).
#lorem(120)

// =============================================================================
= Artifact Availability
// =============================================================================

// TODO: link to your repo, tag/commit you submitted. One-line description
// of each script (`kvsrv1pbsplot`, reproduction scripts).
#lorem(30)

#bibliography("./ref.bib")
