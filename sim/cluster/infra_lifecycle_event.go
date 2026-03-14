// infra_lifecycle_event.go defines DES events and drain policies for node and
// instance lifecycle transitions. Phase 1A.
package cluster

import (
	"container/heap"

	"github.com/sirupsen/logrus"
)

// ─── Node lifecycle events ──────────────────────────────────────────────────

// NodeReadyEvent fires when a provisioning delay elapses and a node becomes Ready.
// Priority 0: processed before admission and routing events at the same timestamp.
type NodeReadyEvent struct {
	timestamp int64
	seqID     int64
	nodeID    string
}

func (e *NodeReadyEvent) Timestamp() int64 { return e.timestamp }
func (e *NodeReadyEvent) Priority() int    { return 0 }

// Execute transitions the node Provisioning → Ready and retries any pending instances.
func (e *NodeReadyEvent) Execute(cs *ClusterSimulator) {
	if cs.placement == nil {
		return
	}
	if err := cs.placement.MarkNodeReady(e.nodeID); err != nil {
		// Node may have been terminated before becoming ready — not a fatal error
		return
	}

	// Retry pending instances that may now fit on the newly-ready node.
	placed := cs.placement.RetryPendingInstances()
	for _, p := range placed {
		// Find the InstanceSimulator for this pending instance and start loading.
		for _, inst := range cs.instances {
			if inst.ID() == p.id {
				inst.TransitionTo(InstanceStateLoading)
				cs.scheduleInstanceLoadedEvent(inst)
				break
			}
		}
	}
}

// NodeDrainedEvent fires when a draining node has no more allocated instances.
// Priority 0: node lifecycle before routing events.
type NodeDrainedEvent struct {
	timestamp int64
	seqID     int64
	nodeID    string
}

func (e *NodeDrainedEvent) Timestamp() int64 { return e.timestamp }
func (e *NodeDrainedEvent) Priority() int    { return 0 }

// Execute transitions the node Draining → Terminated and releases GPU inventory.
func (e *NodeDrainedEvent) Execute(cs *ClusterSimulator) {
	if cs.placement == nil {
		return
	}
	_ = cs.placement.MarkNodeTerminated(e.nodeID) // ignore error (defensive)
}

// ─── Instance lifecycle events ──────────────────────────────────────────────

// InstanceLoadedEvent fires when an instance finishes loading model weights.
// Priority 1: instance lifecycle after node events but before routing events.
type InstanceLoadedEvent struct {
	timestamp  int64
	seqID      int64
	instanceID InstanceID
}

func (e *InstanceLoadedEvent) Timestamp() int64 { return e.timestamp }
func (e *InstanceLoadedEvent) Priority() int    { return 1 }

// Execute transitions the instance Loading → WarmingUp (or Active if no warm-up configured).
func (e *InstanceLoadedEvent) Execute(cs *ClusterSimulator) {
	for _, inst := range cs.instances {
		if inst.ID() == e.instanceID {
			warmUpCount := cs.config.InstanceLifecycle.WarmUpRequestCount
			if warmUpCount <= 0 {
				// Skip WarmingUp phase entirely — go directly to Active
				inst.TransitionTo(InstanceStateActive)
			} else {
				inst.TransitionTo(InstanceStateWarmingUp)
			}
			return
		}
	}
}

// ─── scheduleInstanceLoadedEvent ────────────────────────────────────────────

// scheduleInstanceLoadedEvent schedules an InstanceLoadedEvent for inst based on
// the configured loading delay. If the delay is zero, transitions immediately to
// Loading → WarmingUp (or Active).
func (cs *ClusterSimulator) scheduleInstanceLoadedEvent(inst *InstanceSimulator) {
	delay := cs.placement.SampleLoadingDelay(&cs.config.InstanceLifecycle)
	readyTime := cs.clock + delay

	if delay == 0 {
		// No delay — fire event inline at current clock
		warmUpCount := cs.config.InstanceLifecycle.WarmUpRequestCount
		if warmUpCount <= 0 {
			inst.TransitionTo(InstanceStateActive)
		} else {
			inst.TransitionTo(InstanceStateWarmingUp)
		}
		return
	}

	heap.Push(&cs.clusterEvents, clusterEventEntry{
		event: &InstanceLoadedEvent{
			timestamp:  readyTime,
			instanceID: inst.ID(),
		},
		seqID: cs.nextSeqID(),
	})
}

// ─── DrainPolicy interface and implementations ──────────────────────────────

// DrainPolicy defines the behavior when an instance is drained. (R13: ≥2 implementations)
//
// Extension recipe — adding a new drain policy:
//  1. Add a new DrainPolicyName constant in infra_config.go (e.g. DrainPolicyGraceful).
//  2. Add it to the validDrainPolicies map in infra_config.go.
//  3. Implement the DrainPolicy interface (this file) as a new unexported struct.
//  4. Add a case to NewDrainPolicy() below.
//  5. Add a test in instance_lifecycle_test.go.
type DrainPolicy interface {
	// Drain initiates drain on the given instance within the cluster simulation.
	Drain(inst *InstanceSimulator, cs *ClusterSimulator)
}

// NewDrainPolicy returns the DrainPolicy implementation for the named policy.
// Panics on unknown policy name (constructor invariant per Principle V).
func NewDrainPolicy(name DrainPolicyName) DrainPolicy {
	switch name {
	case DrainPolicyImmediate:
		return &drainImmediate{}
	case DrainPolicyWait:
		return &drainWait{}
	case DrainPolicyRedirect:
		return &drainRedirect{}
	default:
		panic("NewDrainPolicy: unknown policy " + string(name))
	}
}

// drainImmediate terminates the instance immediately.
// In-flight requests receive no further steps; they complete with whatever progress
// they have made. New requests are no longer routed to the instance.
type drainImmediate struct{}

func (d *drainImmediate) Drain(inst *InstanceSimulator, cs *ClusterSimulator) {
	inst.TransitionTo(InstanceStateDraining)
	inst.TransitionTo(InstanceStateTerminated)
	cs.releaseInstanceGPUs(inst)
}

// drainWait stops routing new requests to the instance and waits for in-flight
// requests to complete before transitioning to Terminated.
type drainWait struct{}

func (d *drainWait) Drain(inst *InstanceSimulator, cs *ClusterSimulator) {
	inst.TransitionTo(InstanceStateDraining)
	// Routing exclusion handled by buildRouterState() via inst.IsRoutable().
	// Termination handled by the simulation completion accounting (see cluster.go).
}

// drainRedirect stops routing new requests and re-injects all queued (not yet
// scheduled) requests as new ClusterArrivalEvents so they can be routed elsewhere.
type drainRedirect struct{}

func (d *drainRedirect) Drain(inst *InstanceSimulator, cs *ClusterSimulator) {
	inst.TransitionTo(InstanceStateDraining)

	// Extract queued requests from the instance WaitQ and re-inject into the cluster.
	// Simulation simplification: re-injected at current clock (cs.clock), not original ArrivalTime.
	// The request's ArrivalTime field is preserved, so E2E latency correctly accounts for the
	// full wait time including the pre-drain queue time. INV-5 (arrival ≤ enqueue ≤ schedule ≤
	// completion) is preserved because the new ClusterArrivalEvent fires at cs.clock ≥ ArrivalTime.
	queued := inst.DrainWaitQueue()
	for _, req := range queued {
		heap.Push(&cs.clusterEvents, clusterEventEntry{
			event: &ClusterArrivalEvent{time: cs.clock, request: req},
			seqID: cs.nextSeqID(),
		})
	}
}

// ─── Helper: releaseInstanceGPUs ────────────────────────────────────────────

// releaseInstanceGPUs releases the GPU allocations for a terminated instance.
// Logs a warning if the release fails (instance was not placed — expected for Scheduling state).
func (cs *ClusterSimulator) releaseInstanceGPUs(inst *InstanceSimulator) {
	if cs.placement == nil {
		return
	}
	if err := cs.placement.ReleaseInstance(inst.ID()); err != nil {
		// Instance may never have been placed (still in Scheduling state) — not a bug.
		// Log at debug level so placement bugs surface during investigation.
		logrus.Debugf("[cluster] releaseInstanceGPUs %s: %v", inst.ID(), err)
	}
}

// ─── scheduleNodeDrainedEvent ────────────────────────────────────────────────

// scheduleNodeDrainedEvent enqueues a NodeDrainedEvent at the current clock.
// Called as the drain callback when the last instance on a draining node releases.
func (cs *ClusterSimulator) scheduleNodeDrainedEvent(nodeID string) {
	heap.Push(&cs.clusterEvents, clusterEventEntry{
		event: &NodeDrainedEvent{
			timestamp: cs.clock,
			nodeID:    nodeID,
		},
		seqID: cs.nextSeqID(),
	})
}

// ─── scheduleNodeReadyEvent ──────────────────────────────────────────────────

// scheduleNodeReadyEvent enqueues a NodeReadyEvent at readyTime.
func (cs *ClusterSimulator) scheduleNodeReadyEvent(nodeID string, readyTime int64) {
	heap.Push(&cs.clusterEvents, clusterEventEntry{
		event: &NodeReadyEvent{
			timestamp: readyTime,
			nodeID:    nodeID,
		},
		seqID: cs.nextSeqID(),
	})
}

