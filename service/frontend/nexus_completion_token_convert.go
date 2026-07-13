package frontend

import (
	"errors"
	"fmt"
	"strconv"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/components/nexusoperations"
)

// chasmWorkflowOperationsField is the name of the CHASM Workflow component field holding the
// Nexus operations, keyed by scheduled event ID (see chasm/lib/workflow/workflow.go). It is the
// first segment of a Nexus operation component's path; the scheduled event ID is the second.
const chasmWorkflowOperationsField = "Operations"

// convertCompletionToOtherFramework re-addresses a Nexus operation completion token to the
// framework opposite the one it currently targets (HSM <-> CHASM).
//
// A rebuild (reset or conflict resolution) can rebuild an operation as either framework based on
// dynamic config at the time — an intentional escape hatch to roll a workflow's operations back
// (CHASM -> HSM) or forward (HSM -> CHASM). The callback token, minted when the operation was
// created, then addresses the wrong framework and the completion is not found. Converting the
// token lets the completion be retried against the framework the operation now lives in.
//
// The two token variants share the identity fields needed to locate the operation — namespace,
// workflow (business) ID, run ID, scheduled event ID, and request ID. The scheduled event ID is
// recovered from the source ref's path and re-encoded into the target ref. Versioned-transition /
// staleness fields are intentionally omitted: both completion handlers resolve the operation by
// request ID after a rebuild (see the run fallback in components/nexusoperations/completion.go and
// the RefConsistencyLevel fallback in CompleteNexusOperationChasm).
func convertCompletionToOtherFramework(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	if len(completion.GetComponentRef()) > 0 {
		return chasmToHSMCompletion(completion)
	}
	return hsmToChasmCompletion(completion)
}

// hsmToChasmCompletion builds a CHASM-addressed completion from an HSM-addressed one.
func hsmToChasmCompletion(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	scheduledEventID, err := scheduledEventIDFromHSMRef(completion.GetRef())
	if err != nil {
		return nil, err
	}
	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   completion.GetNamespaceId(),
		BusinessId:    completion.GetWorkflowId(),
		RunId:         completion.GetRunId(),
		ArchetypeId:   chasm.WorkflowArchetypeID,
		ComponentPath: []string{chasmWorkflowOperationsField, strconv.FormatInt(scheduledEventID, 10)},
	}).Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal component ref: %w", err)
	}
	return &tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    completion.GetRequestId(),
	}, nil
}

// chasmToHSMCompletion builds an HSM-addressed completion from a CHASM-addressed one.
func chasmToHSMCompletion(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	var componentRef persistencespb.ChasmComponentRef
	if err := componentRef.Unmarshal(completion.GetComponentRef()); err != nil {
		return nil, fmt.Errorf("failed to unmarshal component ref: %w", err)
	}
	scheduledEventID, err := scheduledEventIDFromComponentPath(componentRef.GetComponentPath())
	if err != nil {
		return nil, err
	}
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: componentRef.GetNamespaceId(),
		WorkflowId:  componentRef.GetBusinessId(),
		RunId:       componentRef.GetRunId(),
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: nexusoperations.OperationMachineType,
				Id:   strconv.FormatInt(scheduledEventID, 10),
			}},
			// Non-nil zero versioned transitions: the HSM completion handler's reset fallback zeroes
			// their transition counts (components/nexusoperations/completion.go), which nil-panics if
			// either is unset.
			MachineInitialVersionedTransition:    &persistencespb.VersionedTransition{},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
		},
		RequestId: completion.GetRequestId(),
	}, nil
}

// scheduledEventIDFromHSMRef extracts the scheduled event ID from an HSM operation ref, whose path
// addresses the operation state machine by Type=OperationMachineType, ID=<scheduledEventID>.
func scheduledEventIDFromHSMRef(ref *persistencespb.StateMachineRef) (int64, error) {
	for _, key := range ref.GetPath() {
		if key.GetType() != nexusoperations.OperationMachineType {
			continue
		}
		scheduledEventID, err := strconv.ParseInt(key.GetId(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid operation state machine id %q: %w", key.GetId(), err)
		}
		return scheduledEventID, nil
	}
	return 0, errors.New("completion token has no operation state machine reference")
}

// scheduledEventIDFromComponentPath extracts the scheduled event ID from a CHASM operation
// component path of the form ["Operations", "<scheduledEventID>"].
func scheduledEventIDFromComponentPath(path []string) (int64, error) {
	if len(path) != 2 || path[0] != chasmWorkflowOperationsField {
		return 0, fmt.Errorf("unexpected nexus operation component path %v", path)
	}
	scheduledEventID, err := strconv.ParseInt(path[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid scheduled event id %q: %w", path[1], err)
	}
	return scheduledEventID, nil
}
