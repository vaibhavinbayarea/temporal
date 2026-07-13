package frontend

import (
	"context"
	"strconv"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/nexusoperations"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	convTestNamespaceID      = "namespace-id"
	convTestWorkflowID       = "workflow-id"
	convTestRunID            = "run-id"
	convTestRequestID        = "request-id"
	convTestScheduledEventID = int64(42)
)

// hsmCompletionToken builds an HSM-addressed completion token (top-level identity fields + a
// StateMachineRef, no ComponentRef).
func hsmCompletionToken() *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: convTestNamespaceID,
		WorkflowId:  convTestWorkflowID,
		RunId:       convTestRunID,
		RequestId:   convTestRequestID,
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: nexusoperations.OperationMachineType,
				Id:   strconv.FormatInt(convTestScheduledEventID, 10),
			}},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 3, TransitionCount: 7},
		},
	}
}

// chasmCompletionToken builds a CHASM-addressed completion token (a serialized ChasmComponentRef,
// no top-level HSM fields).
func chasmCompletionToken(t *testing.T) *tokenspb.NexusOperationCompletion {
	t.Helper()
	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   convTestNamespaceID,
		BusinessId:    convTestWorkflowID,
		RunId:         convTestRunID,
		ArchetypeId:   chasm.WorkflowArchetypeID,
		ComponentPath: []string{chasmWorkflowOperationsField, strconv.FormatInt(convTestScheduledEventID, 10)},
	}).Marshal()
	require.NoError(t, err)
	return &tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    convTestRequestID,
	}
}

func TestConvertCompletion_HSMToCHASM(t *testing.T) {
	t.Parallel()

	converted, err := convertCompletionToOtherFramework(hsmCompletionToken())
	require.NoError(t, err)

	// The converted token is a clean CHASM token: a ComponentRef and request ID, no HSM fields.
	require.Equal(t, convTestRequestID, converted.GetRequestId())
	require.Empty(t, converted.GetNamespaceId())
	require.Empty(t, converted.GetWorkflowId())
	require.Empty(t, converted.GetRunId())
	require.Nil(t, converted.GetRef())

	var ref persistencespb.ChasmComponentRef
	require.NoError(t, ref.Unmarshal(converted.GetComponentRef()))
	require.Equal(t, convTestNamespaceID, ref.GetNamespaceId())
	require.Equal(t, convTestWorkflowID, ref.GetBusinessId())
	require.Equal(t, convTestRunID, ref.GetRunId())
	require.Equal(t, chasm.WorkflowArchetypeID, ref.GetArchetypeId())
	require.Equal(t, []string{chasmWorkflowOperationsField, strconv.FormatInt(convTestScheduledEventID, 10)}, ref.GetComponentPath())
}

func TestConvertCompletion_CHASMToHSM(t *testing.T) {
	t.Parallel()

	converted, err := convertCompletionToOtherFramework(chasmCompletionToken(t))
	require.NoError(t, err)

	// The converted token is a clean HSM token: identity fields + a StateMachineRef, no ComponentRef.
	require.Equal(t, convTestRequestID, converted.GetRequestId())
	require.Equal(t, convTestNamespaceID, converted.GetNamespaceId())
	require.Equal(t, convTestWorkflowID, converted.GetWorkflowId())
	require.Equal(t, convTestRunID, converted.GetRunId())
	require.Empty(t, converted.GetComponentRef())

	require.Len(t, converted.GetRef().GetPath(), 1)
	key := converted.GetRef().GetPath()[0]
	require.Equal(t, nexusoperations.OperationMachineType, key.GetType())
	require.Equal(t, strconv.FormatInt(convTestScheduledEventID, 10), key.GetId())
	// Non-nil so the HSM handler's reset fallback can zero the transition counts without panicking.
	require.NotNil(t, converted.GetRef().GetMachineInitialVersionedTransition())
	require.NotNil(t, converted.GetRef().GetMachineLastUpdateVersionedTransition())
}

func TestConvertCompletion_RoundTripPreservesIdentity(t *testing.T) {
	t.Parallel()

	// HSM -> CHASM -> HSM preserves the identity fields.
	toChasm, err := convertCompletionToOtherFramework(hsmCompletionToken())
	require.NoError(t, err)
	backToHSM, err := convertCompletionToOtherFramework(toChasm)
	require.NoError(t, err)
	require.Equal(t, convTestNamespaceID, backToHSM.GetNamespaceId())
	require.Equal(t, convTestWorkflowID, backToHSM.GetWorkflowId())
	require.Equal(t, convTestRunID, backToHSM.GetRunId())
	require.Equal(t, convTestRequestID, backToHSM.GetRequestId())
	require.Equal(t, strconv.FormatInt(convTestScheduledEventID, 10), backToHSM.GetRef().GetPath()[0].GetId())
}

func TestConvertCompletion_Errors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		token *tokenspb.NexusOperationCompletion
	}{
		{
			name:  "HSM ref without an operation key",
			token: &tokenspb.NexusOperationCompletion{NamespaceId: convTestNamespaceID, RequestId: convTestRequestID},
		},
		{
			name: "HSM ref with a non-numeric operation id",
			token: &tokenspb.NexusOperationCompletion{
				NamespaceId: convTestNamespaceID,
				RequestId:   convTestRequestID,
				Ref: &persistencespb.StateMachineRef{Path: []*persistencespb.StateMachineKey{{
					Type: nexusoperations.OperationMachineType,
					Id:   "not-a-number",
				}}},
			},
		},
		{
			name:  "CHASM ref with a malformed component path",
			token: chasmTokenWithPath(t, []string{"Operations"}),
		},
		{
			name:  "CHASM ref addressing a non-operations path",
			token: chasmTokenWithPath(t, []string{"Callbacks", "1"}),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertCompletionToOtherFramework(tc.token)
			require.Error(t, err)
		})
	}
}

func chasmTokenWithPath(t *testing.T, path []string) *tokenspb.NexusOperationCompletion {
	t.Helper()
	ref, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   convTestNamespaceID,
		BusinessId:    convTestWorkflowID,
		RunId:         convTestRunID,
		ArchetypeId:   chasm.WorkflowArchetypeID,
		ComponentPath: path,
	}).Marshal()
	require.NoError(t, err)
	return &tokenspb.NexusOperationCompletion{ComponentRef: ref, RequestId: convTestRequestID}
}

func TestCompleteOperation_FrameworkFallback(t *testing.T) {
	t.Parallel()

	notFound := serviceerror.NewNotFound("operation not found")
	internalErr := serviceerror.NewInternal("boom")

	testCases := []struct {
		name string
		// chasmDisabled models a CHASM-disabled namespace; it suppresses the HSM->CHASM fallback.
		chasmDisabled bool
		token         func(t *testing.T) *tokenspb.NexusOperationCompletion
		setupClient   func(t *testing.T, client *historyservicemock.MockHistoryServiceClient)
		wantErr       bool
	}{
		{
			name:  "HSM primary succeeds, no fallback",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
					Return(&historyservice.CompleteNexusOperationResponse{}, nil)
			},
		},
		{
			name:  "HSM primary NotFound converts to CHASM and succeeds",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
						Return(nil, notFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationChasmRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
							// The fallback request must carry the converted CHASM ref.
							require.NotEmpty(t, req.GetCompletion().GetComponentRef())
							require.Equal(t, convTestRequestID, req.GetCompletion().GetRequestId())
							return &historyservice.CompleteNexusOperationChasmResponse{}, nil
						}),
				)
			},
		},
		{
			name:  "CHASM primary NotFound converts to HSM and succeeds",
			token: chasmCompletionToken,
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).
						Return(nil, notFound),
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, req *historyservice.CompleteNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.CompleteNexusOperationResponse, error) {
							require.Equal(t, strconv.FormatInt(convTestScheduledEventID, 10), req.GetCompletion().GetRef().GetPath()[0].GetId())
							return &historyservice.CompleteNexusOperationResponse{}, nil
						}),
				)
			},
		},
		{
			name:          "no HSM to CHASM fallback when chasm disabled for namespace",
			chasmDisabled: true,
			token:         func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound)
			},
			wantErr: true,
		},
		{
			name: "no fallback when token has no request ID",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion {
				token := hsmCompletionToken()
				token.RequestId = ""
				return token
			},
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound)
			},
			wantErr: true,
		},
		{
			name:  "no fallback on non-NotFound error",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, internalErr)
			},
			wantErr: true,
		},
		{
			name:  "both frameworks NotFound returns NotFound",
			token: func(*testing.T) *tokenspb.NexusOperationCompletion { return hsmCompletionToken() },
			setupClient: func(t *testing.T, client *historyservicemock.MockHistoryServiceClient) {
				gomock.InOrder(
					client.EXPECT().CompleteNexusOperation(gomock.Any(), gomock.Any()).Return(nil, notFound),
					client.EXPECT().CompleteNexusOperationChasm(gomock.Any(), gomock.Any()).Return(nil, notFound),
				)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := historyservicemock.NewMockHistoryServiceClient(ctrl)
			tc.setupClient(t, client)

			h := &nexusCompletionHandler{HistoryClient: client}
			req := &nexusrpc.CompletionRequest{State: nexus.OperationStateSucceeded, OperationToken: "operation-token"}

			err := h.completeOperation(context.Background(), log.NewNoopLogger(), tc.token(t), &commonpb.Payload{}, req, nil, !tc.chasmDisabled)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
