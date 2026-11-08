package mcp

import (
	"context"

	"github.com/keejkrej/g0code/orchestration"
)

type PendingRequestInput struct {
	ThreadID        string         `json:"threadId"`
	Action          string         `json:"action"`
	RequestID       string         `json:"requestId,omitempty"`
	Decision        *string        `json:"decision,omitempty"`
	Feedback        *string        `json:"feedback,omitempty"`
	Answers         map[string]any `json:"answers,omitempty"`
	ClientRequestID string         `json:"clientRequestId,omitempty"`
}

func (s *Server) pendingRequest(ctx context.Context, input PendingRequestInput) (any, *Failure) {
	snapshot, err := s.orchestrator.Snapshot(ctx, orchestration.ThreadID(input.ThreadID))
	if err != nil {
		return nil, failureFromError(err)
	}
	switch input.Action {
	case "list":
		requests := make([]orchestration.RuntimeRequest, 0)
		for _, request := range snapshot.Projection.RuntimeRequests {
			if request.Status == orchestration.RequestPending {
				requests = append(requests, request)
			}
		}
		return map[string]any{"threadId": input.ThreadID, "requests": requests}, nil
	case "read":
		request, ok := snapshot.Projection.RuntimeRequest(orchestration.RuntimeRequestID(input.RequestID))
		if !ok {
			return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "invalid_request", Message: "request not found: " + input.RequestID}
		}
		return map[string]any{"threadId": input.ThreadID, "request": request}, nil
	case "respond":
		key := requestKeyOrRandom(input.ClientRequestID)
		result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandRuntimeRequestRespond,
			CommandID: orchestration.CommandID(scopedID("command:mcp", key, "respond")), ThreadID: orchestration.ThreadID(input.ThreadID),
			Payload: orchestration.RuntimeRequestResponse{RequestID: orchestration.RuntimeRequestID(input.RequestID),
				Decision: input.Decision, Feedback: input.Feedback, Answers: input.Answers}})
		if err != nil {
			return nil, failureFromError(err)
		}
		s.runEffects(result.Effects)
		return map[string]any{"threadId": input.ThreadID, "requestId": input.RequestID, "sequence": result.Sequence}, nil
	default:
		return nil, invalidRequest("action must be list, read, or respond")
	}
}
