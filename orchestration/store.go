package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type ReceiptStatus string

const (
	ReceiptAccepted ReceiptStatus = "accepted"
	ReceiptRejected ReceiptStatus = "rejected"
)

type Receipt struct {
	CommandID      CommandID     `json:"commandId"`
	ThreadID       ThreadID      `json:"threadId"`
	CommandType    CommandType   `json:"commandType"`
	AcceptedAt     time.Time     `json:"acceptedAt"`
	ResultSequence uint64        `json:"resultSequence"`
	Status         ReceiptStatus `json:"status"`
	Error          *string       `json:"error"`
}

type CommitInput struct {
	ThreadID          ThreadID
	ExpectedSequence  uint64
	ExpectedSequences map[ThreadID]uint64 // Additional aggregates in an atomic command.
	Events            []Event
	Receipt           *Receipt
	CommandID         *CommandID // Optional correlation for provider-ingested events.
}

type CommitResult struct {
	Sequence     uint64
	StoredEvents []StoredEvent
	Receipt      *Receipt
	Committed    bool
}

type EventFilter struct {
	AfterSequence   *uint64
	ThroughSequence *uint64
	ThreadID        *ThreadID
	EventType       *EventType
	CommandID       *CommandID
	RawEventID      *RawEventID
	Limit           int
}

// Store is the persistence seam. Commit must atomically check the thread
// sequence, append all events, fold their projections, and insert the receipt.
// An existing command receipt wins and returns Committed=false.
type Store interface {
	Snapshot(context.Context, ThreadID) (ThreadSnapshot, error)
	List(context.Context) ([]ThreadSnapshot, error)
	Read(context.Context, EventFilter) ([]StoredEvent, error)
	GetReceipt(context.Context, CommandID) (Receipt, bool, error)
	Commit(context.Context, CommitInput) (CommitResult, error)
}

type MemoryStore struct {
	mu              sync.RWMutex
	events          []StoredEvent
	projections     map[ThreadID]Projection
	threadSequences map[ThreadID]uint64
	receipts        map[CommandID]Receipt
	sequence        uint64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:          []StoredEvent{},
		projections:     make(map[ThreadID]Projection),
		threadSequences: make(map[ThreadID]uint64),
		receipts:        make(map[CommandID]Receipt),
	}
}

func (s *MemoryStore) Snapshot(_ context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	projection, ok := s.projections[threadID]
	if !ok {
		return ThreadSnapshot{}, ErrThreadNotFound
	}
	clone, err := cloneJSON(projection)
	if err != nil {
		return ThreadSnapshot{}, fmt.Errorf("clone thread projection: %w", err)
	}
	return ThreadSnapshot{Projection: clone, SnapshotSequence: s.threadSequences[threadID]}, nil
}

func (s *MemoryStore) List(_ context.Context) ([]ThreadSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]ThreadID, 0, len(s.projections))
	for id := range s.projections {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]ThreadSnapshot, 0, len(ids))
	for _, id := range ids {
		clone, err := cloneJSON(s.projections[id])
		if err != nil {
			return nil, fmt.Errorf("clone thread projection: %w", err)
		}
		result = append(result, ThreadSnapshot{Projection: clone, SnapshotSequence: s.threadSequences[id]})
	}
	return result, nil
}

func (s *MemoryStore) Read(_ context.Context, filter EventFilter) ([]StoredEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]StoredEvent, 0)
	for _, stored := range s.events {
		if filter.AfterSequence != nil && stored.Sequence <= *filter.AfterSequence {
			continue
		}
		if filter.ThroughSequence != nil && stored.Sequence > *filter.ThroughSequence {
			continue
		}
		if filter.ThreadID != nil && stored.Event.ThreadID != *filter.ThreadID {
			continue
		}
		if filter.EventType != nil && stored.Event.Type != *filter.EventType {
			continue
		}
		if filter.CommandID != nil && (stored.CommandID == nil || *stored.CommandID != *filter.CommandID) {
			continue
		}
		if filter.RawEventID != nil && (stored.Event.RawEventID == nil || *stored.Event.RawEventID != *filter.RawEventID) {
			continue
		}
		clone, err := cloneJSON(stored)
		if err != nil {
			return nil, fmt.Errorf("clone stored event: %w", err)
		}
		result = append(result, clone)
		if filter.Limit > 0 && len(result) == filter.Limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryStore) GetReceipt(_ context.Context, commandID CommandID) (Receipt, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	receipt, ok := s.receipts[commandID]
	return receipt, ok, nil
}

func (s *MemoryStore) Commit(_ context.Context, input CommitInput) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if input.Receipt != nil {
		if existing, ok := s.receipts[input.Receipt.CommandID]; ok {
			events, err := s.eventsByCommandID(input.Receipt.CommandID)
			if err != nil {
				return CommitResult{}, err
			}
			return CommitResult{Sequence: existing.ResultSequence, StoredEvents: events,
				Receipt: &existing, Committed: false}, nil
		}
	}
	if input.Receipt == nil || input.Receipt.Status == ReceiptAccepted {
		expected := map[ThreadID]uint64{input.ThreadID: input.ExpectedSequence}
		for threadID, sequence := range input.ExpectedSequences {
			expected[threadID] = sequence
		}
		for threadID, sequence := range expected {
			if current := s.threadSequences[threadID]; current != sequence {
				return CommitResult{}, fmt.Errorf("%w for thread %s: expected %d, actual %d", ErrSequenceConflict, threadID, sequence, current)
			}
		}
	}

	resultSequence := s.threadSequences[input.ThreadID]
	storedEvents := make([]StoredEvent, 0, len(input.Events))
	if len(input.Events) > 0 {
		stagedProjections := make(map[ThreadID]Projection)
		stagedThreadSequences := make(map[ThreadID]uint64)
		stagedEvents := make([]StoredEvent, 0, len(input.Events))
		nextSequence := s.sequence
		for _, event := range input.Events {
			projection, loaded := stagedProjections[event.ThreadID]
			if !loaded {
				var err error
				projection, err = cloneJSON(s.projections[event.ThreadID])
				if err != nil {
					return CommitResult{}, fmt.Errorf("clone projection before commit: %w", err)
				}
			}
			eventClone, err := cloneJSON(event)
			if err != nil {
				return CommitResult{}, fmt.Errorf("clone event %s: %w", event.ID, err)
			}
			projection, err = Apply(projection, eventClone)
			if err != nil {
				return CommitResult{}, fmt.Errorf("apply event %s: %w", event.ID, err)
			}
			stagedProjections[event.ThreadID] = projection
			nextSequence++
			stagedThreadSequences[event.ThreadID] = nextSequence
			stored := StoredEvent{Sequence: nextSequence, Event: eventClone}
			if input.Receipt != nil {
				commandID := input.Receipt.CommandID
				stored.CommandID = &commandID
			} else if input.CommandID != nil {
				commandID := *input.CommandID
				stored.CommandID = &commandID
			}
			stagedEvents = append(stagedEvents, stored)
			resultEvent, err := cloneJSON(stored)
			if err != nil {
				return CommitResult{}, fmt.Errorf("clone committed event %s: %w", event.ID, err)
			}
			storedEvents = append(storedEvents, resultEvent)
			resultSequence = nextSequence
		}
		// Publish only after every event and aggregate applied successfully.
		s.sequence = nextSequence
		s.events = append(s.events, stagedEvents...)
		for threadID, projection := range stagedProjections {
			s.projections[threadID] = projection
			s.threadSequences[threadID] = stagedThreadSequences[threadID]
		}
	}

	if input.Receipt != nil {
		receipt := *input.Receipt
		receipt.ResultSequence = resultSequence
		s.receipts[receipt.CommandID] = receipt
		return CommitResult{Sequence: resultSequence, StoredEvents: storedEvents, Receipt: &receipt, Committed: true}, nil
	}
	return CommitResult{Sequence: resultSequence, StoredEvents: storedEvents, Committed: true}, nil
}

func (s *MemoryStore) eventsByCommandID(commandID CommandID) ([]StoredEvent, error) {
	result := make([]StoredEvent, 0)
	for _, stored := range s.events {
		if stored.CommandID != nil && *stored.CommandID == commandID {
			clone, err := cloneJSON(stored)
			if err != nil {
				return nil, fmt.Errorf("clone receipt event: %w", err)
			}
			result = append(result, clone)
		}
	}
	return result, nil
}

func receiptError(receipt Receipt) error {
	detail := "previously rejected"
	if receipt.Error != nil {
		detail = *receipt.Error
	}
	return &Error{
		Op: "replay " + string(receipt.CommandType), CommandID: receipt.CommandID,
		ThreadID: receipt.ThreadID, Err: ErrCommandPreviouslyRejected, Detail: detail,
	}
}

func validateReceipt(command Command, receipt Receipt) error {
	if receipt.ThreadID != command.ThreadID || receipt.CommandType != command.Type {
		return commandError(command, ErrCommandIDConflict,
			fmt.Sprintf("receipt targets %s/%s, command targets %s/%s", receipt.ThreadID, receipt.CommandType, command.ThreadID, command.Type))
	}
	if receipt.Status == ReceiptRejected {
		return receiptError(receipt)
	}
	return nil
}

func isNotFound(err error) bool { return errors.Is(err, ErrThreadNotFound) }
