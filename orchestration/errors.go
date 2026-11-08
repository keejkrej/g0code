package orchestration

import (
	"errors"
	"fmt"
)

var (
	ErrThreadNotFound            = errors.New("orchestration thread not found")
	ErrThreadAlreadyExists       = errors.New("orchestration thread already exists")
	ErrCommandRejected           = errors.New("orchestration command rejected")
	ErrCommandPreviouslyRejected = errors.New("orchestration command previously rejected")
	ErrCommandIDConflict         = errors.New("orchestration command ID conflict")
	ErrSequenceConflict          = errors.New("orchestration sequence conflict")
	ErrInvalidCommand            = errors.New("invalid orchestration command")
	ErrInvalidProviderEvent      = errors.New("invalid provider event")
	ErrRunNotInterruptible       = errors.New("run is not interruptible")
	ErrRequestNotRespondable     = errors.New("runtime request is not respondable")
	ErrStableSourceNotFound      = errors.New("no stable fork source found")
	ErrUnsupportedCapability     = errors.New("provider capability is unavailable")
)

// Error adds command and aggregate context while preserving a sentinel for
// errors.Is. Callers should not have to parse Detail.
type Error struct {
	Op        string
	CommandID CommandID
	ThreadID  ThreadID
	Err       error
	Detail    string
}

func (e *Error) Error() string {
	message := e.Op
	if e.CommandID != "" {
		message += " command " + string(e.CommandID)
	}
	if e.ThreadID != "" {
		message += " thread " + string(e.ThreadID)
	}
	if e.Detail != "" {
		message += ": " + e.Detail
	} else if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *Error) Unwrap() error { return e.Err }

func commandError(command Command, err error, detail string) error {
	return &Error{
		Op:        "dispatch " + string(command.Type),
		CommandID: command.CommandID,
		ThreadID:  command.ThreadID,
		Err:       err,
		Detail:    detail,
	}
}

func requireID[T ~string](name string, value T) error {
	if value == "" {
		return fmt.Errorf("%s is required: %w", name, ErrInvalidCommand)
	}
	return nil
}
