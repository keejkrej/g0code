package orchestration

import "time"

type Actor string

const (
	ActorUser   Actor = "user"
	ActorAgent  Actor = "agent"
	ActorSystem Actor = "system"
)

type CreationSource string

const (
	CreationWeb      CreationSource = "web"
	CreationMobile   CreationSource = "mobile"
	CreationMCP      CreationSource = "mcp"
	CreationProvider CreationSource = "provider"
	CreationServer   CreationSource = "server"
)

type RuntimeMode string

const (
	RuntimeApprovalRequired RuntimeMode = "approval-required"
	RuntimeAutoAcceptEdits  RuntimeMode = "auto-accept-edits"
	RuntimeAuto             RuntimeMode = "auto"
	RuntimeFullAccess       RuntimeMode = "full-access"
)

func RuntimeModeAbove(mode, ceiling RuntimeMode) bool {
	rank := map[RuntimeMode]int{
		RuntimeApprovalRequired: 0,
		RuntimeAutoAcceptEdits:  1,
		RuntimeAuto:             2,
		RuntimeFullAccess:       3,
	}
	return rank[mode] > rank[ceiling]
}

type InteractionMode string

const (
	InteractionDefault InteractionMode = "default"
	InteractionPlan    InteractionMode = "plan"
)

type NativeRefStrength string

const (
	NativeRefStrong NativeRefStrength = "strong"
	NativeRefWeak   NativeRefStrength = "weak"
	NativeRefNone   NativeRefStrength = "none"
)

type ProviderRef struct {
	Driver      ProviderDriverKind `json:"driver"`
	NativeID    *string            `json:"nativeId"`
	Strength    NativeRefStrength  `json:"strength"`
	Fingerprint *string            `json:"fingerprint,omitempty"`
	Ordinal     *int               `json:"ordinal,omitempty"`
}

type ModelSelection struct {
	InstanceID ProviderInstanceID `json:"instanceId"`
	Model      string             `json:"model"`
	Options    map[string]any     `json:"options,omitempty"`
}

func (m ModelSelection) Equal(other ModelSelection) bool {
	if m.InstanceID != other.InstanceID || m.Model != other.Model || len(m.Options) != len(other.Options) {
		return false
	}
	for key, value := range m.Options {
		otherValue, ok := other.Options[key]
		if !ok || !jsonValuesEqual(value, otherValue) {
			return false
		}
	}
	return true
}

type Attachment struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	MIMEType  string `json:"mimeType"`
	SizeBytes int64  `json:"sizeBytes"`
	Path      string `json:"path,omitempty"`
}

type ThreadLineage struct {
	ParentThreadID       *ThreadID `json:"parentThreadId"`
	RelationshipToParent *string   `json:"relationshipToParent"`
	RootThreadID         ThreadID  `json:"rootThreadId"`
	SideChat             bool      `json:"sideChat,omitempty"`
}

type ForkedFrom struct {
	Type             string           `json:"type"`
	ThreadID         ThreadID         `json:"threadId,omitempty"`
	RunID            RunID            `json:"runId,omitempty"`
	NodeID           NodeID           `json:"nodeId,omitempty"`
	ProviderThreadID ProviderThreadID `json:"providerThreadId,omitempty"`
	ProviderTurnID   *ProviderTurnID  `json:"providerTurnId,omitempty"`
}

type Thread struct {
	CreatedBy              Actor              `json:"createdBy"`
	CreationSource         CreationSource     `json:"creationSource"`
	ID                     ThreadID           `json:"id"`
	ProjectID              ProjectID          `json:"projectId"`
	Title                  string             `json:"title"`
	ProviderInstanceID     ProviderInstanceID `json:"providerInstanceId"`
	ModelSelection         ModelSelection     `json:"modelSelection"`
	RuntimeMode            RuntimeMode        `json:"runtimeMode"`
	InteractionMode        InteractionMode    `json:"interactionMode"`
	Branch                 *string            `json:"branch"`
	WorktreePath           *string            `json:"worktreePath"`
	ActiveProviderThreadID *ProviderThreadID  `json:"activeProviderThreadId"`
	Lineage                ThreadLineage      `json:"lineage"`
	ForkedFrom             *ForkedFrom        `json:"forkedFrom"`
	CreatedAt              time.Time          `json:"createdAt"`
	UpdatedAt              time.Time          `json:"updatedAt"`
	ArchivedAt             *time.Time         `json:"archivedAt"`
	SettledOverride        *string            `json:"settledOverride"`
	SettledAt              *time.Time         `json:"settledAt"`
	LastVisitedAt          *time.Time         `json:"lastVisitedAt"`
	DeletedAt              *time.Time         `json:"deletedAt"`
}

type RunStatus string

const (
	RunPreparing   RunStatus = "preparing"
	RunQueued      RunStatus = "queued"
	RunStarting    RunStatus = "starting"
	RunRunning     RunStatus = "running"
	RunWaiting     RunStatus = "waiting"
	RunCompleted   RunStatus = "completed"
	RunInterrupted RunStatus = "interrupted"
	RunFailed      RunStatus = "failed"
	RunCancelled   RunStatus = "cancelled"
	RunRolledBack  RunStatus = "rolled_back"
)

func (s RunStatus) Terminal() bool {
	switch s {
	case RunCompleted, RunInterrupted, RunFailed, RunCancelled, RunRolledBack:
		return true
	default:
		return false
	}
}

func (s RunStatus) Active() bool {
	switch s {
	case RunPreparing, RunStarting, RunRunning, RunWaiting:
		return true
	default:
		return false
	}
}

type Run struct {
	ID                         RunID              `json:"id"`
	ThreadID                   ThreadID           `json:"threadId"`
	Ordinal                    int                `json:"ordinal"`
	ProviderInstanceID         ProviderInstanceID `json:"providerInstanceId"`
	ModelSelection             ModelSelection     `json:"modelSelection"`
	ProviderThreadID           *ProviderThreadID  `json:"providerThreadId"`
	UserMessageID              MessageID          `json:"userMessageId"`
	RootNodeID                 *NodeID            `json:"rootNodeId"`
	ActiveAttemptID            *RunAttemptID      `json:"activeAttemptId"`
	Status                     RunStatus          `json:"status"`
	QueuePosition              *int               `json:"queuePosition,omitempty"`
	QueueHeld                  bool               `json:"queueHeld,omitempty"`
	RequestedAt                time.Time          `json:"requestedAt"`
	StartedAt                  *time.Time         `json:"startedAt"`
	CompletedAt                *time.Time         `json:"completedAt"`
	CheckpointID               *CheckpointID      `json:"checkpointId"`
	ContextHandoffID           *ContextHandoffID  `json:"contextHandoffId"`
	RestartContinuationOfRunID *RunID             `json:"restartContinuationOfRunId,omitempty"`
}

type AttemptStatus string

const (
	AttemptPending     AttemptStatus = "pending"
	AttemptRunning     AttemptStatus = "running"
	AttemptCompleted   AttemptStatus = "completed"
	AttemptInterrupted AttemptStatus = "interrupted"
	AttemptFailed      AttemptStatus = "failed"
	AttemptCancelled   AttemptStatus = "cancelled"
	AttemptSuperseded  AttemptStatus = "superseded"
)

type RunAttempt struct {
	ID                 RunAttemptID       `json:"id"`
	RunID              RunID              `json:"runId"`
	AttemptOrdinal     int                `json:"attemptOrdinal"`
	RootNodeID         NodeID             `json:"rootNodeId"`
	ProviderInstanceID ProviderInstanceID `json:"providerInstanceId"`
	ProviderThreadID   ProviderThreadID   `json:"providerThreadId"`
	ProviderTurnID     *ProviderTurnID    `json:"providerTurnId"`
	Reason             string             `json:"reason"`
	Status             AttemptStatus      `json:"status"`
	StartedAt          *time.Time         `json:"startedAt"`
	CompletedAt        *time.Time         `json:"completedAt"`
}

type NodeKind string

const (
	NodeRootTurn         NodeKind = "root_turn"
	NodeAssistantMessage NodeKind = "assistant_message"
	NodeReasoning        NodeKind = "reasoning"
	NodePlan             NodeKind = "plan"
	NodeTodoList         NodeKind = "todo_list"
	NodeToolCall         NodeKind = "tool_call"
	NodeApprovalRequest  NodeKind = "approval_request"
	NodeUserInputRequest NodeKind = "user_input_request"
	NodeSubagent         NodeKind = "subagent"
	NodeHook             NodeKind = "hook"
	NodeSystem           NodeKind = "system"
)

type NodeStatus string

const (
	NodeIdle        NodeStatus = "idle"
	NodePending     NodeStatus = "pending"
	NodeRunning     NodeStatus = "running"
	NodeWaiting     NodeStatus = "waiting"
	NodeCompleted   NodeStatus = "completed"
	NodeInterrupted NodeStatus = "interrupted"
	NodeFailed      NodeStatus = "failed"
	NodeCancelled   NodeStatus = "cancelled"
	NodeRolledBack  NodeStatus = "rolled_back"
)

type ExecutionNode struct {
	ID                NodeID             `json:"id"`
	ThreadID          ThreadID           `json:"threadId"`
	RunID             *RunID             `json:"runId"`
	ParentNodeID      *NodeID            `json:"parentNodeId"`
	RootNodeID        NodeID             `json:"rootNodeId"`
	Kind              NodeKind           `json:"kind"`
	Status            NodeStatus         `json:"status"`
	CountsForRun      bool               `json:"countsForRun"`
	ProviderThreadID  *ProviderThreadID  `json:"providerThreadId"`
	ProviderTurnID    *ProviderTurnID    `json:"providerTurnId"`
	NativeItemRef     *ProviderRef       `json:"nativeItemRef"`
	RuntimeRequestID  *RuntimeRequestID  `json:"runtimeRequestId"`
	CheckpointScopeID *CheckpointScopeID `json:"checkpointScopeId"`
	StartedAt         *time.Time         `json:"startedAt"`
	CompletedAt       *time.Time         `json:"completedAt"`
}

type Subagent struct {
	ID                 NodeID             `json:"id"`
	ThreadID           ThreadID           `json:"threadId"`
	RunID              *RunID             `json:"runId"`
	ParentNodeID       NodeID             `json:"parentNodeId"`
	Origin             string             `json:"origin"`
	CreatedBy          Actor              `json:"createdBy"`
	Driver             ProviderDriverKind `json:"driver"`
	ProviderInstanceID ProviderInstanceID `json:"providerInstanceId"`
	ProviderThreadID   *ProviderThreadID  `json:"providerThreadId"`
	ChildThreadID      *ThreadID          `json:"childThreadId"`
	NativeTaskRef      *ProviderRef       `json:"nativeTaskRef"`
	Prompt             string             `json:"prompt"`
	Title              *string            `json:"title"`
	Model              *string            `json:"model"`
	ModelSelection     *ModelSelection    `json:"modelSelection,omitempty"`
	CompletionWake     string             `json:"completionWake,omitempty"`
	Status             NodeStatus         `json:"status"`
	Progress           string             `json:"progress,omitempty"`
	Result             *string            `json:"result"`
	StartedAt          *time.Time         `json:"startedAt"`
	CompletedAt        *time.Time         `json:"completedAt"`
	UpdatedAt          time.Time          `json:"updatedAt"`
}

type SessionCapabilities struct {
	SupportsMultipleProviderThreadsPerSession bool `json:"supportsMultipleProviderThreadsPerSession"`
	SupportsModelSwitchInSession              bool `json:"supportsModelSwitchInSession"`
	SupportsProviderSwitchingViaHandoff       bool `json:"supportsProviderSwitchingViaHandoff"`
	SupportsRuntimeModeSwitchInSession        bool `json:"supportsRuntimeModeSwitchInSession"`
	PendingRequestsSurviveRestart             bool `json:"pendingRequestsSurviveRestart"`
}

type ThreadCapabilities struct {
	CanCreateEmptyThread      bool `json:"canCreateEmptyThread"`
	CanReadThreadSnapshot     bool `json:"canReadThreadSnapshot"`
	CanRollbackThread         bool `json:"canRollbackThread"`
	CanForkThread             bool `json:"canForkThread"`
	CanForkFromTurn           bool `json:"canForkFromTurn"`
	CanForkFromSubagentThread bool `json:"canForkFromSubagentThread"`
	ExposesNativeThreadID     bool `json:"exposesNativeThreadId"`
}

type TurnCapabilities struct {
	ExposesNativeTurnID                bool   `json:"exposesNativeTurnId"`
	EmitsTurnStarted                   bool   `json:"emitsTurnStarted"`
	EmitsTurnCompleted                 bool   `json:"emitsTurnCompleted"`
	SupportsInterrupt                  bool   `json:"supportsInterrupt"`
	SupportsActiveSteering             bool   `json:"supportsActiveSteering"`
	ActiveSteeringInterruptsTools      bool   `json:"activeSteeringInterruptsTools,omitempty"`
	ActiveSteeringAcceptsAttachments   bool   `json:"activeSteeringAcceptsAttachments,omitempty"`
	SupportsSteeringByInterruptRestart bool   `json:"supportsSteeringByInterruptRestart"`
	SupportsQueuedMessages             bool   `json:"supportsQueuedMessages"`
	TerminalStatusQuality              string `json:"terminalStatusQuality"`
}

type StreamingCapabilities struct {
	StreamsAssistantText  bool `json:"streamsAssistantText"`
	StreamsReasoning      bool `json:"streamsReasoning"`
	StreamsToolOutput     bool `json:"streamsToolOutput"`
	StreamsPlanText       bool `json:"streamsPlanText"`
	EmitsMessageCompleted bool `json:"emitsMessageCompleted"`
}

type ToolCapabilities struct {
	ExposesToolItemIDs           bool `json:"exposesToolItemIds"`
	EmitsToolStarted             bool `json:"emitsToolStarted"`
	EmitsToolCompleted           bool `json:"emitsToolCompleted"`
	EmitsToolOutput              bool `json:"emitsToolOutput"`
	SupportsMCPTools             bool `json:"supportsMcpTools"`
	SupportsDynamicToolCallbacks bool `json:"supportsDynamicToolCallbacks"`
}

type ApprovalCapabilities struct {
	SupportsCommandApproval            bool `json:"supportsCommandApproval"`
	SupportsFileReadApproval           bool `json:"supportsFileReadApproval"`
	SupportsFileChangeApproval         bool `json:"supportsFileChangeApproval"`
	SupportsApplyPatchApproval         bool `json:"supportsApplyPatchApproval"`
	ApprovalsHaveNativeRequestIDs      bool `json:"approvalsHaveNativeRequestIds"`
	ApprovalCallbacksAreLiveOnly       bool `json:"approvalCallbacksAreLiveOnly"`
	ApprovalsCanOriginateFromSubagents bool `json:"approvalsCanOriginateFromSubagents"`
}

type PlanningCapabilities struct {
	EmitsPlanUpdated            bool `json:"emitsPlanUpdated"`
	EmitsTodoList               bool `json:"emitsTodoList"`
	EmitsProposedPlan           bool `json:"emitsProposedPlan"`
	SupportsStructuredQuestions bool `json:"supportsStructuredQuestions"`
	PlanDeltasHaveItemIDs       bool `json:"planDeltasHaveItemIds"`
}

type SubagentCapabilities struct {
	SupportsSubagents        bool `json:"supportsSubagents"`
	ExposesSubagentThreadIDs bool `json:"exposesSubagentThreadIds"`
	EmitsSubagentLifecycle   bool `json:"emitsSubagentLifecycle"`
	CanWaitForSubagents      bool `json:"canWaitForSubagents"`
	CanCloseSubagents        bool `json:"canCloseSubagents"`
	CanForkSubagentThread    bool `json:"canForkSubagentThread"`
}

type ContextCapabilities struct {
	AcceptsSystemContext        bool `json:"acceptsSystemContext"`
	AcceptsDeveloperContext     bool `json:"acceptsDeveloperContext"`
	AcceptsSyntheticUserContext bool `json:"acceptsSyntheticUserContext"`
	CanGenerateSummaries        bool `json:"canGenerateSummaries"`
	CanConsumeHandoffSummaries  bool `json:"canConsumeHandoffSummaries"`
	SupportsDeltaHandoff        bool `json:"supportsDeltaHandoff"`
	SupportsFullThreadHandoff   bool `json:"supportsFullThreadHandoff"`
	MaxRecommendedHandoffChars  *int `json:"maxRecommendedHandoffChars"`
}

type CheckpointCapabilities struct {
	AppCanCheckpointFilesystem          bool `json:"appCanCheckpointFilesystem"`
	SupportsNestedCheckpointScopes      bool `json:"supportsNestedCheckpointScopes"`
	ProviderCanRollbackConversation     bool `json:"providerCanRollbackConversation"`
	ProviderRollbackReturnsSnapshot     bool `json:"providerRollbackReturnsSnapshot"`
	ProviderCanReadConversationSnapshot bool `json:"providerCanReadConversationSnapshot"`
}

type IdentityCapabilities struct {
	NativeThreadIDs  NativeRefStrength `json:"nativeThreadIds"`
	NativeTurnIDs    NativeRefStrength `json:"nativeTurnIds"`
	NativeItemIDs    NativeRefStrength `json:"nativeItemIds"`
	NativeRequestIDs NativeRefStrength `json:"nativeRequestIds"`
}

type ProviderCapabilities struct {
	Sessions      SessionCapabilities    `json:"sessions"`
	Threads       ThreadCapabilities     `json:"threads"`
	Turns         TurnCapabilities       `json:"turns"`
	Streaming     StreamingCapabilities  `json:"streaming"`
	Tools         ToolCapabilities       `json:"tools"`
	Approvals     ApprovalCapabilities   `json:"approvals"`
	Planning      PlanningCapabilities   `json:"planning"`
	Subagents     SubagentCapabilities   `json:"subagents"`
	Context       ContextCapabilities    `json:"context"`
	Checkpointing CheckpointCapabilities `json:"checkpointing"`
	Identity      IdentityCapabilities   `json:"identity"`
	RuntimePolicy struct {
		Enforcement string `json:"enforcement"`
	} `json:"runtimePolicy"`
}

type ProviderSessionStatus string

const (
	SessionStarting ProviderSessionStatus = "starting"
	SessionReady    ProviderSessionStatus = "ready"
	SessionRunning  ProviderSessionStatus = "running"
	SessionWaiting  ProviderSessionStatus = "waiting"
	SessionStopped  ProviderSessionStatus = "stopped"
	SessionError    ProviderSessionStatus = "error"
)

type ProviderSession struct {
	ID                 ProviderSessionID     `json:"id"`
	Driver             ProviderDriverKind    `json:"driver"`
	ProviderInstanceID ProviderInstanceID    `json:"providerInstanceId"`
	Status             ProviderSessionStatus `json:"status"`
	CWD                string                `json:"cwd"`
	Model              *string               `json:"model"`
	Capabilities       ProviderCapabilities  `json:"capabilities"`
	CreatedAt          time.Time             `json:"createdAt"`
	UpdatedAt          time.Time             `json:"updatedAt"`
	LastError          *string               `json:"lastError"`
}

type ProviderThreadStatus string

const (
	ProviderThreadNotLoaded ProviderThreadStatus = "not_loaded"
	ProviderThreadIdle      ProviderThreadStatus = "idle"
	ProviderThreadActive    ProviderThreadStatus = "active"
	ProviderThreadArchived  ProviderThreadStatus = "archived"
	ProviderThreadClosed    ProviderThreadStatus = "closed"
	ProviderThreadError     ProviderThreadStatus = "error"
)

type ProviderThreadForkSource struct {
	ProviderThreadID ProviderThreadID `json:"providerThreadId"`
	ProviderTurnID   *ProviderTurnID  `json:"providerTurnId,omitempty"`
	CheckpointID     *CheckpointID    `json:"checkpointId,omitempty"`
}

type ProviderThread struct {
	ID                        ProviderThreadID          `json:"id"`
	Driver                    ProviderDriverKind        `json:"driver"`
	ProviderInstanceID        ProviderInstanceID        `json:"providerInstanceId"`
	ProviderSessionID         *ProviderSessionID        `json:"providerSessionId"`
	AppThreadID               *ThreadID                 `json:"appThreadId"`
	OwnerNodeID               *NodeID                   `json:"ownerNodeId"`
	NativeThreadRef           *ProviderRef              `json:"nativeThreadRef"`
	NativeConversationHeadRef *ProviderRef              `json:"nativeConversationHeadRef"`
	Status                    ProviderThreadStatus      `json:"status"`
	FirstRunOrdinal           *int                      `json:"firstRunOrdinal"`
	LastRunOrdinal            *int                      `json:"lastRunOrdinal"`
	HandoffIDs                []ContextHandoffID        `json:"handoffIds"`
	ForkedFrom                *ProviderThreadForkSource `json:"forkedFrom"`
	CreatedAt                 time.Time                 `json:"createdAt"`
	UpdatedAt                 time.Time                 `json:"updatedAt"`
}

type ProviderTurnStatus string

const (
	ProviderTurnPending     ProviderTurnStatus = "pending"
	ProviderTurnRunning     ProviderTurnStatus = "running"
	ProviderTurnCompleted   ProviderTurnStatus = "completed"
	ProviderTurnInterrupted ProviderTurnStatus = "interrupted"
	ProviderTurnFailed      ProviderTurnStatus = "failed"
	ProviderTurnCancelled   ProviderTurnStatus = "cancelled"
)

func (s ProviderTurnStatus) Terminal() bool {
	switch s {
	case ProviderTurnCompleted, ProviderTurnInterrupted, ProviderTurnFailed, ProviderTurnCancelled:
		return true
	default:
		return false
	}
}

type TokenUsage struct {
	UsedTokens            int    `json:"usedTokens"`
	MaxTokens             *int   `json:"maxTokens,omitempty"`
	InputTokens           int    `json:"inputTokens,omitempty"`
	CachedInputTokens     int    `json:"cachedInputTokens,omitempty"`
	OutputTokens          int    `json:"outputTokens,omitempty"`
	ReasoningOutputTokens int    `json:"reasoningOutputTokens,omitempty"`
	PromptCacheTTLMS      *int   `json:"promptCacheTtlMs,omitempty"`
	PromptCacheExpiresAt  string `json:"promptCacheExpiresAt,omitempty"`
	UpdatedAt             string `json:"updatedAt"`
}

type ProviderTurn struct {
	ID               ProviderTurnID     `json:"id"`
	ProviderThreadID ProviderThreadID   `json:"providerThreadId"`
	NodeID           NodeID             `json:"nodeId"`
	RunAttemptID     *RunAttemptID      `json:"runAttemptId"`
	NativeTurnRef    *ProviderRef       `json:"nativeTurnRef"`
	Ordinal          int                `json:"ordinal"`
	Status           ProviderTurnStatus `json:"status"`
	StartedAt        *time.Time         `json:"startedAt"`
	CompletedAt      *time.Time         `json:"completedAt"`
	TokenUsage       *TokenUsage        `json:"tokenUsage,omitempty"`
}

type RuntimeRequestKind string

const (
	RequestCommand         RuntimeRequestKind = "command"
	RequestFileRead        RuntimeRequestKind = "file-read"
	RequestFileChange      RuntimeRequestKind = "file-change"
	RequestMCPElicitation  RuntimeRequestKind = "mcp-elicitation"
	RequestPermission      RuntimeRequestKind = "permission"
	RequestDynamicToolCall RuntimeRequestKind = "dynamic_tool_call"
	RequestUserInput       RuntimeRequestKind = "user_input"
	RequestAuthRefresh     RuntimeRequestKind = "auth_refresh"
)

type RuntimeRequestStatus string

const (
	RequestPending   RuntimeRequestStatus = "pending"
	RequestResolved  RuntimeRequestStatus = "resolved"
	RequestExpired   RuntimeRequestStatus = "expired"
	RequestCancelled RuntimeRequestStatus = "cancelled"
)

type ResponseCapability struct {
	Type              string             `json:"type"`
	ProviderSessionID *ProviderSessionID `json:"providerSessionId,omitempty"`
	SupportsFeedback  bool               `json:"supportsFeedback,omitempty"`
	Reason            string             `json:"reason,omitempty"`
}

type RuntimeRequest struct {
	ID                 RuntimeRequestID     `json:"id"`
	NodeID             NodeID               `json:"nodeId"`
	ProviderTurnID     *ProviderTurnID      `json:"providerTurnId"`
	NativeRequestRef   *ProviderRef         `json:"nativeRequestRef"`
	Kind               RuntimeRequestKind   `json:"kind"`
	Status             RuntimeRequestStatus `json:"status"`
	ResponseCapability ResponseCapability   `json:"responseCapability"`
	CreatedAt          time.Time            `json:"createdAt"`
	ResolvedAt         *time.Time           `json:"resolvedAt"`
	Decision           *string              `json:"decision,omitempty"`
	Feedback           *string              `json:"feedback,omitempty"`
	Answers            map[string]any       `json:"answers,omitempty"`
}

type MessageRole string

const (
	MessageUser      MessageRole = "user"
	MessageAssistant MessageRole = "assistant"
	MessageSystem    MessageRole = "system"
)

type ConversationMessage struct {
	CreatedBy      Actor          `json:"createdBy"`
	CreationSource CreationSource `json:"creationSource"`
	ID             MessageID      `json:"id"`
	ThreadID       ThreadID       `json:"threadId"`
	RunID          *RunID         `json:"runId"`
	NodeID         *NodeID        `json:"nodeId"`
	Role           MessageRole    `json:"role"`
	Text           string         `json:"text"`
	Attachments    []Attachment   `json:"attachments"`
	Streaming      bool           `json:"streaming"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
}

type PlanStep struct {
	ID               string     `json:"id"`
	Text             string     `json:"text"`
	Status           string     `json:"status"`
	DurationAnchorAt *time.Time `json:"durationAnchorAt,omitempty"`
	DurationMS       *int64     `json:"durationMs,omitempty"`
}

type PlanArtifact struct {
	ID        PlanID              `json:"id"`
	ThreadID  ThreadID            `json:"threadId"`
	RunID     *RunID              `json:"runId"`
	NodeID    NodeID              `json:"nodeId"`
	Kind      string              `json:"kind"`
	Status    string              `json:"status"`
	Markdown  string              `json:"markdown,omitempty"`
	Steps     []PlanStep          `json:"steps,omitempty"`
	Questions []UserInputQuestion `json:"questions,omitempty"`
}

type UserInputQuestion struct {
	ID       string `json:"id"`
	Header   string `json:"header"`
	Question string `json:"question"`
	Options  []struct {
		Label       string `json:"label"`
		Description string `json:"description"`
		Value       string `json:"value,omitempty"`
		Preview     string `json:"preview,omitempty"`
	} `json:"options"`
}

type TurnItemStatus string

const (
	TurnItemPending     TurnItemStatus = "pending"
	TurnItemRunning     TurnItemStatus = "running"
	TurnItemWaiting     TurnItemStatus = "waiting"
	TurnItemCompleted   TurnItemStatus = "completed"
	TurnItemInterrupted TurnItemStatus = "interrupted"
	TurnItemFailed      TurnItemStatus = "failed"
	TurnItemCancelled   TurnItemStatus = "cancelled"
	TurnItemDeclined    TurnItemStatus = "declined"
)

// TurnItem is the common correlation envelope shared by t3code's turn-item
// variants. Data holds variant-specific fields such as input, output, diff,
// questions, or a checkpoint ref. Type remains the forward-compatible tag.
type TurnItem struct {
	CreatedBy        Actor             `json:"createdBy,omitempty"`
	CreationSource   CreationSource    `json:"creationSource,omitempty"`
	ID               TurnItemID        `json:"id"`
	ThreadID         ThreadID          `json:"threadId"`
	RunID            *RunID            `json:"runId"`
	NodeID           *NodeID           `json:"nodeId"`
	ProviderThreadID *ProviderThreadID `json:"providerThreadId"`
	ProviderTurnID   *ProviderTurnID   `json:"providerTurnId"`
	NativeItemRef    *ProviderRef      `json:"nativeItemRef"`
	ParentItemID     *TurnItemID       `json:"parentItemId"`
	Ordinal          int               `json:"ordinal"`
	Status           TurnItemStatus    `json:"status"`
	Title            string            `json:"title,omitempty"`
	StartedAt        *time.Time        `json:"startedAt,omitempty"`
	CompletedAt      *time.Time        `json:"completedAt,omitempty"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	Type             string            `json:"type"`
	MessageID        MessageID         `json:"messageId,omitempty"`
	Text             string            `json:"text,omitempty"`
	RequestID        RuntimeRequestID  `json:"requestId,omitempty"`
	Data             map[string]any    `json:"data,omitempty"`
}

type CheckpointScope struct {
	ID                  CheckpointScopeID  `json:"id"`
	ThreadID            ThreadID           `json:"threadId"`
	RunID               *RunID             `json:"runId"`
	NodeID              NodeID             `json:"nodeId"`
	ParentScopeID       *CheckpointScopeID `json:"parentScopeId"`
	ProviderThreadID    *ProviderThreadID  `json:"providerThreadId"`
	Kind                string             `json:"kind"`
	OrdinalWithinParent int                `json:"ordinalWithinParent"`
	AdvancesAppRunCount bool               `json:"advancesAppRunCount"`
	CWD                 string             `json:"cwd"`
	CreatedAt           time.Time          `json:"createdAt"`
}

type CheckpointFileSummary struct {
	Path      string `json:"path"`
	Additions int    `json:"additions,omitempty"`
	Deletions int    `json:"deletions,omitempty"`
}

type Checkpoint struct {
	ID                 CheckpointID            `json:"id"`
	ThreadID           ThreadID                `json:"threadId"`
	ScopeID            CheckpointScopeID       `json:"scopeId"`
	RunID              *RunID                  `json:"runId"`
	NodeID             NodeID                  `json:"nodeId"`
	ParentCheckpointID *CheckpointID           `json:"parentCheckpointId"`
	OrdinalWithinScope int                     `json:"ordinalWithinScope"`
	AppRunOrdinal      *int                    `json:"appRunOrdinal"`
	Ref                string                  `json:"ref"`
	Status             string                  `json:"status"`
	Files              []CheckpointFileSummary `json:"files"`
	CapturedAt         time.Time               `json:"capturedAt"`
}

type ContextSourcePoint struct {
	ThreadID          ThreadID      `json:"threadId"`
	RunID             *RunID        `json:"runId,omitempty"`
	CheckpointID      *CheckpointID `json:"checkpointId,omitempty"`
	TurnItemID        *TurnItemID   `json:"turnItemId,omitempty"`
	ProviderThreadRef *ProviderRef  `json:"providerThreadRef,omitempty"`
	ProviderTurnRef   *ProviderRef  `json:"providerTurnRef,omitempty"`
}

type ContextTransferResolution struct {
	Strategy          string            `json:"strategy"`
	ProviderThreadRef *ProviderRef      `json:"providerThreadRef,omitempty"`
	ContextHandoffID  *ContextHandoffID `json:"contextHandoffId,omitempty"`
}

type ContextTransfer struct {
	ID                       ContextTransferID          `json:"id"`
	Type                     string                     `json:"type"`
	SourceThreadID           ThreadID                   `json:"sourceThreadId"`
	TargetThreadID           ThreadID                   `json:"targetThreadId"`
	SourcePoint              ContextSourcePoint         `json:"sourcePoint"`
	BasePoint                *ContextSourcePoint        `json:"basePoint"`
	SourceProviderInstanceID *ProviderInstanceID        `json:"sourceProviderInstanceId"`
	TargetProviderInstanceID *ProviderInstanceID        `json:"targetProviderInstanceId"`
	TargetRunID              *RunID                     `json:"targetRunId"`
	Status                   string                     `json:"status"`
	Resolution               *ContextTransferResolution `json:"resolution"`
	CreatedBy                Actor                      `json:"createdBy"`
	Error                    *string                    `json:"error"`
	CreatedAt                time.Time                  `json:"createdAt"`
	UpdatedAt                time.Time                  `json:"updatedAt"`
	ConsumedAt               *time.Time                 `json:"consumedAt"`
}

type ContextHandoff struct {
	ID                    ContextHandoffID   `json:"id"`
	TransferID            *ContextTransferID `json:"transferId,omitempty"`
	ThreadID              ThreadID           `json:"threadId"`
	TargetRunID           RunID              `json:"targetRunId"`
	FromProviderThreadIDs []ProviderThreadID `json:"fromProviderThreadIds"`
	ToProviderThreadID    ProviderThreadID   `json:"toProviderThreadId"`
	CoveredRunOrdinals    struct {
		From int `json:"from"`
		To   int `json:"to"`
	} `json:"coveredRunOrdinals"`
	Strategy                    string              `json:"strategy"`
	Status                      string              `json:"status"`
	SummaryMessageID            *MessageID          `json:"summaryMessageId"`
	SummaryText                 string              `json:"summaryText"`
	CreatedByProviderInstanceID *ProviderInstanceID `json:"createdByProviderInstanceId"`
	CreatedAt                   time.Time           `json:"createdAt"`
	UpdatedAt                   time.Time           `json:"updatedAt"`
}

type Projection struct {
	Thread           Thread                `json:"thread"`
	Runs             []Run                 `json:"runs"`
	Attempts         []RunAttempt          `json:"attempts"`
	Nodes            []ExecutionNode       `json:"nodes"`
	Subagents        []Subagent            `json:"subagents"`
	ProviderSessions []ProviderSession     `json:"providerSessions"`
	ProviderThreads  []ProviderThread      `json:"providerThreads"`
	ProviderTurns    []ProviderTurn        `json:"providerTurns"`
	RuntimeRequests  []RuntimeRequest      `json:"runtimeRequests"`
	Messages         []ConversationMessage `json:"messages"`
	Plans            []PlanArtifact        `json:"plans"`
	TurnItems        []TurnItem            `json:"turnItems"`
	CheckpointScopes []CheckpointScope     `json:"checkpointScopes"`
	Checkpoints      []Checkpoint          `json:"checkpoints"`
	ContextHandoffs  []ContextHandoff      `json:"contextHandoffs"`
	ContextTransfers []ContextTransfer     `json:"contextTransfers"`
	UpdatedAt        time.Time             `json:"updatedAt"`
}

type ThreadSnapshot struct {
	Projection       Projection `json:"projection"`
	SnapshotSequence uint64     `json:"snapshotSequence"`
}

func (p Projection) Run(id RunID) (Run, bool) {
	for _, run := range p.Runs {
		if run.ID == id {
			return run, true
		}
	}
	return Run{}, false
}

func (p Projection) Node(id NodeID) (ExecutionNode, bool) {
	for _, node := range p.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return ExecutionNode{}, false
}

func (p Projection) ProviderThread(id ProviderThreadID) (ProviderThread, bool) {
	for _, thread := range p.ProviderThreads {
		if thread.ID == id {
			return thread, true
		}
	}
	return ProviderThread{}, false
}

func (p Projection) ProviderTurn(id ProviderTurnID) (ProviderTurn, bool) {
	for _, turn := range p.ProviderTurns {
		if turn.ID == id {
			return turn, true
		}
	}
	return ProviderTurn{}, false
}

func (p Projection) RuntimeRequest(id RuntimeRequestID) (RuntimeRequest, bool) {
	for _, request := range p.RuntimeRequests {
		if request.ID == id {
			return request, true
		}
	}
	return RuntimeRequest{}, false
}

func (p Projection) ActiveRun() (Run, bool) {
	for i := len(p.Runs) - 1; i >= 0; i-- {
		if p.Runs[i].Status.Active() {
			return p.Runs[i], true
		}
	}
	return Run{}, false
}

func (p Projection) NextRunOrdinal() int {
	next := 1
	for _, run := range p.Runs {
		if run.Ordinal >= next {
			next = run.Ordinal + 1
		}
	}
	return next
}

func LatestProviderTurnForAttempt(turns []ProviderTurn, attemptID RunAttemptID) (ProviderTurn, bool) {
	var latest ProviderTurn
	found := false
	for _, turn := range turns {
		if turn.RunAttemptID == nil || *turn.RunAttemptID != attemptID {
			continue
		}
		if !found || turn.Ordinal > latest.Ordinal {
			latest, found = turn, true
		}
	}
	return latest, found
}
