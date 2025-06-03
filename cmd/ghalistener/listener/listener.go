package listener

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/actions/actions-runner-controller/cmd/ghalistener/metrics"
	"github.com/actions/actions-runner-controller/github/actions"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
)

const (
	sessionCreationMaxRetries = 10
)

// message types
const (
	messageTypeJobAvailable = "JobAvailable"
	messageTypeJobAssigned  = "JobAssigned"
	messageTypeJobStarted   = "JobStarted"
	messageTypeJobCompleted = "JobCompleted"
)

//go:generate mockery --name Client --output ./mocks --outpkg mocks --case underscore
type Client interface {
	GetAcquirableJobs(ctx context.Context, runnerScaleSetId int) (*actions.AcquirableJobList, error)
	CreateMessageSession(ctx context.Context, runnerScaleSetId int, owner string) (*actions.RunnerScaleSetSession, error)
	GetMessage(ctx context.Context, messageQueueUrl, messageQueueAccessToken string, lastMessageId int64, maxCapacity int) (*actions.RunnerScaleSetMessage, error)
	DeleteMessage(ctx context.Context, messageQueueUrl, messageQueueAccessToken string, messageId int64) error
	AcquireJobs(ctx context.Context, runnerScaleSetId int, messageQueueAccessToken string, requestIds []int64) ([]int64, error)
	RefreshMessageSession(ctx context.Context, runnerScaleSetId int, sessionId *uuid.UUID) (*actions.RunnerScaleSetSession, error)
	DeleteMessageSession(ctx context.Context, runnerScaleSetId int, sessionId *uuid.UUID) error
}

type Config struct {
	Client     Client
	ScaleSetID int
	MinRunners int
	MaxRunners int
	Logger     logr.Logger
	Metrics    metrics.Publisher
}

func (c *Config) Validate() error {
	if c.Client == nil {
		return errors.New("client is required")
	}
	if c.ScaleSetID == 0 {
		return errors.New("scaleSetID is required")
	}
	if c.MinRunners < 0 {
		return errors.New("minRunners must be greater than or equal to 0")
	}
	if c.MaxRunners < 0 {
		return errors.New("maxRunners must be greater than or equal to 0")
	}
	if c.MaxRunners > 0 && c.MinRunners > c.MaxRunners {
		return errors.New("minRunners must be less than or equal to maxRunners")
	}
	return nil
}

// The Listener's role is to manage all interactions with the actions service.
// It receives messages and processes them using the given handler.
type Listener struct {
	// configured fields
	scaleSetID int               // The ID of the scale set associated with the listener.
	client     Client            // The client used to interact with the scale set.
	metrics    metrics.Publisher // The publisher used to publish metrics.

	// internal fields
	logger   logr.Logger // The logger used for logging.
	hostname string      // The hostname of the listener.

	// updated fields
	lastMessageID int64                          // The ID of the last processed message.
	maxCapacity   int                            // The maximum number of runners that can be created.
	session       *actions.RunnerScaleSetSession // The session for managing the runner scale set.
}

func New(config Config) (*Listener, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	listener := &Listener{
		scaleSetID:  config.ScaleSetID,
		client:      config.Client,
		logger:      config.Logger,
		metrics:     metrics.Discard,
		maxCapacity: config.MaxRunners,
	}

	if config.Metrics != nil {
		listener.metrics = config.Metrics
	}

	listener.metrics.PublishStatic(config.MinRunners, config.MaxRunners)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = uuid.NewString()
		listener.logger.Info("Failed to get hostname, fallback to uuid", "uuid", hostname, "error", err)
	}
	listener.hostname = hostname

	return listener, nil
}

//go:generate mockery --name Handler --output ./mocks --outpkg mocks --case underscore
type Handler interface {
	HandleJobStarted(ctx context.Context, jobInfo *actions.JobStarted) error
	HandleDesiredRunnerCount(ctx context.Context, count, jobsCompleted int) (int, error)
}

// Listen listens for incoming messages and handles them using the provided handler.
// It continuously listens for messages until the context is cancelled.
// The initial message contains the current statistics and acquirable jobs, if any.
// The handler is responsible for handling the initial message and subsequent messages.
// If an error occurs during any step, Listen returns an error.
func (l *Listener) Listen(ctx context.Context, handler Handler) error {
	l.logger.Info("🚀 LISTENER STARTING",
		"scaleSetID", l.scaleSetID,
		"hostname", l.hostname,
		"maxCapacity", l.maxCapacity)

	if err := l.createSession(ctx); err != nil {
		return fmt.Errorf("createSession failed: %w", err)
	}

	defer func() {
		if err := l.deleteMessageSession(); err != nil {
			l.logger.Error(err, "failed to delete message session")
		}
	}()

	initialMessage := &actions.RunnerScaleSetMessage{
		MessageId:   0,
		MessageType: "RunnerScaleSetJobMessages",
		Statistics:  l.session.Statistics,
		Body:        "",
	}

	if l.session.Statistics == nil {
		return fmt.Errorf("session statistics is nil")
	}

	l.logger.Info("📊 INITIAL SESSION STATISTICS",
		"totalAvailableJobs", l.session.Statistics.TotalAvailableJobs,
		"totalAcquiredJobs", l.session.Statistics.TotalAcquiredJobs,
		"totalAssignedJobs", l.session.Statistics.TotalAssignedJobs,
		"totalRunningJobs", l.session.Statistics.TotalRunningJobs,
		"totalRegisteredRunners", l.session.Statistics.TotalRegisteredRunners,
		"totalBusyRunners", l.session.Statistics.TotalBusyRunners,
		"totalIdleRunners", l.session.Statistics.TotalIdleRunners)

	l.metrics.PublishStatistics(initialMessage.Statistics)

	desiredRunners, err := handler.HandleDesiredRunnerCount(ctx, initialMessage.Statistics.TotalAssignedJobs, 0)
	if err != nil {
		return fmt.Errorf("handling initial message failed: %w", err)
	}

	l.logger.Info("🎯 INITIAL SCALING DECISION",
		"desiredRunners", desiredRunners,
		"basedOnAssignedJobs", initialMessage.Statistics.TotalAssignedJobs)

	l.metrics.PublishDesiredRunners(desiredRunners)

	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			l.logger.Info("🛑 LISTENER STOPPING", "reason", ctx.Err(), "messagesProcessed", messageCount)
			return ctx.Err()
		default:
		}

		messageCount++
		l.logger.Info("🔄 POLLING FOR MESSAGE", "pollNumber", messageCount, "lastMessageID", l.lastMessageID)

		msg, err := l.getMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get message: %w", err)
		}

		if msg == nil {
			l.logger.Info("📭 NO MESSAGE RECEIVED", "pollNumber", messageCount)
			_, err := handler.HandleDesiredRunnerCount(ctx, 0, 0)
			if err != nil {
				return fmt.Errorf("handling nil message failed: %w", err)
			}

			continue
		}

		l.logger.Info("📨 MESSAGE RECEIVED",
			"pollNumber", messageCount,
			"messageId", msg.MessageId,
			"messageType", msg.MessageType,
			"bodyLength", len(msg.Body),
			"hasStatistics", msg.Statistics != nil)

		// Remove cancellation from the context to avoid cancelling the message handling.
		if err := l.handleMessage(context.WithoutCancel(ctx), handler, msg); err != nil {
			return fmt.Errorf("failed to handle message: %w", err)
		}
	}
}

func (l *Listener) handleMessage(ctx context.Context, handler Handler, msg *actions.RunnerScaleSetMessage) error {
	l.logger.Info("🔍 PARSING MESSAGE",
		"messageId", msg.MessageId,
		"messageType", msg.MessageType)

	parsedMsg, err := l.parseMessage(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	l.logger.Info("📊 MESSAGE STATISTICS",
		"totalAvailableJobs", parsedMsg.statistics.TotalAvailableJobs,
		"totalAcquiredJobs", parsedMsg.statistics.TotalAcquiredJobs,
		"totalAssignedJobs", parsedMsg.statistics.TotalAssignedJobs,
		"totalRunningJobs", parsedMsg.statistics.TotalRunningJobs,
		"totalRegisteredRunners", parsedMsg.statistics.TotalRegisteredRunners,
		"totalBusyRunners", parsedMsg.statistics.TotalBusyRunners,
		"totalIdleRunners", parsedMsg.statistics.TotalIdleRunners)

	l.logger.Info("📋 MESSAGE CONTENT SUMMARY",
		"jobsAvailable", len(parsedMsg.jobsAvailable),
		"jobsStarted", len(parsedMsg.jobsStarted),
		"jobsCompleted", len(parsedMsg.jobsCompleted))

	l.metrics.PublishStatistics(parsedMsg.statistics)

	if len(parsedMsg.jobsAvailable) > 0 {
		l.logger.Info("🎯 ATTEMPTING JOB ACQUISITION",
			"availableJobCount", len(parsedMsg.jobsAvailable),
			"scaleSetID", l.scaleSetID)

		for i, job := range parsedMsg.jobsAvailable {
			l.logger.Info("📄 JOB AVAILABLE DETAILS",
				"jobIndex", i+1,
				"runnerRequestId", job.RunnerRequestId,
				"repositoryName", job.RepositoryName,
				"ownerName", job.OwnerName,
				"jobWorkflowRef", job.JobWorkflowRef,
				"eventName", job.EventName,
				"requestLabels", job.RequestLabels,
				"acquireJobUrl", job.AcquireJobUrl,
				"queueTime", job.QueueTime,
				"scaleSetAssignTime", job.ScaleSetAssignTime,
				"scaleSetID", l.scaleSetID)
		}

		acquiredJobIDs, err := l.acquireAvailableJobs(ctx, parsedMsg.jobsAvailable)
		if err != nil {
			l.logger.Error(err, "❌ JOB ACQUISITION FAILED",
				"attemptedJobCount", len(parsedMsg.jobsAvailable),
				"scaleSetID", l.scaleSetID)
			return fmt.Errorf("failed to acquire jobs: %w", err)
		}

		l.logger.Info("✅ JOB ACQUISITION RESULT",
			"requestedJobs", len(parsedMsg.jobsAvailable),
			"acquiredJobs", len(acquiredJobIDs),
			"acquiredJobIds", fmt.Sprint(acquiredJobIDs),
			"scaleSetID", l.scaleSetID)

		if len(acquiredJobIDs) == 0 {
			l.logger.Info("⚠️ NO JOBS ACQUIRED",
				"availableJobs", len(parsedMsg.jobsAvailable),
				"possibleCause", "AcquireJobs endpoint returned 204 or jobs claimed by other listeners",
				"scaleSetID", l.scaleSetID)
		}
	}

	for i, jobCompleted := range parsedMsg.jobsCompleted {
		l.logger.Info("🏁 JOB COMPLETED",
			"jobIndex", i+1,
			"runnerRequestId", jobCompleted.RunnerRequestId,
			"result", jobCompleted.Result,
			"runnerId", jobCompleted.RunnerId,
			"runnerName", jobCompleted.RunnerName,
			"repositoryName", jobCompleted.RepositoryName,
			"workflowRunId", jobCompleted.WorkflowRunId,
			"finishTime", jobCompleted.FinishTime,
			"scaleSetID", l.scaleSetID)
		l.metrics.PublishJobCompleted(jobCompleted)
	}

	l.lastMessageID = msg.MessageId

	if err := l.deleteLastMessage(ctx); err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	for i, jobStarted := range parsedMsg.jobsStarted {
		l.logger.Info("🏃 JOB STARTED",
			"jobIndex", i+1,
			"runnerRequestId", jobStarted.RunnerRequestId,
			"runnerId", jobStarted.RunnerId,
			"runnerName", jobStarted.RunnerName,
			"repositoryName", jobStarted.RepositoryName,
			"workflowRunId", jobStarted.WorkflowRunId,
			"runnerAssignTime", jobStarted.RunnerAssignTime,
			"scaleSetID", l.scaleSetID)

		if err := handler.HandleJobStarted(ctx, jobStarted); err != nil {
			return fmt.Errorf("failed to handle job started: %w", err)
		}
		l.metrics.PublishJobStarted(jobStarted)
	}

	l.logger.Info("🎯 CALCULATING DESIRED RUNNERS",
		"totalAssignedJobs", parsedMsg.statistics.TotalAssignedJobs,
		"jobsCompleted", len(parsedMsg.jobsCompleted),
		"scaleSetID", l.scaleSetID)

	desiredRunners, err := handler.HandleDesiredRunnerCount(ctx, parsedMsg.statistics.TotalAssignedJobs, len(parsedMsg.jobsCompleted))
	if err != nil {
		return fmt.Errorf("failed to handle desired runner count: %w", err)
	}

	l.logger.Info("📈 SCALING DECISION",
		"desiredRunners", desiredRunners,
		"basedOnAssignedJobs", parsedMsg.statistics.TotalAssignedJobs,
		"completedJobs", len(parsedMsg.jobsCompleted),
		"scaleSetID", l.scaleSetID)

	l.metrics.PublishDesiredRunners(desiredRunners)
	return nil
}

func (l *Listener) createSession(ctx context.Context) error {
	var session *actions.RunnerScaleSetSession
	var retries int

	l.logger.Info("🔌 CREATING MESSAGE SESSION", "scaleSetID", l.scaleSetID, "hostname", l.hostname)

	for {
		var err error
		session, err = l.client.CreateMessageSession(ctx, l.scaleSetID, l.hostname)
		if err == nil {
			break
		}

		clientErr := &actions.HttpClientSideError{}
		if !errors.As(err, &clientErr) {
			l.logger.Error(err, "❌ SESSION CREATION FAILED", "scaleSetID", l.scaleSetID)
			return fmt.Errorf("failed to create session: %w", err)
		}

		if clientErr.Code != http.StatusConflict {
			l.logger.Error(err, "❌ SESSION CREATION FAILED", "statusCode", clientErr.Code, "scaleSetID", l.scaleSetID)
			return fmt.Errorf("failed to create session: %w", err)
		}

		retries++
		if retries >= sessionCreationMaxRetries {
			l.logger.Error(err, "❌ SESSION CREATION FAILED - MAX RETRIES", "retries", retries, "scaleSetID", l.scaleSetID)
			return fmt.Errorf("failed to create session after %d retries: %w", retries, err)
		}

		l.logger.Info("⏳ SESSION CONFLICT - RETRYING", "error", err.Error(), "retryNumber", retries, "scaleSetID", l.scaleSetID)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-time.After(30 * time.Second):
		}
	}

	statistics, err := json.Marshal(session.Statistics)
	if err != nil {
		return fmt.Errorf("failed to marshal statistics: %w", err)
	}

	l.logger.Info("✅ SESSION CREATED SUCCESSFULLY",
		"scaleSetID", l.scaleSetID,
		"sessionId", session.SessionId,
		"ownerName", session.OwnerName,
		"messageQueueUrl", session.MessageQueueUrl,
		"runnerScaleSetId", session.RunnerScaleSet.Id,
		"runnerScaleSetName", session.RunnerScaleSet.Name,
		"statistics", string(statistics))

	l.session = session

	return nil
}

func (l *Listener) getMessage(ctx context.Context) (*actions.RunnerScaleSetMessage, error) {
	l.logger.Info("📡 REQUESTING MESSAGE",
		"lastMessageID", l.lastMessageID,
		"maxCapacity", l.maxCapacity,
		"scaleSetID", l.scaleSetID)

	msg, err := l.client.GetMessage(ctx, l.session.MessageQueueUrl, l.session.MessageQueueAccessToken, l.lastMessageID, l.maxCapacity)
	if err == nil { // if NO error
		if msg != nil {
			l.logger.Info("📨 MESSAGE RECEIVED",
				"messageId", msg.MessageId,
				"messageType", msg.MessageType,
				"bodyLength", len(msg.Body),
				"scaleSetID", l.scaleSetID)
		} else {
			l.logger.Info("📭 NO MESSAGE AVAILABLE", "scaleSetID", l.scaleSetID)
		}
		return msg, nil
	}

	expiredError := &actions.MessageQueueTokenExpiredError{}
	if !errors.As(err, &expiredError) {
		l.logger.Error(err, "❌ GET MESSAGE FAILED", "scaleSetID", l.scaleSetID)
		return nil, fmt.Errorf("failed to get next message: %w", err)
	}

	l.logger.Info("🔄 MESSAGE TOKEN EXPIRED - REFRESHING", "scaleSetID", l.scaleSetID)

	if err := l.refreshSession(ctx); err != nil {
		return nil, err
	}

	l.logger.Info("📡 RETRYING MESSAGE REQUEST", "lastMessageID", l.lastMessageID, "scaleSetID", l.scaleSetID)

	msg, err = l.client.GetMessage(ctx, l.session.MessageQueueUrl, l.session.MessageQueueAccessToken, l.lastMessageID, l.maxCapacity)
	if err != nil { // if NO error
		l.logger.Error(err, "❌ GET MESSAGE FAILED AFTER REFRESH", "scaleSetID", l.scaleSetID)
		return nil, fmt.Errorf("failed to get next message after message session refresh: %w", err)
	}

	if msg != nil {
		l.logger.Info("📨 MESSAGE RECEIVED AFTER REFRESH",
			"messageId", msg.MessageId,
			"messageType", msg.MessageType,
			"bodyLength", len(msg.Body),
			"scaleSetID", l.scaleSetID)
	}

	return msg, nil
}

func (l *Listener) deleteLastMessage(ctx context.Context) error {
	l.logger.Info("🗑️ DELETING MESSAGE", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)

	err := l.client.DeleteMessage(ctx, l.session.MessageQueueUrl, l.session.MessageQueueAccessToken, l.lastMessageID)
	if err == nil { // if NO error
		l.logger.Info("✅ MESSAGE DELETED", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)
		return nil
	}

	expiredError := &actions.MessageQueueTokenExpiredError{}
	if !errors.As(err, &expiredError) {
		l.logger.Error(err, "❌ DELETE MESSAGE FAILED", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)
		return fmt.Errorf("failed to delete last message: %w", err)
	}

	l.logger.Info("🔄 DELETE TOKEN EXPIRED - REFRESHING", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)

	if err := l.refreshSession(ctx); err != nil {
		return err
	}

	err = l.client.DeleteMessage(ctx, l.session.MessageQueueUrl, l.session.MessageQueueAccessToken, l.lastMessageID)
	if err != nil {
		l.logger.Error(err, "❌ DELETE MESSAGE FAILED AFTER REFRESH", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)
		return fmt.Errorf("failed to delete last message after message session refresh: %w", err)
	}

	l.logger.Info("✅ MESSAGE DELETED AFTER REFRESH", "messageId", l.lastMessageID, "scaleSetID", l.scaleSetID)
	return nil
}

type parsedMessage struct {
	statistics    *actions.RunnerScaleSetStatistic
	jobsStarted   []*actions.JobStarted
	jobsAvailable []*actions.JobAvailable
	jobsCompleted []*actions.JobCompleted
}

func (l *Listener) parseMessage(ctx context.Context, msg *actions.RunnerScaleSetMessage) (*parsedMessage, error) {
	if msg.MessageType != "RunnerScaleSetJobMessages" {
		l.logger.Info("⏭️ SKIPPING MESSAGE", "messageType", msg.MessageType, "scaleSetID", l.scaleSetID)
		return nil, fmt.Errorf("invalid message type: %s", msg.MessageType)
	}

	l.logger.Info("🔍 PARSING MESSAGE CONTENT",
		"messageId", msg.MessageId,
		"messageType", msg.MessageType,
		"bodyLength", len(msg.Body),
		"scaleSetID", l.scaleSetID)

	if msg.Statistics == nil {
		l.logger.Error(nil, "❌ MESSAGE MISSING STATISTICS", "messageId", msg.MessageId, "scaleSetID", l.scaleSetID)
		return nil, fmt.Errorf("invalid message: statistics is nil")
	}

	l.logger.Info("📊 MESSAGE STATISTICS DETAILS",
		"messageId", msg.MessageId,
		"totalAvailableJobs", msg.Statistics.TotalAvailableJobs,
		"totalAcquiredJobs", msg.Statistics.TotalAcquiredJobs,
		"totalAssignedJobs", msg.Statistics.TotalAssignedJobs,
		"totalRunningJobs", msg.Statistics.TotalRunningJobs,
		"totalRegisteredRunners", msg.Statistics.TotalRegisteredRunners,
		"totalBusyRunners", msg.Statistics.TotalBusyRunners,
		"totalIdleRunners", msg.Statistics.TotalIdleRunners,
		"scaleSetID", l.scaleSetID)

	var batchedMessages []json.RawMessage
	if len(msg.Body) > 0 {
		l.logger.Info("📦 PARSING BATCHED MESSAGES", "bodyLength", len(msg.Body), "scaleSetID", l.scaleSetID)
		if err := json.Unmarshal([]byte(msg.Body), &batchedMessages); err != nil {
			l.logger.Error(err, "❌ FAILED TO PARSE BATCHED MESSAGES", "bodyLength", len(msg.Body), "scaleSetID", l.scaleSetID)
			return nil, fmt.Errorf("failed to unmarshal batched messages: %w", err)
		}
		l.logger.Info("📦 BATCHED MESSAGES PARSED", "messageCount", len(batchedMessages), "scaleSetID", l.scaleSetID)
	} else {
		l.logger.Info("📦 NO BATCHED MESSAGES", "scaleSetID", l.scaleSetID)
	}

	parsedMsg := &parsedMessage{
		statistics: msg.Statistics,
	}

	for i, msg := range batchedMessages {
		l.logger.Info("🔍 PARSING BATCH MESSAGE", "batchIndex", i+1, "totalBatches", len(batchedMessages), "scaleSetID", l.scaleSetID)

		var messageType actions.JobMessageType
		if err := json.Unmarshal(msg, &messageType); err != nil {
			l.logger.Error(err, "❌ FAILED TO DECODE MESSAGE TYPE", "batchIndex", i+1, "scaleSetID", l.scaleSetID)
			return nil, fmt.Errorf("failed to decode job message type: %w", err)
		}

		l.logger.Info("📨 BATCH MESSAGE TYPE", "batchIndex", i+1, "messageType", messageType.MessageType, "scaleSetID", l.scaleSetID)

		switch messageType.MessageType {
		case messageTypeJobAvailable:
			var jobAvailable actions.JobAvailable
			if err := json.Unmarshal(msg, &jobAvailable); err != nil {
				l.logger.Error(err, "❌ FAILED TO DECODE JOB AVAILABLE", "batchIndex", i+1, "scaleSetID", l.scaleSetID)
				return nil, fmt.Errorf("failed to decode job available: %w", err)
			}

			l.logger.Info("🎯 JOB AVAILABLE MESSAGE",
				"batchIndex", i+1,
				"runnerRequestId", jobAvailable.RunnerRequestId,
				"repositoryName", jobAvailable.RepositoryName,
				"ownerName", jobAvailable.OwnerName,
				"jobWorkflowRef", jobAvailable.JobWorkflowRef,
				"eventName", jobAvailable.EventName,
				"requestLabels", jobAvailable.RequestLabels,
				"queueTime", jobAvailable.QueueTime,
				"scaleSetAssignTime", jobAvailable.ScaleSetAssignTime,
				"acquireJobUrl", jobAvailable.AcquireJobUrl,
				"scaleSetID", l.scaleSetID)

			parsedMsg.jobsAvailable = append(parsedMsg.jobsAvailable, &jobAvailable)

		case messageTypeJobAssigned:
			var jobAssigned actions.JobAssigned
			if err := json.Unmarshal(msg, &jobAssigned); err != nil {
				l.logger.Error(err, "❌ FAILED TO DECODE JOB ASSIGNED", "batchIndex", i+1, "scaleSetID", l.scaleSetID)
				return nil, fmt.Errorf("failed to decode job assigned: %w", err)
			}

			l.logger.Info("📋 JOB ASSIGNED MESSAGE",
				"batchIndex", i+1,
				"runnerRequestId", jobAssigned.RunnerRequestId,
				"repositoryName", jobAssigned.RepositoryName,
				"ownerName", jobAssigned.OwnerName,
				"jobWorkflowRef", jobAssigned.JobWorkflowRef,
				"scaleSetID", l.scaleSetID)

		case messageTypeJobStarted:
			var jobStarted actions.JobStarted
			if err := json.Unmarshal(msg, &jobStarted); err != nil {
				l.logger.Error(err, "❌ FAILED TO DECODE JOB STARTED", "batchIndex", i+1, "scaleSetID", l.scaleSetID)
				return nil, fmt.Errorf("could not decode job started message. %w", err)
			}

			l.logger.Info("🏃 JOB STARTED MESSAGE",
				"batchIndex", i+1,
				"runnerRequestId", jobStarted.RunnerRequestId,
				"runnerId", jobStarted.RunnerId,
				"runnerName", jobStarted.RunnerName,
				"repositoryName", jobStarted.RepositoryName,
				"ownerName", jobStarted.OwnerName,
				"runnerAssignTime", jobStarted.RunnerAssignTime,
				"scaleSetID", l.scaleSetID)

			parsedMsg.jobsStarted = append(parsedMsg.jobsStarted, &jobStarted)

		case messageTypeJobCompleted:
			var jobCompleted actions.JobCompleted
			if err := json.Unmarshal(msg, &jobCompleted); err != nil {
				l.logger.Error(err, "❌ FAILED TO DECODE JOB COMPLETED", "batchIndex", i+1, "scaleSetID", l.scaleSetID)
				return nil, fmt.Errorf("failed to decode job completed: %w", err)
			}

			l.logger.Info("🏁 JOB COMPLETED MESSAGE",
				"batchIndex", i+1,
				"runnerRequestId", jobCompleted.RunnerRequestId,
				"result", jobCompleted.Result,
				"runnerId", jobCompleted.RunnerId,
				"runnerName", jobCompleted.RunnerName,
				"repositoryName", jobCompleted.RepositoryName,
				"finishTime", jobCompleted.FinishTime,
				"scaleSetID", l.scaleSetID)

			parsedMsg.jobsCompleted = append(parsedMsg.jobsCompleted, &jobCompleted)

		default:
			l.logger.Info("❓ UNKNOWN MESSAGE TYPE",
				"batchIndex", i+1,
				"messageType", messageType.MessageType,
				"scaleSetID", l.scaleSetID)
		}
	}

	l.logger.Info("✅ MESSAGE PARSING COMPLETE",
		"messageId", msg.MessageId,
		"jobsAvailable", len(parsedMsg.jobsAvailable),
		"jobsStarted", len(parsedMsg.jobsStarted),
		"jobsCompleted", len(parsedMsg.jobsCompleted),
		"scaleSetID", l.scaleSetID)

	return parsedMsg, nil
}

func (l *Listener) acquireAvailableJobs(ctx context.Context, jobsAvailable []*actions.JobAvailable) ([]int64, error) {
	ids := make([]int64, 0, len(jobsAvailable))
	for _, job := range jobsAvailable {
		ids = append(ids, job.RunnerRequestId)
	}

	l.logger.Info("🎯 ATTEMPTING JOB ACQUISITION",
		"jobCount", len(ids),
		"requestIds", fmt.Sprint(ids),
		"scaleSetID", l.scaleSetID,
		"sessionToken", func() string {
			if len(l.session.MessageQueueAccessToken) > 10 {
				return l.session.MessageQueueAccessToken[:10] + "..."
			}
			return l.session.MessageQueueAccessToken
		}())

	idsAcquired, err := l.client.AcquireJobs(ctx, l.scaleSetID, l.session.MessageQueueAccessToken, ids)
	if err == nil { // if NO errors
		l.logger.Info("✅ JOB ACQUISITION SUCCESS",
			"requestedJobs", len(ids),
			"acquiredJobs", len(idsAcquired),
			"acquiredIds", fmt.Sprint(idsAcquired),
			"scaleSetID", l.scaleSetID)
		return idsAcquired, nil
	}

	l.logger.Error(err, "❌ JOB ACQUISITION FAILED - INITIAL ATTEMPT",
		"requestedJobs", len(ids),
		"requestIds", fmt.Sprint(ids),
		"scaleSetID", l.scaleSetID,
		"errorType", fmt.Sprintf("%T", err))

	expiredError := &actions.MessageQueueTokenExpiredError{}
	if !errors.As(err, &expiredError) {
		l.logger.Error(err, "❌ JOB ACQUISITION FAILED - NOT TOKEN ERROR",
			"requestedJobs", len(ids),
			"scaleSetID", l.scaleSetID)
		return nil, fmt.Errorf("failed to acquire jobs: %w", err)
	}

	l.logger.Info("🔄 JOB ACQUISITION TOKEN EXPIRED - REFRESHING SESSION", "scaleSetID", l.scaleSetID)

	if err := l.refreshSession(ctx); err != nil {
		l.logger.Error(err, "❌ SESSION REFRESH FAILED", "scaleSetID", l.scaleSetID)
		return nil, err
	}

	l.logger.Info("🎯 RETRYING JOB ACQUISITION AFTER TOKEN REFRESH", "requestedJobs", len(ids), "scaleSetID", l.scaleSetID)

	idsAcquired, err = l.client.AcquireJobs(ctx, l.scaleSetID, l.session.MessageQueueAccessToken, ids)
	if err != nil {
		l.logger.Error(err, "❌ JOB ACQUISITION FAILED - AFTER REFRESH",
			"requestedJobs", len(ids),
			"requestIds", fmt.Sprint(ids),
			"scaleSetID", l.scaleSetID,
			"errorType", fmt.Sprintf("%T", err))
		return nil, fmt.Errorf("failed to acquire jobs after session refresh: %w", err)
	}

	l.logger.Info("✅ JOB ACQUISITION SUCCESS - AFTER REFRESH",
		"requestedJobs", len(ids),
		"acquiredJobs", len(idsAcquired),
		"acquiredIds", fmt.Sprint(idsAcquired),
		"scaleSetID", l.scaleSetID)

	return idsAcquired, nil
}

func (l *Listener) refreshSession(ctx context.Context) error {
	l.logger.Info("🔄 REFRESHING SESSION",
		"scaleSetID", l.scaleSetID,
		"sessionId", l.session.SessionId)

	session, err := l.client.RefreshMessageSession(ctx, l.session.RunnerScaleSet.Id, l.session.SessionId)
	if err != nil {
		l.logger.Error(err, "❌ SESSION REFRESH FAILED", "scaleSetID", l.scaleSetID, "sessionId", l.session.SessionId)
		return fmt.Errorf("refresh message session failed. %w", err)
	}

	l.logger.Info("✅ SESSION REFRESHED",
		"scaleSetID", l.scaleSetID,
		"oldSessionId", l.session.SessionId,
		"newSessionId", session.SessionId)

	l.session = session
	return nil
}

func (l *Listener) deleteMessageSession() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	l.logger.Info("🗑️ DELETING MESSAGE SESSION",
		"scaleSetID", l.scaleSetID,
		"sessionId", l.session.SessionId)

	if err := l.client.DeleteMessageSession(ctx, l.session.RunnerScaleSet.Id, l.session.SessionId); err != nil {
		l.logger.Error(err, "❌ DELETE SESSION FAILED", "scaleSetID", l.scaleSetID, "sessionId", l.session.SessionId)
		return fmt.Errorf("failed to delete message session: %w", err)
	}

	l.logger.Info("✅ MESSAGE SESSION DELETED", "scaleSetID", l.scaleSetID, "sessionId", l.session.SessionId)
	return nil
}
