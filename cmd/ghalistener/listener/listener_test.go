package listener

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	listenermocks "github.com/actions/actions-runner-controller/cmd/ghalistener/listener/mocks"
	"github.com/actions/actions-runner-controller/cmd/ghalistener/metrics"
	"github.com/actions/actions-runner-controller/github/actions"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("InvalidConfig", func(t *testing.T) {
		t.Parallel()
		var config Config
		_, err := New(config)
		assert.NotNil(t, err)
	})

	t.Run("ValidConfig", func(t *testing.T) {
		t.Parallel()
		config := Config{
			Client:     listenermocks.NewClient(t),
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}
		l, err := New(config)
		assert.Nil(t, err)
		assert.NotNil(t, l)
	})
}

func TestListener_createSession(t *testing.T) {
	t.Parallel()
	t.Run("FailOnce", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)
		client.On("CreateMessageSession", ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		err = l.createSession(ctx)
		assert.NotNil(t, err)
	})

	t.Run("FailContext", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)
		client.On("CreateMessageSession", ctx, mock.Anything, mock.Anything).Return(nil,
			&actions.HttpClientSideError{Code: http.StatusConflict}).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		err = l.createSession(ctx)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("SetsSession", func(t *testing.T) {
		t.Parallel()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("CreateMessageSession", mock.Anything, mock.Anything, mock.Anything).Return(session, nil).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		err = l.createSession(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, session, l.session)
	})
}

func TestListener_getMessage(t *testing.T) {
	t.Parallel()

	t.Run("ReceivesMessage", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
			MaxRunners: 10,
		}

		client := listenermocks.NewClient(t)
		want := &actions.RunnerScaleSetMessage{
			MessageId: 1,
		}
		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).Return(want, nil).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)
		l.session = &actions.RunnerScaleSetSession{}

		got, err := l.getMessage(ctx)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("NotExpiredError", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
			MaxRunners: 10,
		}

		client := listenermocks.NewClient(t)
		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).Return(nil, &actions.HttpClientSideError{Code: http.StatusNotFound}).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{}

		_, err = l.getMessage(ctx)
		assert.NotNil(t, err)
	})

	t.Run("RefreshAndSucceeds", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
			MaxRunners: 10,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).Return(nil, &actions.MessageQueueTokenExpiredError{}).Once()

		want := &actions.RunnerScaleSetMessage{
			MessageId: 1,
		}
		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).Return(want, nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &uuid,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		got, err := l.getMessage(ctx)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("RefreshAndFails", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
			MaxRunners: 10,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).Return(nil, &actions.MessageQueueTokenExpiredError{}).Twice()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &uuid,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		got, err := l.getMessage(ctx)
		assert.NotNil(t, err)
		assert.Nil(t, got)
	})
}

func TestListener_refreshSession(t *testing.T) {
	t.Parallel()

	t.Run("SuccessfullyRefreshes", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		newUUID := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &newUUID,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		oldUUID := uuid.New()
		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &oldUUID,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		err = l.refreshSession(ctx)
		assert.Nil(t, err)
		assert.Equal(t, session, l.session)
	})

	t.Run("FailsToRefresh", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(nil, errors.New("error")).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		oldUUID := uuid.New()
		oldSession := &actions.RunnerScaleSetSession{
			SessionId:      &oldUUID,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}
		l.session = oldSession

		err = l.refreshSession(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, oldSession, l.session)
	})
}

func TestListener_deleteLastMessage(t *testing.T) {
	t.Parallel()

	t.Run("SuccessfullyDeletes", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		client.On("DeleteMessage", ctx, mock.Anything, mock.Anything, mock.MatchedBy(func(lastMessageID any) bool {
			return lastMessageID.(int64) == int64(5)
		})).Return(nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{}
		l.lastMessageID = 5

		err = l.deleteLastMessage(ctx)
		assert.Nil(t, err)
	})

	t.Run("FailsToDelete", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		client.On("DeleteMessage", ctx, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error")).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{}
		l.lastMessageID = 5

		err = l.deleteLastMessage(ctx)
		assert.NotNil(t, err)
	})

	t.Run("RefreshAndSucceeds", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		newUUID := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &newUUID,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		client.On("DeleteMessage", ctx, mock.Anything, mock.Anything, mock.Anything).Return(&actions.MessageQueueTokenExpiredError{}).Once()

		client.On("DeleteMessage", ctx, mock.Anything, mock.Anything, mock.MatchedBy(func(lastMessageID any) bool {
			return lastMessageID.(int64) == int64(5)
		})).Return(nil).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		oldUUID := uuid.New()
		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &oldUUID,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}
		l.lastMessageID = 5

		config.Client = client

		err = l.deleteLastMessage(ctx)
		assert.NoError(t, err)
	})

	t.Run("RefreshAndFails", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		newUUID := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &newUUID,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		client.On("DeleteMessage", ctx, mock.Anything, mock.Anything, mock.Anything).Return(&actions.MessageQueueTokenExpiredError{}).Twice()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		oldUUID := uuid.New()
		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &oldUUID,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}
		l.lastMessageID = 5

		config.Client = client

		err = l.deleteLastMessage(ctx)
		assert.Error(t, err)
	})
}

func TestListener_Listen(t *testing.T) {
	t.Parallel()

	t.Run("CreateSessionFails", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)
		client.On("CreateMessageSession", ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()
		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		err = l.Listen(ctx, nil)
		assert.NotNil(t, err)
	})

	t.Run("CallHandleRegardlessOfInitialMessage", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              &actions.RunnerScaleSetStatistic{},
		}
		client.On("CreateMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()
		client.On("DeleteMessageSession", mock.Anything, session.RunnerScaleSet.Id, session.SessionId).Return(nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		var called bool
		handler := listenermocks.NewHandler(t)
		handler.On("HandleDesiredRunnerCount", mock.Anything, mock.Anything, 0).
			Return(0, nil).
			Run(
				func(mock.Arguments) {
					called = true
					cancel()
				},
			).
			Once()

		err = l.Listen(ctx, handler)
		assert.True(t, errors.Is(err, context.Canceled))
		assert.True(t, called)
	})

	t.Run("CancelContextAfterGetMessage", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())

		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
			MaxRunners: 10,
		}

		client := listenermocks.NewClient(t)
		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              &actions.RunnerScaleSetStatistic{},
		}
		client.On("CreateMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()
		client.On("DeleteMessageSession", mock.Anything, session.RunnerScaleSet.Id, session.SessionId).Return(nil).Once()

		msg := &actions.RunnerScaleSetMessage{
			MessageId:   1,
			MessageType: "RunnerScaleSetJobMessages",
			Statistics:  &actions.RunnerScaleSetStatistic{},
		}
		client.On("GetMessage", ctx, mock.Anything, mock.Anything, mock.Anything, 10).
			Return(msg, nil).
			Run(
				func(mock.Arguments) {
					cancel()
				},
			).
			Once()

		// Ensure delete message is called without cancel
		client.On("DeleteMessage", context.WithoutCancel(ctx), mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		config.Client = client

		handler := listenermocks.NewHandler(t)
		handler.On("HandleDesiredRunnerCount", mock.Anything, mock.Anything, 0).
			Return(0, nil).
			Once()

		handler.On("HandleDesiredRunnerCount", mock.Anything, mock.Anything, 0).
			Return(0, nil).
			Once()

		l, err := New(config)
		require.Nil(t, err)

		err = l.Listen(ctx, handler)
		assert.ErrorIs(t, context.Canceled, err)
	})
}

func TestListener_acquireAvailableJobs(t *testing.T) {
	t.Parallel()

	t.Run("FailingToAcquireJobs", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		uuid := uuid.New()
		l.session = &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              &actions.RunnerScaleSetStatistic{},
		}

		availableJobs := []*actions.JobAvailable{
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 1,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 2,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 3,
				},
			},
		}
		_, err = l.acquireAvailableJobs(ctx, availableJobs)
		assert.Error(t, err)
	})

	t.Run("SuccessfullyAcquiresJobsOnFirstRun", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		jobIDs := []int64{1, 2, 3}

		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.Anything).Return(jobIDs, nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		uuid := uuid.New()
		l.session = &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              &actions.RunnerScaleSetStatistic{},
		}

		availableJobs := []*actions.JobAvailable{
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 1,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 2,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 3,
				},
			},
		}
		acquiredJobIDs, err := l.acquireAvailableJobs(ctx, availableJobs)
		assert.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, acquiredJobIDs)
	})

	t.Run("RefreshAndSucceeds", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		// Second call to AcquireJobs will succeed
		want := []int64{1, 2, 3}
		availableJobs := []*actions.JobAvailable{
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 1,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 2,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 3,
				},
			},
		}

		// First call to AcquireJobs will fail with a token expired error
		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				ids := args.Get(3).([]int64)
				assert.Equal(t, want, ids)
			}).
			Return(nil, &actions.MessageQueueTokenExpiredError{}).
			Once()

		// Second call should succeed
		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				ids := args.Get(3).([]int64)
				assert.Equal(t, want, ids)
			}).
			Return(want, nil).
			Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &uuid,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		got, err := l.acquireAvailableJobs(ctx, availableJobs)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("RefreshAndFails", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID: 1,
			Metrics:    metrics.Discard,
		}

		client := listenermocks.NewClient(t)

		uuid := uuid.New()
		session := &actions.RunnerScaleSetSession{
			SessionId:               &uuid,
			OwnerName:               "example",
			RunnerScaleSet:          &actions.RunnerScaleSet{},
			MessageQueueUrl:         "https://example.com",
			MessageQueueAccessToken: "1234567890",
			Statistics:              nil,
		}
		client.On("RefreshMessageSession", ctx, mock.Anything, mock.Anything).Return(session, nil).Once()

		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, &actions.MessageQueueTokenExpiredError{}).Twice()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &uuid,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		availableJobs := []*actions.JobAvailable{
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 1,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 2,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					RunnerRequestId: 3,
				},
			},
		}

		got, err := l.acquireAvailableJobs(ctx, availableJobs)
		assert.NotNil(t, err)
		assert.Nil(t, got)
	})
}

func TestListener_parseMessage(t *testing.T) {
	t.Run("FailOnEmptyStatistics", func(t *testing.T) {
		msg := &actions.RunnerScaleSetMessage{
			MessageId:   1,
			MessageType: "RunnerScaleSetJobMessages",
			Statistics:  nil,
		}

		l := &Listener{}
		parsedMsg, err := l.parseMessage(context.Background(), msg)
		assert.Error(t, err)
		assert.Nil(t, parsedMsg)
	})

	t.Run("FailOnIncorrectMessageType", func(t *testing.T) {
		msg := &actions.RunnerScaleSetMessage{
			MessageId:   1,
			MessageType: "RunnerMessages", // arbitrary message type
			Statistics:  &actions.RunnerScaleSetStatistic{},
		}

		l := &Listener{}
		parsedMsg, err := l.parseMessage(context.Background(), msg)
		assert.Error(t, err)
		assert.Nil(t, parsedMsg)
	})

	t.Run("ParseAll", func(t *testing.T) {
		msg := &actions.RunnerScaleSetMessage{
			MessageId:   1,
			MessageType: "RunnerScaleSetJobMessages",
			Body:        "",
			Statistics: &actions.RunnerScaleSetStatistic{
				TotalAvailableJobs:     1,
				TotalAcquiredJobs:      2,
				TotalAssignedJobs:      3,
				TotalRunningJobs:       4,
				TotalRegisteredRunners: 5,
				TotalBusyRunners:       6,
				TotalIdleRunners:       7,
			},
		}

		var batchedMessages []any
		jobsAvailable := []*actions.JobAvailable{
			{
				AcquireJobUrl: "https://github.com/example",
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobAvailable,
					},
					RunnerRequestId: 1,
				},
			},
			{
				AcquireJobUrl: "https://github.com/example",
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobAvailable,
					},
					RunnerRequestId: 2,
				},
			},
		}
		for _, msg := range jobsAvailable {
			batchedMessages = append(batchedMessages, msg)
		}

		jobsAssigned := []*actions.JobAssigned{
			{
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobAssigned,
					},
					RunnerRequestId: 3,
				},
			},
			{
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobAssigned,
					},
					RunnerRequestId: 4,
				},
			},
		}
		for _, msg := range jobsAssigned {
			batchedMessages = append(batchedMessages, msg)
		}

		jobsStarted := []*actions.JobStarted{
			{
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobStarted,
					},
					RunnerRequestId: 5,
				},
				RunnerId:   2,
				RunnerName: "runner2",
			},
		}
		for _, msg := range jobsStarted {
			batchedMessages = append(batchedMessages, msg)
		}

		jobsCompleted := []*actions.JobCompleted{
			{
				JobMessageBase: actions.JobMessageBase{
					JobMessageType: actions.JobMessageType{
						MessageType: messageTypeJobCompleted,
					},
					RunnerRequestId: 6,
				},
				Result:     "success",
				RunnerId:   1,
				RunnerName: "runner1",
			},
		}
		for _, msg := range jobsCompleted {
			batchedMessages = append(batchedMessages, msg)
		}

		b, err := json.Marshal(batchedMessages)
		require.NoError(t, err)

		msg.Body = string(b)

		l := &Listener{}
		parsedMsg, err := l.parseMessage(context.Background(), msg)
		require.NoError(t, err)

		assert.Equal(t, msg.Statistics, parsedMsg.statistics)
		assert.Equal(t, jobsAvailable, parsedMsg.jobsAvailable)
		assert.Equal(t, jobsStarted, parsedMsg.jobsStarted)
		assert.Equal(t, jobsCompleted, parsedMsg.jobsCompleted)
	})
}

func TestCalculateJobLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		totalJobs             int
		maxJobsPercentage     int
		maxJobsPerAcquisition int
		expectedLimit         int
		description           string
	}{
		{
			name:                  "Default behavior - 100% with no absolute limit",
			totalJobs:             100,
			maxJobsPercentage:     100,
			maxJobsPerAcquisition: 0,
			expectedLimit:         100,
			description:           "Should acquire all jobs when percentage is 100% and no absolute limit",
		},
		{
			name:                  "Percentage limit applied - 17% of 600 jobs",
			totalJobs:             600,
			maxJobsPercentage:     17,
			maxJobsPerAcquisition: 0,
			expectedLimit:         102, // 600 * 17% = 102
			description:           "Should acquire 17% of 600 jobs for fair distribution across 6 clusters",
		},
		{
			name:                  "Absolute limit applied when smaller than percentage",
			totalJobs:             1000,
			maxJobsPercentage:     50,
			maxJobsPerAcquisition: 200,
			expectedLimit:         200,
			description:           "Should use absolute limit when it's smaller than percentage limit",
		},
		{
			name:                  "Percentage limit applied when smaller than absolute",
			totalJobs:             100,
			maxJobsPercentage:     20,
			maxJobsPerAcquisition: 50,
			expectedLimit:         20, // 100 * 20% = 20, which is less than 50
			description:           "Should use percentage limit when it's smaller than absolute limit",
		},
		{
			name:                  "Zero jobs available",
			totalJobs:             0,
			maxJobsPercentage:     50,
			maxJobsPerAcquisition: 10,
			expectedLimit:         0,
			description:           "Should return 0 when no jobs are available",
		},
		{
			name:                  "Edge case - 1% of large number",
			totalJobs:             10000,
			maxJobsPercentage:     1,
			maxJobsPerAcquisition: 0,
			expectedLimit:         100, // 10000 * 1% = 100
			description:           "Should handle small percentages correctly",
		},
		{
			name:                  "Edge case - very small percentage rounds down",
			totalJobs:             10,
			maxJobsPercentage:     5,
			maxJobsPerAcquisition: 0,
			expectedLimit:         0, // 10 * 5% = 0.5, rounds down to 0
			description:           "Should round down when percentage calculation results in fractional jobs",
		},
		{
			name:                  "Edge case - very small percentage rounds up to 1",
			totalJobs:             100,
			maxJobsPercentage:     1,
			maxJobsPerAcquisition: 0,
			expectedLimit:         1, // 100 * 1% = 1
			description:           "Should correctly calculate small percentages that result in whole numbers",
		},
		{
			name:                  "Both limits equal",
			totalJobs:             200,
			maxJobsPercentage:     25,
			maxJobsPerAcquisition: 50,
			expectedLimit:         50, // 200 * 25% = 50, equal to absolute limit
			description:           "Should work correctly when both limits result in the same value",
		},
		{
			name:                  "Zero percentage means no jobs",
			totalJobs:             1000,
			maxJobsPercentage:     0,
			maxJobsPerAcquisition: 100,
			expectedLimit:         0, // 0% means no jobs regardless of absolute limit
			description:           "Should return 0 when percentage is 0, even with absolute limit set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := calculateJobLimitWithParams(tt.totalJobs, tt.maxJobsPercentage, tt.maxJobsPerAcquisition)
			assert.Equal(t, tt.expectedLimit, result, tt.description)
		})
	}
}

func TestCalculateJobLimitRealWorldScenarios(t *testing.T) {
	t.Parallel()

	t.Run("6-cluster deployment scenario", func(t *testing.T) {
		t.Parallel()

		// Simulate 6 clusters each configured with 17% (approximately 1/6)
		scenarios := []struct {
			totalJobs     int
			expectedLimit int
		}{
			{600, 102},  // 600 * 17% = 102 jobs per cluster
			{300, 51},   // 300 * 17% = 51 jobs per cluster
			{60, 10},    // 60 * 17% = 10.2 → 10 jobs per cluster
			{1200, 204}, // 1200 * 17% = 204 jobs per cluster
		}

		for _, scenario := range scenarios {
			result := calculateJobLimitWithParams(scenario.totalJobs, 17, 0)
			assert.Equal(t, scenario.expectedLimit, result,
				"For %d total jobs, each of 6 clusters should get ~%d jobs",
				scenario.totalJobs, scenario.expectedLimit)
		}
	})

	t.Run("Combined percentage and absolute limits", func(t *testing.T) {
		t.Parallel()

		// Real scenario: limit to 20% but never more than 100 jobs per acquisition
		testCases := []struct {
			totalJobs     int
			expectedLimit int
			reason        string
		}{
			{200, 40, "20% of 200 = 40, under absolute limit"},
			{500, 100, "20% of 500 = 100, equals absolute limit"},
			{1000, 100, "20% of 1000 = 200, but capped at absolute limit of 100"},
		}

		for _, tc := range testCases {
			result := calculateJobLimitWithParams(tc.totalJobs, 20, 100)
			assert.Equal(t, tc.expectedLimit, result, tc.reason)
		}
	})
}

func TestCalculateJobLimitBoundaryConditions(t *testing.T) {
	t.Parallel()

	t.Run("Maximum percentage", func(t *testing.T) {
		t.Parallel()
		result := calculateJobLimitWithParams(100, 100, 0)
		assert.Equal(t, 100, result, "100% should return all jobs")
	})

	t.Run("Minimum percentage", func(t *testing.T) {
		t.Parallel()
		result := calculateJobLimitWithParams(100, 0, 0)
		assert.Equal(t, 0, result, "0% should return no jobs")
	})

	t.Run("Large job count", func(t *testing.T) {
		t.Parallel()
		result := calculateJobLimitWithParams(1000000, 1, 0)
		assert.Equal(t, 10000, result, "Should handle large numbers correctly")
	})

	t.Run("No absolute limit (zero)", func(t *testing.T) {
		t.Parallel()
		result := calculateJobLimitWithParams(500, 50, 0)
		assert.Equal(t, 250, result, "Zero absolute limit means no cap on percentage")
	})
}
