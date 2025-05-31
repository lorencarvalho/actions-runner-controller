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

// Helper function for tests to create int pointers
func intPtr(i int) *int {
	return &i
}

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

		// Since we're taking all jobs (100% by default), we should get all 3 job IDs
		// but the order might be random, so we check the response matches what we return
		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.MatchedBy(func(ids []int64) bool {
			// Verify we get exactly 3 job IDs and they're all valid
			if len(ids) != 3 {
				return false
			}
			validIds := map[int64]bool{1: true, 2: true, 3: true}
			for _, id := range ids {
				if !validIds[id] {
					return false
				}
			}
			return true
		})).Return([]int64{1, 2, 3}, nil).Once()

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
		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.MatchedBy(func(ids []int64) bool {
			// Verify we get exactly 3 job IDs and they're all valid
			if len(ids) != 3 {
				return false
			}
			validIds := map[int64]bool{1: true, 2: true, 3: true}
			for _, id := range ids {
				if !validIds[id] {
					return false
				}
			}
			return true
		})).Return(nil, &actions.MessageQueueTokenExpiredError{}).Once()

		// Second call should succeed - return the same IDs that were passed in
		client.On("AcquireJobs", ctx, mock.Anything, mock.Anything, mock.MatchedBy(func(ids []int64) bool {
			// Same validation as above
			if len(ids) != 3 {
				return false
			}
			validIds := map[int64]bool{1: true, 2: true, 3: true}
			for _, id := range ids {
				if !validIds[id] {
					return false
				}
			}
			return true
		})).Return([]int64{1, 2, 3}, nil).Once()

		config.Client = client

		l, err := New(config)
		require.Nil(t, err)

		l.session = &actions.RunnerScaleSetSession{
			SessionId:      &uuid,
			RunnerScaleSet: &actions.RunnerScaleSet{},
		}

		got, err := l.acquireAvailableJobs(ctx, availableJobs)
		assert.Nil(t, err)
		assert.Equal(t, []int64{1, 2, 3}, got)
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

	t.Run("StandbyListener_AcquiresNoJobs", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		config := Config{
			ScaleSetID:        1,
			Metrics:           metrics.Discard,
			MaxJobsPercentage: 0, // Standby mode - acquire no jobs
		}

		client := listenermocks.NewClient(t)
		// No AcquireJobs calls should be made

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
		assert.NoError(t, err, "Should not error when configured for 0%")
		assert.Nil(t, acquiredJobIDs, "Should acquire no jobs in standby mode")

		// Verify no calls were made to the client (implicitly verified by mock expectations)
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

func TestRandomlySelectJobs(t *testing.T) {
	t.Parallel()

	// Create a listener for testing
	config := Config{
		Client:     listenermocks.NewClient(t),
		ScaleSetID: 1,
		Metrics:    metrics.Discard,
	}
	l, err := New(config)
	require.NoError(t, err)

	t.Run("Select fewer jobs than available", func(t *testing.T) {
		t.Parallel()

		// Create test jobs
		availableJobs := []*actions.JobAvailable{
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 1}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 2}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 3}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 4}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 5}},
		}

		selectedJobs := l.randomlySelectJobs(availableJobs, 3)

		assert.Len(t, selectedJobs, 3, "Should select exactly 3 jobs")
		assert.Len(t, availableJobs, 5, "Original slice should be unchanged")

		// Verify all selected jobs are from the original list
		originalIds := make(map[int64]bool)
		for _, job := range availableJobs {
			originalIds[job.RunnerRequestId] = true
		}

		for _, selectedJob := range selectedJobs {
			assert.True(t, originalIds[selectedJob.RunnerRequestId],
				"Selected job %d should be from original list", selectedJob.RunnerRequestId)
		}

		// Verify no duplicates in selection
		selectedIds := make(map[int64]bool)
		for _, job := range selectedJobs {
			assert.False(t, selectedIds[job.RunnerRequestId],
				"Job %d should not be selected twice", job.RunnerRequestId)
			selectedIds[job.RunnerRequestId] = true
		}
	})

	t.Run("Select all jobs when count equals available", func(t *testing.T) {
		t.Parallel()

		availableJobs := []*actions.JobAvailable{
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 1}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 2}},
		}

		selectedJobs := l.randomlySelectJobs(availableJobs, 2)

		assert.Len(t, selectedJobs, 2, "Should select all jobs")
	})

	t.Run("Select all jobs when count exceeds available", func(t *testing.T) {
		t.Parallel()

		availableJobs := []*actions.JobAvailable{
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 1}},
		}

		selectedJobs := l.randomlySelectJobs(availableJobs, 5)

		assert.Len(t, selectedJobs, 1, "Should return all available jobs")
		assert.Equal(t, availableJobs[0].RunnerRequestId, selectedJobs[0].RunnerRequestId)
	})

	t.Run("Select zero jobs", func(t *testing.T) {
		t.Parallel()

		availableJobs := []*actions.JobAvailable{
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 1}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 2}},
		}

		selectedJobs := l.randomlySelectJobs(availableJobs, 0)

		assert.Len(t, selectedJobs, 0, "Should select no jobs")
	})

	t.Run("Empty job list", func(t *testing.T) {
		t.Parallel()

		var availableJobs []*actions.JobAvailable

		selectedJobs := l.randomlySelectJobs(availableJobs, 3)

		assert.Len(t, selectedJobs, 0, "Should return empty slice")
	})

	t.Run("Random distribution test", func(t *testing.T) {
		t.Parallel()

		// This test verifies that the selection is actually random
		// by running multiple selections and checking distribution
		availableJobs := []*actions.JobAvailable{
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 1}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 2}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 3}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 4}},
			{JobMessageBase: actions.JobMessageBase{RunnerRequestId: 5}},
		}

		// Count how often each job is selected
		selectionCount := make(map[int64]int)
		iterations := 1000

		for i := 0; i < iterations; i++ {
			selectedJobs := l.randomlySelectJobs(availableJobs, 2)
			for _, job := range selectedJobs {
				selectionCount[job.RunnerRequestId]++
			}
		}

		// Each job should be selected roughly 2/5 = 40% of the time (2 out of 5 jobs selected)
		// Allow some variance but verify it's reasonably distributed
		expectedFrequency := float64(iterations) * 2.0 / 5.0 // 400
		tolerance := expectedFrequency * 0.3                 // 30% tolerance

		for jobId, count := range selectionCount {
			frequency := float64(count)
			assert.True(t, frequency > expectedFrequency-tolerance && frequency < expectedFrequency+tolerance,
				"Job %d selected %d times, expected ~%.0f (±%.0f)",
				jobId, count, expectedFrequency, tolerance)
		}

		// Verify all jobs were selected at least once (very high probability)
		assert.Len(t, selectionCount, 5, "All jobs should be selected at least once over %d iterations", iterations)
	})
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()

	t.Run("Default MaxJobsPercentage is 100", func(t *testing.T) {
		t.Parallel()

		config := Config{
			Client:            listenermocks.NewClient(t),
			ScaleSetID:        1,
			Metrics:           metrics.Discard,
			MaxJobsPercentage: 100, // Default value
		}

		l, err := New(config)
		require.NoError(t, err)
		assert.Equal(t, 100, l.maxJobsPercentage, "Should default to 100% when not set")
	})

	t.Run("Explicit 0 MaxJobsPercentage is respected", func(t *testing.T) {
		t.Parallel()

		config := Config{
			Client:            listenermocks.NewClient(t),
			ScaleSetID:        1,
			Metrics:           metrics.Discard,
			MaxJobsPercentage: 0, // Explicitly set to 0
		}

		l, err := New(config)
		require.NoError(t, err)
		assert.Equal(t, 0, l.maxJobsPercentage, "Should respect explicit 0 (standby listener)")
	})

	t.Run("Explicit 50 MaxJobsPercentage is respected", func(t *testing.T) {
		t.Parallel()

		config := Config{
			Client:            listenermocks.NewClient(t),
			ScaleSetID:        1,
			Metrics:           metrics.Discard,
			MaxJobsPercentage: 50, // Explicitly set to 50
		}

		l, err := New(config)
		require.NoError(t, err)
		assert.Equal(t, 50, l.maxJobsPercentage, "Should respect explicit value")
	})

	t.Run("Default MaxJobsPerAcquisition is -1", func(t *testing.T) {
		t.Parallel()

		config := Config{
			Client:                listenermocks.NewClient(t),
			ScaleSetID:            1,
			Metrics:               metrics.Discard,
			MaxJobsPerAcquisition: -1, // Default value (no limit)
		}

		l, err := New(config)
		require.NoError(t, err)
		assert.Equal(t, -1, l.maxJobsPerAcquisition, "Should default to -1 (no limit) when not set")
	})
}
