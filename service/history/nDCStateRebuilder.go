// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	ctx "context"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCStateRebuilder interface {
		prepareMutableState(
			ctx ctx.Context,
			branchIndex int,
			incomingVersion int64,
		) (mutableState, bool, error)
	}

	nDCStateRebuilderImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryV2Manager

		context      workflowExecutionContext
		mutableState mutableState
		historySize  int64
		logger       log.Logger
	}
)

var _ nDCStateRebuilder = (*nDCStateRebuilderImpl)(nil)

func newNDCStateRebuilder(
	shard ShardContext,

	context workflowExecutionContext,
	mutableState mutableState,
	logger log.Logger,
) *nDCStateRebuilderImpl {

	return &nDCStateRebuilderImpl{
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    shard.GetHistoryV2Manager(),

		context:      context,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *nDCStateRebuilderImpl) prepareMutableState(
	ctx ctx.Context,
	branchIndex int,
	incomingVersion int64,
) (mutableState, bool, error) {

	// NOTE: this function also need to preserve whether a workflow is a zombie or not
	//  this is done by the rebuild function below

	versionHistories := r.mutableState.GetVersionHistories()
	currentVersionHistoryIndex := versionHistories.GetCurrentVersionHistoryIndex()

	// replication task to be applied to current branch
	if branchIndex == currentVersionHistoryIndex {
		return r.mutableState, false, nil
	}

	currentVersionHistory, err := versionHistories.GetVersionHistory(currentVersionHistoryIndex)
	if err != nil {
		return nil, false, err
	}
	currentLastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return nil, false, err
	}

	// mutable state does not need rebuild
	if incomingVersion < currentLastItem.GetVersion() {
		return r.mutableState, false, nil
	}

	if incomingVersion == currentLastItem.GetVersion() {
		return nil, false, &shared.BadRequestError{
			Message: "nDCStateRebuilder encounter replication task version == current branch last write version",
		}
	}

	// task.getVersion() > currentLastItem
	// incoming replication task, after application, will become the current branch
	// (because higher version wins), we need to rebuild the mutable state for that
	rebuildMutableState, err := r.rebuild(ctx, branchIndex, uuid.New())
	if err != nil {
		return nil, false, err
	}
	return rebuildMutableState, true, nil
}

func (r *nDCStateRebuilderImpl) rebuild(
	ctx ctx.Context,
	branchIndex int,
	requestID string,
) (mutableState, error) {

	versionHistories := r.mutableState.GetVersionHistories()
	replayVersionHistory, err := versionHistories.GetVersionHistory(branchIndex)
	if err != nil {
		return nil, err
	}
	lastItem, err := replayVersionHistory.GetLastItem()
	if err != nil {
		return nil, err
	}

	iter := collection.NewPagingIterator(r.getPaginationFn(
		common.FirstEventID,
		lastItem.GetEventID()+1,
		replayVersionHistory.GetBranchToken(),
	))

	// need to specially handling the first batch, to initialize mutable state & state builder
	batch, err := iter.Next()
	if err != nil {
		return nil, err
	}
	firstEventBatch := batch.(*shared.History).Events
	rebuildMutableState, stateBuilder := r.initializeBuilders(firstEventBatch[0].GetVersion())
	if err := r.applyEvents(stateBuilder, firstEventBatch, requestID); err != nil {
		return nil, err
	}

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return nil, err
		}
		events := batch.(*shared.History).Events
		if err := r.applyEvents(stateBuilder, events, requestID); err != nil {
			return nil, err
		}
	}

	// after rebuilt verification
	rebuildVersionHistories := rebuildMutableState.GetVersionHistories()
	rebuildVersionHistory, err := rebuildVersionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	err = rebuildVersionHistory.SetBranchToken(replayVersionHistory.GetBranchToken())
	if err != nil {
		return nil, err
	}

	if !rebuildVersionHistory.Equals(replayVersionHistory) {
		return nil, &shared.InternalServiceError{
			Message: "nDCStateRebuilder encounter mismatch version history after rebuild",
		}
	}

	// set the current branch index to target branch index
	// set the version history back
	//
	// caller can use the IsRebuilt function in VersionHistories
	// telling whether mutable state is rebuilt, before apply new history events
	if err := versionHistories.SetCurrentVersionHistoryIndex(branchIndex); err != nil {
		return nil, err
	}
	if err = rebuildMutableState.SetVersionHistories(versionHistories); err != nil {
		return nil, err
	}

	r.context.clear()
	r.context.setHistorySize(r.historySize)
	return rebuildMutableState, nil
}

func (r *nDCStateRebuilderImpl) initializeBuilders(
	version int64,
) (mutableState, stateBuilder) {
	resetMutableStateBuilder := newMutableStateBuilderWithVersionHistories(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		version,
		// if can see replication task, meaning that domain is
		// global domain with > 1 target clusters
		cache.ReplicationPolicyMultiCluster,
	)
	resetMutableStateBuilder.executionInfo.EventStoreVersion = nDCMutableStateEventStoreVersion
	stateBuilder := newStateBuilder(r.shard, resetMutableStateBuilder, r.logger)
	return resetMutableStateBuilder, stateBuilder
}

func (r *nDCStateRebuilderImpl) applyEvents(
	stateBuilder stateBuilder,
	events []*shared.HistoryEvent,
	requestID string,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	_, _, _, err := stateBuilder.applyEvents(
		executionInfo.DomainID,
		requestID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		events,
		nil, // no new run history when rebuilding mutable state
		nDCMutableStateEventStoreVersion,
		nDCMutableStateEventStoreVersion,
	)
	if err != nil {
		r.logger.Error("nDCStateRebuilder unable to rebuild mutable state.", tag.Error(err))
		return err
	}
	return nil
}

func (r *nDCStateRebuilderImpl) getPaginationFn(
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn {

	executionInfo := r.mutableState.GetExecutionInfo()
	return func(paginationToken []byte) ([]interface{}, []byte, error) {

		_, historyBatches, token, size, err := PaginateHistory(
			nil,
			r.historyV2Mgr,
			nil,
			r.logger,
			true,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			firstEventID,
			nextEventID,
			paginationToken,
			nDCMutableStateEventStoreVersion,
			branchToken,
			nDCDefaultPageSize,
			common.IntPtr(r.shard.GetShardID()),
		)
		if err != nil {
			return nil, nil, err
		}
		r.historySize += int64(size)

		var paginateItems []interface{}
		for _, history := range historyBatches {
			paginateItems = append(paginateItems, history)
		}
		return paginateItems, token, nil
	}
}
