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
	"fmt"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCTransactionMgrForExistingWorkflow interface {
		dispatchForExistingWorkflow(
			ctx ctx.Context,
			now time.Time,
			isWorkflowRebuilt bool,
			targetWorkflow nDCWorkflow,
			newWorkflow nDCWorkflow,
		) error
	}

	nDCTransactionMgrForExistingWorkflowImpl struct {
		transactionMgr *nDCTransactionMgrImpl
	}
)

func newNDCTransactionMgrForExistingWorkflow(
	transactionMgr *nDCTransactionMgrImpl,
) *nDCTransactionMgrForExistingWorkflowImpl {

	return &nDCTransactionMgrForExistingWorkflowImpl{
		transactionMgr: transactionMgr,
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchForExistingWorkflow(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	// NOTE: this function does NOT mutate current workflow, target workflow or new workflow,
	//  workflow mutation is done in methods within executeTransaction function

	// this is a performance optimization so most update does not need to
	// check whether target workflow is current workflow by calling DB API
	if !isWorkflowRebuilt && targetWorkflow.getMutableState().IsCurrentWorkflowGuaranteed() {
		// NOTE: if target workflow is rebuilt, then IsCurrentWorkflowGuaranteed is not trustworthy

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			now,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	targetExecutionInfo := targetWorkflow.getMutableState().GetExecutionInfo()
	domainID := targetExecutionInfo.DomainID
	workflowID := targetExecutionInfo.WorkflowID
	targetRunID := targetExecutionInfo.RunID

	// the target workflow is rebuilt
	// we need to check the current workflow execution
	currentRunID, err := r.transactionMgr.getCurrentWorkflowRunID(
		ctx,
		domainID,
		workflowID,
	)
	if err != nil {
		return err
	}
	if currentRunID == "" {
		// this means a bug in our code or DB is inconsistent...
		return &shared.InternalServiceError{
			Message: "nDCTransactionMgr unable to locate current workflow during update",
		}
	}

	if currentRunID == targetRunID {
		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			now,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := loadNDCWorkflow(
		ctx,
		r.transactionMgr.domainCache,
		r.transactionMgr.historyCache,
		r.transactionMgr.clusterMetadata,
		domainID,
		workflowID,
		currentRunID,
	)
	if err != nil {
		return err
	}

	targetWorkflowIsNewer, err := targetWorkflow.happensAfter(currentWorkflow)
	if err != nil {
		return err
	}

	if !targetWorkflowIsNewer {
		// target workflow is older than current workflow, need to suppress the target workflow
		return r.dispatchWorkflowUpdateAsZombie(
			ctx,
			now,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	// isWorkflowRebuilt is irrelevant here, because the DB API to be used
	// will set target workflow using snapshot
	return r.executeTransaction(
		ctx,
		now,
		nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			now,
			nDCTransactionPolicyUpdateAsCurrent,
			nil,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		now,
		nDCTransactionPolicyConflictResolveAsCurrent,
		nil,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsZombie(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			now,
			nDCTransactionPolicyUpdateAsZombie,
			nil,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		now,
		nDCTransactionPolicyConflictResolveAsZombie,
		nil,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if newWorkflow == nil {
		return targetWorkflow.getContext().updateWorkflowExecutionAsPassive(now)
	}

	return targetWorkflow.getContext().updateWorkflowExecutionWithNewAsPassive(
		now,
		newWorkflow.getContext(),
		newWorkflow.getMutableState(),
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsZombie(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if err := targetWorkflow.suppressWorkflowBy(
		currentWorkflow,
	); err != nil {
		return err
	}

	var newContext workflowExecutionContext
	var newMutableState mutableState
	var newTransactionPolicy *transactionPolicy
	if newWorkflow != nil {
		if err := newWorkflow.suppressWorkflowBy(
			currentWorkflow,
		); err != nil {
			return err
		}

		newContext = newWorkflow.getContext()
		newMutableState = newWorkflow.getMutableState()
		newTransactionPolicy = transactionPolicyPassive.ptr()
	}

	return targetWorkflow.getContext().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		transactionPolicyPassive,
		newTransactionPolicy,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) suppressCurrentAndUpdateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if currentWorkflow.getMutableState().IsWorkflowExecutionRunning() {
		if err := currentWorkflow.suppressWorkflowBy(
			targetWorkflow,
		); err != nil {
			return err
		}
	}

	return targetWorkflow.getContext().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.getMutableState(),
		newWorkflow.getContext(),
		newWorkflow.getMutableState(),
		currentWorkflow.getContext(),
		currentWorkflow.getMutableState(),
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsCurrent(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	var newContext workflowExecutionContext
	var newMutableState mutableState
	if newWorkflow != nil {
		newContext = newWorkflow.getContext()
		newMutableState = newWorkflow.getMutableState()
	}

	return targetWorkflow.getContext().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.getMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsZombie(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	if err := targetWorkflow.suppressWorkflowBy(
		currentWorkflow,
	); err != nil {
		return err
	}

	var newContext workflowExecutionContext
	var newMutableState mutableState
	if newWorkflow != nil {
		if err := newWorkflow.suppressWorkflowBy(
			currentWorkflow,
		); err != nil {
			return err
		}
		newContext = newWorkflow.getContext()
		newMutableState = newWorkflow.getMutableState()
	}

	return targetWorkflow.getContext().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetWorkflow.getMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) executeTransaction(
	ctx ctx.Context,
	now time.Time,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) (retError error) {

	defer func() {
		r.cleanupTransaction(currentWorkflow, targetWorkflow, newWorkflow, retError)
	}()

	switch transactionPolicy {
	case nDCTransactionPolicyUpdateAsCurrent:
		return r.updateAsCurrent(
			ctx,
			now,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyUpdateAsZombie:
		return r.updateAsZombie(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent:
		return r.suppressCurrentAndUpdateAsCurrent(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsCurrent:
		return r.conflictResolveAsCurrent(
			ctx,
			now,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsZombie:
		return r.conflictResolveAsZombie(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	default:
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("nDCTransactionMgr encounter unknown transaction type: %v", transactionPolicy),
		}
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) cleanupTransaction(
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.getReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.getReleaseFn()(err)
	}
	if newWorkflow != nil {
		newWorkflow.getReleaseFn()(err)
	}
}
