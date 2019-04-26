// Copyright (c) 2017 Uber Technologies, Inc.
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

package cassandra

import (
	"context"
)

type (
	BatchType         byte
	ColumnKind        int
	ColumnOrder       bool
	Consistency       uint16
	RetryType         uint16
	SerialConsistency uint16
	Type              int

	Session interface {
		SetConsistency(cons Consistency)
		SetPageSize(n int)
		SetPrefetch(p float64)
		SetTrace(trace Tracer)
		Query(stmt string, values ...interface{}) Query
		Bind(stmt string, b func(q QueryInfo) ([]interface{}, error)) Query
		Close()
		Closed() bool
		KeyspaceMetadata(keyspace string) (KeyspaceMetadata, error)
		ExecuteBatch(batch Batch) error
		ExecuteBatchCAS(batch Batch, dest ...interface{}) (applied bool, iter Iter, err error)
		MapExecuteBatchCAS(batch Batch, dest map[string]interface{}) (applied bool, iter Iter, err error)
		NewBatch(typ BatchType) Batch
	}

	Query interface {
		String() string
		Attempts() int
		Latency() int64
		Consistency(c Consistency) Query
		GetConsistency() Consistency
		Trace(trace Tracer) Query
		PageSize(n int) Query
		DefaultTimestamp(enable bool) Query
		WithTimestamp(timestamp int64) Query
		RoutingKey(routingKey []byte) Query
		WithContext(ctx context.Context) Query
		GetRoutingKey() ([]byte, error)
		Prefetch(p float64) Query
		RetryPolicy(r RetryPolicy) Query
		Bind(v ...interface{}) Query
		SerialConsistency(cons SerialConsistency) Query
		PageState(state []byte) Query
		NoSkipMetadata() Query
		Exec() error
		MapScan(m map[string]interface{}) error
		Scan(dest ...interface{}) error
		ScanCAS(dest ...interface{}) (applied bool, err error)
		MapScanCAS(dest map[string]interface{}) (applied bool, err error)
		Release()
	}

	Batch interface {
		Attempts() int
		Latency() int64
		GetConsistency() Consistency
		Query(stmt string, args ...interface{})
		Bind(stmt string, bind func(q QueryInfo) ([]interface{}, error))
		RetryPolicy(r RetryPolicy) Batch
		WithContext(ctx context.Context) Batch
		Size() int
		SerialConsistency(cons SerialConsistency) Batch
		DefaultTimestamp(enable bool) Batch
		WithTimestamp(timestamp int64) Batch
		GetRoutingKey() ([]byte, error)
	}

	KeyspaceMetadata interface {
		GetName() string
		IsDurableWrites() bool
		GetStrategyClass() string
		GetStrategyOptions() map[string]interface{}
		GetTables() map[string]TableMetadata
	}

	TableMetadata interface {
		GetKeyspace() string
		GetName() string
		GetKeyValidator() string
		GetComparator() string
		GetDefaultValidator() string
		GetKeyAliases() []string
		GetColumnAliases() []string
		GetValueAlias() string
		GetPartitionKey() []ColumnMetadata
		GetClusteringColumns() []ColumnMetadata
		GetColumns() map[string]ColumnMetadata
		GetOrderedColumns() []string
	}

	ColumnMetadata interface {
		GetKeyspace() string
		GetTable() string
		GetName() string
		GetComponentIndex() int
		GetKind() ColumnKind
		GetValidator() string
		GetType() TypeInfo
		GetClusteringOrder() string
		GetOrder() ColumnOrder
		GetIndex() ColumnIndexMetadata
	}

	TypeInfo interface {
		Type() Type
		Version() byte
		Custom() string

		New() interface{}
	}

	ColumnIndexMetadata interface {
		GetName() string
		GetType() string
		GetOptions() map[string]interface{}
	}

	QueryInfo interface {
		GetID() []byte
		GetArgs() []ColumnInfo
		GetRval() []ColumnInfo
		GetPKeyColumns() []int
	}

	RetryPolicy interface {
		Attempt(RetryableQuery) bool
	}

	RetryableQuery interface {
		Attempts() int
		GetConsistency() Consistency
	}

	HostInfo interface {
	}

	ColumnInfo interface {
		GetKeyspace() string
		GetTable() string
		GetName() string
		GetTypeInfo() TypeInfo
	}

	Tracer interface {
		Trace(traceId []byte)
	}

	Iter interface {
		Host() HostInfo
		Columns() []ColumnInfo
	}
)
