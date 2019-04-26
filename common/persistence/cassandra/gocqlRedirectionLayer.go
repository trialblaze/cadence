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

	"github.com/gocql/gocql"
)

type (
	SessionImpl struct {
		session *gocql.Session
	}

	QueryImpl struct {
		query *gocql.Query
	}

	BatchImpl struct {
		batch *gocql.Batch
	}

	KeyspaceMetadataImpl struct {
		metadata *gocql.KeyspaceMetadata
	}

	TableMetadataImpl struct {
		metadata *gocql.TableMetadata
	}

	ColumnMetadataImpl struct {
		metadata *gocql.ColumnMetadata
	}

	TypeInfoImpl struct {
		info gocql.TypeInfo
	}

	ColumnIndexMetadataImpl struct {
		metadata *gocql.ColumnIndexMetadata
	}

	QueryInfoImpl struct {
		info *gocql.QueryInfo
	}

	RetryPolicyImpl struct {
		policy gocql.RetryPolicy
	}

	RetryableQueryImpl struct {
		query gocql.RetryableQuery
	}

	HostInfoImpl struct {
		info *gocql.HostInfo
	}

	ColumnInfoImpl struct {
		info *gocql.ColumnInfo
	}

	TracerImpl struct {
		tracer gocql.Tracer
	}

	IterImpl struct {
		iter *gocql.Iter
	}
)

var _ Session = (*SessionImpl)(nil)
var _ Query = (*QueryImpl)(nil)
var _ Batch = (*BatchImpl)(nil)
var _ KeyspaceMetadata = (*KeyspaceMetadataImpl)(nil)
var _ TableMetadata = (*TableMetadataImpl)(nil)
var _ ColumnMetadata = (*ColumnMetadataImpl)(nil)
var _ TypeInfo = (*TypeInfoImpl)(nil)
var _ ColumnIndexMetadata = (*ColumnIndexMetadataImpl)(nil)
var _ QueryInfo = (*QueryInfoImpl)(nil)
var _ RetryPolicy = (*RetryPolicyImpl)(nil)
var _ RetryableQuery = (*RetryableQueryImpl)(nil)
var _ HostInfo = (*HostInfoImpl)(nil)
var _ ColumnInfo = (*ColumnInfoImpl)(nil)
var _ Tracer = (*TracerImpl)(nil)
var _ Iter = (*IterImpl)(nil)

func NewSession(session *gocql.Session) *SessionImpl {
	return &SessionImpl{session: session}
}
func (g *SessionImpl) SetConsistency(cons Consistency) {
	g.session.SetConsistency(gocql.Consistency(cons))
}
func (g *SessionImpl) SetPageSize(n int) {
	g.session.SetPageSize(n)
}
func (g *SessionImpl) SetPrefetch(p float64) {
	g.session.SetPrefetch(p)
}
func (g *SessionImpl) SetTrace(trace Tracer) {
	g.session.SetTrace(trace)
}
func (g *SessionImpl) Query(stmt string, values ...interface{}) Query {
	return NewQuery(g.session.Query(stmt, values))
}
func (g *SessionImpl) Bind(stmt string, b func(q QueryInfo) ([]interface{}, error)) Query {
	return NewQuery(g.session.Bind(stmt, func(q *gocql.QueryInfo) ([]interface{}, error) {
		return b(NewQueryInfo(q))
	}))
}
func (g *SessionImpl) Close() {
	g.session.Close()
}
func (g *SessionImpl) Closed() bool {
	return g.session.Closed()
}
func (g *SessionImpl) KeyspaceMetadata(keyspace string) (KeyspaceMetadata, error) {
	result, err := g.session.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, err
	}
	return NewKeyspaceMetadata(result), nil
}
func (g *SessionImpl) ExecuteBatch(batch Batch) error {
	return g.session.ExecuteBatch(batch.(*BatchImpl).batch)
}
func (g *SessionImpl) ExecuteBatchCAS(batch Batch, dest ...interface{}) (bool, Iter, error) {
	applied, iter, err := g.session.ExecuteBatchCAS(batch.(*BatchImpl).batch, dest)
	if err != nil {
		return applied, nil, err
	}
	return applied, NewIter(iter), nil
}
func (g *SessionImpl) MapExecuteBatchCAS(batch Batch, dest map[string]interface{}) (bool, Iter, error) {
	applied, iter, err := g.session.MapExecuteBatchCAS(batch.(*BatchImpl).batch, dest)
	if err != nil {
		return applied, nil, err
	}
	return applied, NewIter(iter), nil
}
func (g *SessionImpl) NewBatch(typ BatchType) Batch {
	return NewBatch(g.session.NewBatch(gocql.BatchType(typ)))
}

func NewQuery(query *gocql.Query) *QueryImpl {
	return &QueryImpl{}
}
func (g *QueryImpl) String() string {
	return g.query.String()
}
func (g *QueryImpl) Attempts() int {
	return g.query.Attempts()
}
func (g *QueryImpl) Latency() int64 {
	return g.query.Latency()
}
func (g *QueryImpl) Consistency(c Consistency) Query {
	g.query = g.query.Consistency(gocql.Consistency(c))
	return g
}
func (g *QueryImpl) GetConsistency() Consistency {
	return Consistency(g.query.GetConsistency())
}
func (g *QueryImpl) Trace(trace Tracer) Query {
	g.query = g.query.Trace(trace)
	return g
}
func (g *QueryImpl) PageSize(n int) Query {
	g.query = g.query.PageSize(n)
	return g
}
func (g *QueryImpl) DefaultTimestamp(enable bool) Query {
	g.query = g.query.DefaultTimestamp(enable)
	return g
}
func (g *QueryImpl) WithTimestamp(timestamp int64) Query {
	g.query = g.query.WithTimestamp(timestamp)
	return g
}
func (g *QueryImpl) RoutingKey(routingKey []byte) Query {
	g.query = g.query.RoutingKey(routingKey)
	return g
}
func (g *QueryImpl) WithContext(ctx context.Context) Query {
	g.query = g.query.WithContext(ctx)
	return g
}
func (g *QueryImpl) GetRoutingKey() ([]byte, error) {
	return g.query.GetRoutingKey()
}
func (g *QueryImpl) Prefetch(p float64) Query {
	g.query = g.query.Prefetch(p)
	return g
}
func (g *QueryImpl) RetryPolicy(r RetryPolicy) Query {
	g.query = g.query.RetryPolicy(r.(*RetryPolicyImpl).policy)
	return g
}
func (g *QueryImpl) Bind(v ...interface{}) Query {
	g.query = g.query.Bind(v)
	return g
}
func (g *QueryImpl) SerialConsistency(cons SerialConsistency) Query {
	g.query = g.query.SerialConsistency(gocql.SerialConsistency(cons))
	return g
}
func (g *QueryImpl) PageState(state []byte) Query {
	g.query = g.query.PageState(state)
	return g
}
func (g *QueryImpl) NoSkipMetadata() Query {
	g.query = g.query.NoSkipMetadata()
	return g
}
func (g *QueryImpl) Exec() error {
	return g.query.Exec()
}
func (g *QueryImpl) MapScan(m map[string]interface{}) error {
	return g.query.MapScan(m)
}
func (g *QueryImpl) Scan(dest ...interface{}) error {
	return g.query.Scan(dest)
}
func (g *QueryImpl) ScanCAS(dest ...interface{}) (applied bool, err error) {
	return g.query.ScanCAS(dest)
}
func (g *QueryImpl) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	return g.query.MapScanCAS(dest)
}
func (g *QueryImpl) Release() {
	g.query.Release()
}

func NewBatch(batch *gocql.Batch) *BatchImpl {
	return &BatchImpl{batch: batch}
}
func (g *BatchImpl) Attempts() int {
	return g.batch.Attempts()
}
func (g *BatchImpl) Latency() int64 {
	return g.batch.Latency()
}
func (g *BatchImpl) GetConsistency() Consistency {
	return Consistency(g.batch.GetConsistency())
}
func (g *BatchImpl) Query(stmt string, args ...interface{}) {
	g.batch.Query(stmt, args)
}
func (g *BatchImpl) Bind(stmt string, bind func(q QueryInfo) ([]interface{}, error)) {
	g.batch.Bind(stmt, func(q *gocql.QueryInfo) ([]interface{}, error) {
		return bind(NewQueryInfo(q))
	})
}
func (g *BatchImpl) RetryPolicy(r RetryPolicy) Batch {
	g.batch = g.batch.RetryPolicy(r.(*RetryPolicyImpl).policy)
	return g
}
func (g *BatchImpl) WithContext(ctx context.Context) Batch {
	g.batch = g.batch.WithContext(ctx)
	return g
}
func (g *BatchImpl) Size() int {
	return g.batch.Size()
}
func (g *BatchImpl) SerialConsistency(cons SerialConsistency) Batch {
	g.batch = g.batch.SerialConsistency(gocql.SerialConsistency(cons))
	return g
}
func (g *BatchImpl) DefaultTimestamp(enable bool) Batch {
	g.batch = g.batch.DefaultTimestamp(enable)
	return g
}
func (g *BatchImpl) WithTimestamp(timestamp int64) Batch {
	g.batch = g.batch.WithTimestamp(timestamp)
	return g
}
func (g *BatchImpl) GetRoutingKey() ([]byte, error) {
	return g.batch.GetRoutingKey()
}

func NewKeyspaceMetadata(metadata *gocql.KeyspaceMetadata) *KeyspaceMetadataImpl {
	return &KeyspaceMetadataImpl{metadata: metadata}
}

func (g *KeyspaceMetadataImpl) GetName() string {
	return g.metadata.Name
}
func (g *KeyspaceMetadataImpl) IsDurableWrites() bool {
	return g.metadata.DurableWrites
}
func (g *KeyspaceMetadataImpl) GetStrategyClass() string {
	return g.metadata.StrategyClass
}
func (g *KeyspaceMetadataImpl) GetStrategyOptions() map[string]interface{} {
	return g.metadata.StrategyOptions
}
func (g *KeyspaceMetadataImpl) GetTables() map[string]TableMetadata {
	result := map[string]TableMetadata{}
	for k, v := range g.metadata.Tables {
		result[k] = NewTableMetadata(v)
	}
	return result
}

func NewTableMetadata(metadata *gocql.TableMetadata) *TableMetadataImpl {
	return &TableMetadataImpl{metadata: metadata}
}

func (g *TableMetadataImpl) GetKeyspace() string {
	return g.metadata.Keyspace
}
func (g *TableMetadataImpl) GetName() string {
	return g.metadata.Name
}
func (g *TableMetadataImpl) GetKeyValidator() string {
	return g.metadata.KeyValidator
}
func (g *TableMetadataImpl) GetComparator() string {
	return g.metadata.Comparator
}
func (g *TableMetadataImpl) GetDefaultValidator() string {
	return g.metadata.DefaultValidator
}
func (g *TableMetadataImpl) GetKeyAliases() []string {
	return g.metadata.KeyAliases
}
func (g *TableMetadataImpl) GetColumnAliases() []string {
	return g.metadata.ColumnAliases
}
func (g *TableMetadataImpl) GetValueAlias() string {
	return g.metadata.ValueAlias
}
func (g *TableMetadataImpl) GetPartitionKey() []ColumnMetadata {
	result := []ColumnMetadata{}
	for _, item := range g.metadata.PartitionKey {
		result = append(result, NewColumnMetadata(item))
	}
	return result
}
func (g *TableMetadataImpl) GetClusteringColumns() []ColumnMetadata {
	result := []ColumnMetadata{}
	for _, item := range g.metadata.ClusteringColumns {
		result = append(result, NewColumnMetadata(item))
	}
	return result
}
func (g *TableMetadataImpl) GetColumns() map[string]ColumnMetadata {
	result := map[string]ColumnMetadata{}
	for k, v := range g.metadata.Columns {
		result[k] = NewColumnMetadata(v)
	}
	return result
}
func (g *TableMetadataImpl) GetOrderedColumns() []string {
	return g.metadata.OrderedColumns
}

func NewColumnMetadata(metadata *gocql.ColumnMetadata) *ColumnMetadataImpl {
	return &ColumnMetadataImpl{metadata: metadata}
}

func (g *ColumnMetadataImpl) GetKeyspace() string {
	return g.metadata.Keyspace
}
func (g *ColumnMetadataImpl) GetTable() string {
	return g.metadata.Table
}
func (g *ColumnMetadataImpl) GetName() string {
	return g.metadata.Name
}
func (g *ColumnMetadataImpl) GetComponentIndex() int {
	return g.metadata.ComponentIndex
}
func (g *ColumnMetadataImpl) GetKind() ColumnKind {
	return ColumnKind(g.metadata.Kind)
}
func (g *ColumnMetadataImpl) GetValidator() string {
	return g.metadata.Validator
}
func (g *ColumnMetadataImpl) GetType() TypeInfo {
	return NewTypeInfo(g.metadata.Type)
}
func (g *ColumnMetadataImpl) GetClusteringOrder() string {
	return g.metadata.ClusteringOrder
}
func (g *ColumnMetadataImpl) GetOrder() ColumnOrder {
	return ColumnOrder(g.metadata.Order)
}
func (g *ColumnMetadataImpl) GetIndex() ColumnIndexMetadata {
	return NewColumnIndexMetadata(&g.metadata.Index)
}

func NewTypeInfo(info gocql.TypeInfo) *TypeInfoImpl {
	return &TypeInfoImpl{info: info}
}
func (g *TypeInfoImpl) Type() Type {
	return Type(g.info.Type())
}
func (g *TypeInfoImpl) Version() byte {
	return g.info.Version()
}
func (g *TypeInfoImpl) Custom() string {
	return g.info.Custom()
}
func (g *TypeInfoImpl) New() interface{} {
	return g.info.New()
}

func NewColumnIndexMetadata(metadata *gocql.ColumnIndexMetadata) *ColumnIndexMetadataImpl {
	return &ColumnIndexMetadataImpl{metadata: metadata}
}
func (g *ColumnIndexMetadataImpl) GetName() string {
	return g.metadata.Name
}
func (g *ColumnIndexMetadataImpl) GetType() string {
	return g.metadata.Type
}
func (g *ColumnIndexMetadataImpl) GetOptions() map[string]interface{} {
	return g.metadata.Options
}

func NewQueryInfo(info *gocql.QueryInfo) *QueryInfoImpl {
	return &QueryInfoImpl{info: info}
}
func (g *QueryInfoImpl) GetID() []byte {
	return g.info.Id
}
func (g *QueryInfoImpl) GetArgs() []ColumnInfo {
	result := []ColumnInfo{}
	for _, item := range g.info.Args {
		result = append(result, NewColumnInfo(&item))
	}
	return result
}
func (g *QueryInfoImpl) GetRval() []ColumnInfo {
	result := []ColumnInfo{}
	for _, item := range g.info.Rval {
		result = append(result, NewColumnInfo(&item))
	}
	return result
}
func (g *QueryInfoImpl) GetPKeyColumns() []int {
	return g.info.PKeyColumns
}

func NewRetryPolicy(policy gocql.RetryPolicy) *RetryPolicyImpl {
	return &RetryPolicyImpl{policy: policy}
}
func (g *RetryPolicyImpl) Attempt(query RetryableQuery) bool {
	return g.policy.Attempt(query.(*RetryableQueryImpl).query)
}

func NewRetryableQuery(query gocql.RetryableQuery) *RetryableQueryImpl {
	return &RetryableQueryImpl{query: query}
}
func (g *RetryableQueryImpl) Attempts() int {
	return g.query.Attempts()
}
func (g *RetryableQueryImpl) GetConsistency() Consistency {
	return Consistency(g.query.GetConsistency())
}

func NewColumnInfo(info *gocql.ColumnInfo) *ColumnInfoImpl {
	return &ColumnInfoImpl{info: info}
}
func (g *ColumnInfoImpl) GetKeyspace() string {
	return g.info.Keyspace
}
func (g *ColumnInfoImpl) GetTable() string {
	return g.info.Table
}
func (g *ColumnInfoImpl) GetName() string {
	return g.info.Name
}
func (g *ColumnInfoImpl) GetTypeInfo() TypeInfo {
	return NewTypeInfo(g.info.TypeInfo)
}

func NewTracer(tracer gocql.Tracer) *TracerImpl {
	return &TracerImpl{tracer: tracer}
}

func (g *TracerImpl) Trace(traceId []byte) {
	g.tracer.Trace(traceId)
}

func NewIter(iter *gocql.Iter) *IterImpl {
	return &IterImpl{iter: iter}
}
func (g *IterImpl) Host() HostInfo {
	return NewHostInfo(g.iter.Host())
}
func (g *IterImpl) Columns() []ColumnInfo {
	result := []ColumnInfo{}
	for _, item := range g.iter.Columns() {
		result = append(result, NewColumnInfo(&item))
	}
	return result
}

func NewHostInfo(info *gocql.HostInfo) *HostInfoImpl {
	return &HostInfoImpl{info: info}
}
