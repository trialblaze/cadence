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

package archiver

import (
	"fmt"
	"strings"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/service/config"
)

type (
	// ArchivalMetadata provides cluster level archival information
	ArchivalMetadata interface {
		GetHistoryConfig() ArchivalConfig
		GetVisibilityConfig() ArchivalConfig
	}

	// ArchivalConfig is an immutable representation of the archival configuration of the cluster
	// This config is determined at cluster startup time
	ArchivalConfig interface {
		ClusterConfiguredForArchival() bool
		GetClusterStatus() ArchivalStatus
		ReadEnabled() bool
		GetDomainDefaultStatus() shared.ArchivalStatus
		GetDomainDefaultURI() string
	}

	archivalMetadata struct {
		historyConfig    ArchivalConfig
		visibilityConfig ArchivalConfig
	}

	archivalConfig struct {
		clusterStatus       ArchivalStatus
		enableRead          bool
		domainDefaultStatus shared.ArchivalStatus
		domainDefaultURI    string
	}

	// ArchivalStatus represents the archival status of the cluster
	ArchivalStatus int
)

const (
	// ArchivalDisabled means this cluster is not configured to handle archival
	ArchivalDisabled ArchivalStatus = iota
	// ArchivalPaused means this cluster is configured to handle archival but is currently not archiving
	// This state is not yet implemented, as of now ArchivalPaused is treated the same way as ArchivalDisabled
	ArchivalPaused
	// ArchivalEnabled means this cluster is currently archiving
	ArchivalEnabled
)

// NewArchivalMetadata constructs a new ArchivalMetadata
func NewArchivalMetadata(
	historyStatus string,
	historyReadEnabled bool,
	visibilityStatus string,
	visibilityReadEnabled bool,
	domainDefaults *config.ArchivalDomainDefaults,
) ArchivalMetadata {
	return &archivalMetadata{
		historyConfig: NewArchivalConfig(
			historyStatus,
			historyReadEnabled,
			domainDefaults.History.Status,
			domainDefaults.History.URI,
		),
		visibilityConfig: NewArchivalConfig(
			visibilityStatus,
			visibilityReadEnabled,
			domainDefaults.Visibility.Status,
			domainDefaults.Visibility.URI,
		),
	}
}

func (metadata *archivalMetadata) GetHistoryConfig() ArchivalConfig {
	return metadata.historyConfig
}

func (metadata *archivalMetadata) GetVisibilityConfig() ArchivalConfig {
	return metadata.visibilityConfig
}

// NewArchivalConfig constructs a new valid ArchivalConfig
func NewArchivalConfig(
	clusterStatusStr string,
	enableRead bool,
	domainDefaultStatusStr string,
	domainDefaultURI string,
) ArchivalConfig {
	clusterStatus, err := getClusterArchivalStatus(clusterStatusStr)
	if err != nil {
		panic(err)
	}
	domainDefaultStatus, err := getDomainArchivalStatus(domainDefaultStatusStr)
	if err != nil {
		panic(err)
	}

	ac := &archivalConfig{
		clusterStatus:       clusterStatus,
		enableRead:          enableRead,
		domainDefaultStatus: domainDefaultStatus,
		domainDefaultURI:    domainDefaultURI,
	}
	if !ac.isValid() {
		panic("invalid cluster level archival configuration")
	}
	return ac
}

// ClusterConfiguredForArchival returns true if cluster is configured to handle archival, false otherwise
func (a *archivalConfig) ClusterConfiguredForArchival() bool {
	return a.clusterStatus == ArchivalEnabled
}

func (a *archivalConfig) GetClusterStatus() ArchivalStatus {
	return a.clusterStatus
}

func (a *archivalConfig) ReadEnabled() bool {
	return a.enableRead
}

func (a *archivalConfig) GetDomainDefaultStatus() shared.ArchivalStatus {
	return a.domainDefaultStatus
}

func (a *archivalConfig) GetDomainDefaultURI() string {
	return a.domainDefaultURI
}

func (a *archivalConfig) isValid() bool {
	URISet := len(a.domainDefaultURI) != 0
	return (URISet && a.ClusterConfiguredForArchival()) || (!URISet && !a.ClusterConfiguredForArchival())
}

func getClusterArchivalStatus(str string) (ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", "disabled":
		return ArchivalDisabled, nil
	case "paused":
		return ArchivalPaused, nil
	case "enabled":
		return ArchivalEnabled, nil
	}
	return ArchivalDisabled, fmt.Errorf("invalid archival status of %v for cluster, valid status are: {\"\", \"disabled\", \"paused\", \"enabled\"}", str)
}

func getDomainArchivalStatus(str string) (shared.ArchivalStatus, error) {
	str = strings.TrimSpace(strings.ToLower(str))
	switch str {
	case "", "disabled":
		return shared.ArchivalStatusDisabled, nil
	case "enabled":
		return shared.ArchivalStatusEnabled, nil
	}
	return shared.ArchivalStatusDisabled, fmt.Errorf("invalid archival status of %v for domain, valid status are: {\"\", \"disabled\", \"enabled\"}", str)
}
