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

package clock

import (
	"fmt"
	"time"
)

type (
	// Resolution is the clock resolution
	Resolution int

	ResolutionPolicy interface {
		apply(timestamp time.Time) time.Time
		resolution() time.Duration
	}

	ResolutionPolicyImpl struct {
		res Resolution
	}
)

const (
	// time source resolution

	// ResolutionMilliseconds is millisecond resolution
	ResolutionMilliseconds Resolution = iota
	// ResolutionMicroseconds is microsecond resolution
	ResolutionMicroseconds
	// ResolutionNanoseconds is nanosecond resolution
	ResolutionNanoseconds
)

func NewResolutionPolicy(
	resolution Resolution,
) *ResolutionPolicyImpl {

	return &ResolutionPolicyImpl{res: resolution}
}

func (r *ResolutionPolicyImpl) apply(timestamp time.Time) time.Time {
	switch r.res {
	case ResolutionMilliseconds:
		return time.Unix(0, timestamp.UnixNano()/1e6*1e6)
	case ResolutionMicroseconds:
		return time.Unix(0, timestamp.UnixNano()/1e3*1e3)
	case ResolutionNanoseconds:
		return timestamp
	default:
		panic(fmt.Sprintf("unknown clock resolution: %v", r.resolution))
	}
}

func (r *ResolutionPolicyImpl) resolution() time.Duration {
	switch r.res {
	case ResolutionMilliseconds:
		return time.Millisecond
	case ResolutionMicroseconds:
		return time.Microsecond
	case ResolutionNanoseconds:
		return time.Nanosecond
	default:
		panic(fmt.Sprintf("unknown clock resolution: %v", r.resolution))
	}
}
