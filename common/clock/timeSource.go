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

import "time"

type (
	timeSource struct {
		resolution *ResolutionPolicyImpl
	}

	// TimeSource is an interface for any
	// entity that provides the current
	// time. Its primarily used to mock
	// out time sources in unit test
	TimeSource interface {
		Now() time.Time
		Resolution() time.Duration

		IsBefore(time1 time.Time, time2 time.Time) bool
		MaybeBefore(time1 time.Time, time2 time.Time) bool
		IsAfter(time1 time.Time, time2 time.Time) bool
		MaybeAfter(time1 time.Time, time2 time.Time) bool
		MaybeEqual(time1 time.Time, time2 time.Time) bool
	}
	// RealTimeSource serves real wall-clock time
	RealTimeSource struct {
		*timeSource
	}

	// EventTimeSource serves fake controlled time
	EventTimeSource struct {
		*timeSource
		now time.Time
	}
)

// NewRealTimeSource returns a time source that servers
// real wall clock time
func NewRealTimeSource() *RealTimeSource {
	return &RealTimeSource{
		timeSource: &timeSource{resolution: NewResolutionPolicy(ResolutionMilliseconds)},
	}
}

// Now return the real current time
func (ts *RealTimeSource) Now() time.Time {
	return ts.resolution.apply(time.Now())
}

// NewEventTimeSource returns a time source that servers
// fake controlled time
func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{
		timeSource: &timeSource{resolution: NewResolutionPolicy(ResolutionMilliseconds)},
	}
}

// Now return the fake current time
func (ts *EventTimeSource) Now() time.Time {
	return ts.now
}

// Update update the fake current time
func (ts *EventTimeSource) Update(now time.Time) *EventTimeSource {
	ts.now = ts.resolution.apply(now)
	return ts
}

// Resolution return the time resolution
func (ts *timeSource) Resolution() time.Duration {
	return ts.resolution.resolution()
}

// IsBefore return is time1 happens before time2
func (ts *timeSource) IsBefore(time1 time.Time, time2 time.Time) bool {
	res := int64(ts.Resolution())
	return time1.UnixNano()+res <= time2.UnixNano()
}

// MaybeBefore return maybe time1 happens before time2
func (ts *timeSource) MaybeBefore(time1 time.Time, time2 time.Time) bool {
	res := int64(ts.Resolution())
	return time1.UnixNano() < time2.UnixNano()+res
}

// IsAfter return is time1 happens after time2
func (ts *timeSource) IsAfter(time1 time.Time, time2 time.Time) bool {
	return ts.IsBefore(time2, time1)
}

// MaybeAfter return maybe time1 happens after time2
func (ts *timeSource) MaybeAfter(time1 time.Time, time2 time.Time) bool {
	return ts.MaybeBefore(time2, time1)
}

// MaybeAfter return maybe time1 happens after time2
func (ts *timeSource) MaybeEqual(time1 time.Time, time2 time.Time) bool {
	res := int64(ts.Resolution())
	t1 := time1.UnixNano()
	t2 := time2.UnixNano()
	return t2-res < t1 && t1 < t2+res
}
