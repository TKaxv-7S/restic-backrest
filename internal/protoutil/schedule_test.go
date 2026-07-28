package protoutil_test

import (
	"testing"
	"time"

	v1 "github.com/garethgeorge/backrest/gen/go/v1"
	"github.com/garethgeorge/backrest/internal/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSchedule(t *testing.T) {
	// Cron expressions are evaluated in the reference time's own zone, so use
	// Local here to match the CLOCK_LOCAL cases below.
	now := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local) // Sunday, Oct 1st 2023 10:00 Local

	tests := []struct {
		name        string
		schedule    *v1.Schedule
		lastRan     time.Time
		curTime     time.Time
		expected    time.Time
		expectError bool
	}{
		{
			name: "MaxFrequencyDays - 1 day",
			schedule: &v1.Schedule{
				Clock: v1.Schedule_CLOCK_LOCAL,
				Schedule: &v1.Schedule_MaxFrequencyDays{
					MaxFrequencyDays: 1,
				},
			},
			lastRan:  now.Add(-24 * time.Hour),
			curTime:  now,
			expected: now.Add(24 * time.Hour),
		},
		{
			name: "Cron - Every minute",
			schedule: &v1.Schedule{
				Clock: v1.Schedule_CLOCK_LOCAL,
				Schedule: &v1.Schedule_Cron{
					Cron: "* * * * *",
				},
			},
			lastRan:  now,
			curTime:  now,
			expected: now.Add(1 * time.Minute),
		},
		{
			name: "Cron - Sunday (0) - Should work",
			schedule: &v1.Schedule{
				Clock: v1.Schedule_CLOCK_LOCAL,
				Schedule: &v1.Schedule_Cron{
					Cron: "0 10 * * 0", // 10:00 AM on Sunday
				},
			},
			lastRan: now.Add(-1 * time.Hour),
			curTime: now,
			// now is Sunday 10:00:00.
			// If we are exactly at scheduled time, Next() usually returns next slot?
			// Let's assume next slot is next week.
			expected: now.Add(7 * 24 * time.Hour),
		},
		{
			name: "Cron - First Monday of the month (#)",
			schedule: &v1.Schedule{
				Clock: v1.Schedule_CLOCK_LOCAL,
				Schedule: &v1.Schedule_Cron{
					Cron: "0 0 * * 1#1",
				},
			},
			lastRan: now,
			curTime: now,
			// Oct 1st 2023 is a Sunday, so the first Monday is Oct 2nd.
			expected: time.Date(2023, 10, 2, 0, 0, 0, 0, time.Local),
		},
		{
			name: "Cron - Last day of the month (L)",
			schedule: &v1.Schedule{
				Clock: v1.Schedule_CLOCK_LOCAL,
				Schedule: &v1.Schedule_Cron{
					Cron: "0 0 L * *",
				},
			},
			lastRan:  now,
			curTime:  now,
			expected: time.Date(2023, 10, 31, 0, 0, 0, 0, time.Local),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protoutil.ResolveSchedule(tt.schedule, tt.lastRan, tt.curTime)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.WithinDuration(t, tt.expected, got, 1*time.Second)
			}
		})
	}
}

func TestValidateSchedule(t *testing.T) {
	tests := []struct {
		name          string
		schedule      *v1.Schedule
		expectError   bool
		errorContains string
	}{
		{
			name: "Valid Cron (0)",
			schedule: &v1.Schedule{
				Schedule: &v1.Schedule_Cron{Cron: "0 10 * * 0"},
			},
			expectError: false,
		},
		{
			name: "Valid Cron (7) - Sunday, same as 0",
			schedule: &v1.Schedule{
				Schedule: &v1.Schedule_Cron{Cron: "0 10 * * 7"},
			},
			expectError: false,
		},
		{
			name: "Valid Frequency",
			schedule: &v1.Schedule{
				Schedule: &v1.Schedule_MaxFrequencyDays{MaxFrequencyDays: 1},
			},
			expectError: false,
		},
		// Extended (Quartz-style) syntax. cronstrue renders these in the WebUI, so
		// the backend must accept everything the schedule form is willing to show.
		{
			name:        "Nth weekday of month (#)",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 1#1"}},
			expectError: false,
		},
		{
			name:        "Nth weekday of month by name",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * MON#1"}},
			expectError: false,
		},
		{
			name:        "Last weekday of month (L)",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 5L"}},
			expectError: false,
		},
		{
			name:        "Last day of month (L)",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 L * *"}},
			expectError: false,
		},
		{
			name:        "Nearest weekday (W)",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 15W * *"}},
			expectError: false,
		},
		{
			name:        "Descriptor",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "@daily"}},
			expectError: false,
		},
		// Day-of-week ranges ending in 7 parse but never fire under the upstream
		// (archived) gorhill parser; the hashicorp fork resolves 7 to Sunday.
		{
			name:        "Day of week range ending in 7",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 1-7"}},
			expectError: false,
		},
		{
			name:        "Day of week range MON-SUN",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * MON-SUN"}},
			expectError: false,
		},
		{
			name:          "Nth weekday out of range",
			schedule:      &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 1#6"}},
			expectError:   true,
			errorContains: "invalid cron",
		},
		{
			name:          "Expression that never matches a real date",
			schedule:      &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 30 2 *"}},
			expectError:   true,
			errorContains: "never matches a real date",
		},
		{
			name:          "Empty cron",
			schedule:      &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: ""}},
			expectError:   true,
			errorContains: "empty cron expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := protoutil.ValidateSchedule(tt.schedule)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestResolveScheduleClockZone pins down which zone a cron expression's
// wall-clock fields are interpreted in. The parser evaluates against the
// reference time's own location, and ResolveSchedule picks that location from
// the schedule's clock, so CLOCK_UTC fires on UTC wall-clock rather than the
// host's local wall-clock.
func TestResolveScheduleClockZone(t *testing.T) {
	curTime := time.Date(2023, 10, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		clock v1.Schedule_Clock
		zone  *time.Location
	}{
		{name: "UTC clock fires on UTC wall-clock", clock: v1.Schedule_CLOCK_UTC, zone: time.UTC},
		{name: "Local clock fires on local wall-clock", clock: v1.Schedule_CLOCK_LOCAL, zone: time.Local},
		{name: "Default clock fires on local wall-clock", clock: v1.Schedule_CLOCK_DEFAULT, zone: time.Local},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protoutil.ResolveSchedule(&v1.Schedule{
				Clock:    tt.clock,
				Schedule: &v1.Schedule_Cron{Cron: "0 3 * * *"}, // 03:00 daily
			}, curTime, curTime)
			require.NoError(t, err)
			assert.Equal(t, 3, got.In(tt.zone).Hour(), "expected 03:00 in %v, got %v", tt.zone, got)
			assert.Equal(t, 0, got.In(tt.zone).Minute())
		})
	}
}

func transactionalTimeEqual(t1, t2 time.Time) bool {
	return t1.Equal(t2)
}

func TestNominalPeriod(t *testing.T) {
	// Fixed reference time in UTC so DST transitions can't widen sampled cron gaps.
	from := time.Date(2023, 10, 1, 10, 0, 0, 0, time.UTC) // Sunday, Oct 1st 2023 10:00

	tests := []struct {
		name        string
		schedule    *v1.Schedule
		expected    time.Duration
		expectError bool
	}{
		{
			name:     "MaxFrequencyDays",
			schedule: &v1.Schedule{Schedule: &v1.Schedule_MaxFrequencyDays{MaxFrequencyDays: 7}},
			expected: 7 * 24 * time.Hour,
		},
		{
			name:     "MaxFrequencyHours",
			schedule: &v1.Schedule{Schedule: &v1.Schedule_MaxFrequencyHours{MaxFrequencyHours: 6}},
			expected: 6 * time.Hour,
		},
		{
			name:     "Cron daily",
			schedule: &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * *"}},
			expected: 24 * time.Hour,
		},
		{
			name:     "Cron weekly",
			schedule: &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 0"}},
			expected: 7 * 24 * time.Hour,
		},
		{
			name: "Cron weekdays only uses widest gap",
			// 9am Mon-Fri: the widest quiet stretch is Friday 9am -> Monday 9am.
			schedule: &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 9 * * 1-5"}},
			expected: 72 * time.Hour,
		},
		{
			name: "Cron first Monday of month uses widest gap",
			// First Mondays sit 28 or 35 days apart depending on the month.
			schedule: &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "0 0 * * 1#1"}},
			expected: 35 * 24 * time.Hour,
		},
		{
			name:        "Disabled",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Disabled{Disabled: true}},
			expectError: true,
		},
		{
			name:        "Nil schedule",
			schedule:    &v1.Schedule{},
			expectError: true,
		},
		{
			name:        "Invalid cron",
			schedule:    &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: "not a cron"}},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protoutil.NominalPeriod(tt.schedule, from)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// FuzzValidateSchedule guards the cron parser against panics on arbitrary
// expressions. Schedules come from the config file, so a panic here would take
// down the server rather than surface as a validation error.
func FuzzValidateSchedule(f *testing.F) {
	for _, seed := range []string{
		"* * * * *", "0 0 * * 1#1", "0 0 L * *", "0 0 15W * *", "@daily",
		"0 0 * * 1-7", "not a cron", "", "* * * * * * *", "0 0 30 2 *",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cron string) {
		sched := &v1.Schedule{Schedule: &v1.Schedule_Cron{Cron: cron}}
		if err := protoutil.ValidateSchedule(sched); err != nil {
			return
		}
		// Anything that validates must also resolve and report a period without panicking.
		_, _ = protoutil.ResolveSchedule(sched, time.Now(), time.Now())
		_, _ = protoutil.NominalPeriod(sched, time.Now())
	})
}
