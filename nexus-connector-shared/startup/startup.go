// Package startup is how every connector program starts, so each program's main reads as its steps:
//
//	s := startup.New("Outbound Connector", env)
//	cfg, err := configFrom(env)
//	s.Config(err)                     // every problem is collected, not just the first
//	opts, c := s.Temporal()           // client options (with the payload codec) and the connection
//	if err := s.Problems(); err != nil {
//		log.Fatal(err)                // "The Outbound Connector can't start:" and every problem
//	}
//	defer c.Close()
//	log.Print(cfg.report(env, opts))  // the startup report
//	err = s.Ready(ctx, connectToSystem) // waits for the payload codec, then the program's own system
//
// The payload codec (encryption) is handled here and in temporalclient: a program's own code never
// mentions it.
package startup

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/codec"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/temporalclient"
	"go.temporal.io/sdk/client"
)

// ErrStopped is what Ready returns when its context ends, as when the program is interrupted, before
// everything is ready. It is not a failure: the program should stop quietly.
var ErrStopped = errors.New("stopped while starting")

// Startup collects what a program finds wrong as it starts, so one run reports every problem
// rather than only the first.
type Startup struct {
	program  string // such as "Outbound Connector"
	env      settings.Env
	problems []string
	opts     client.Options
	client   client.Client
	// Dial connects to Temporal: client.Dial, unless a test replaces it.
	Dial func(client.Options) (client.Client, error)
	// Logger logs Ready's waits: slog.Default(), unless a test replaces it.
	Logger *slog.Logger
}

// New starts checking program (such as "Outbound Connector"), configured by env. An empty program
// is called "program".
func New(program string, env settings.Env) *Startup {
	if program == "" {
		program = "program"
	}
	return &Startup{program: program, env: env, Dial: client.Dial, Logger: slog.Default()}
}

// Add records err, if it isn't nil, as a problem under heading, such as "Failed to connect to
// Stripe". Each line of err (errors.Join puts one problem on each) is listed under it.
func (s *Startup) Add(heading string, err error) {
	if err != nil {
		s.problems = append(s.problems, heading+":\n  - "+strings.ReplaceAll(err.Error(), "\n", "\n  - "))
	}
}

// Config records err, if it isn't nil, as a problem with the program's own settings.
func (s *Startup) Config(err error) {
	s.Add("Invalid "+s.program+" configuration", err)
}

// Temporal loads the Temporal client options from the environment (temporalclient.Options: the
// TEMPORAL_* variables, the profile file, the namespace default and the payload codec) and
// connects, recording any problem. The client is nil if either failed.
func (s *Startup) Temporal() (client.Options, client.Client) {
	opts, err := temporalclient.Options(s.env)
	if err != nil {
		s.Add("Failed to load Temporal client configuration", err)
		return client.Options{}, nil
	}
	s.opts = opts
	c, err := s.Dial(opts)
	if err != nil {
		s.Add("Failed to connect to Temporal", err)
		return opts, nil
	}
	s.client = c
	return opts, c
}

// Problems is every problem recorded, as one error, or nil if there are none. If there are any, it
// closes the Temporal client Temporal opened: the program can't start.
func (s *Startup) Problems() error {
	if len(s.problems) == 0 {
		return nil
	}
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
	return problems{program: s.program, problems: s.problems}
}

// problems is Problems' error: "The Outbound Connector can't start:", then each problem.
type problems struct {
	program  string
	problems []string
}

func (p problems) Error() string {
	return "The " + p.program + " can't start:\n" + strings.Join(p.problems, "\n")
}

// Ready waits until the program can serve: first for the payload codec, if one is set, then for
// each of waits in turn, such as a connection to the program's own system. Each wait retries and
// logs until it works, or returns an error that stops the program. Call it after logging the
// startup report. It returns ErrStopped if ctx ends first.
func (s *Startup) Ready(ctx context.Context, waits ...func(context.Context) error) error {
	codecWait := func(ctx context.Context) error {
		return codec.WaitAtStartup(ctx, s.env, s.opts.DataConverter, s.Logger)
	}
	for _, wait := range append([]func(context.Context) error{codecWait}, waits...) {
		if err := wait(ctx); err != nil {
			if ctx.Err() != nil {
				return ErrStopped
			}
			return err
		}
	}
	if ctx.Err() != nil {
		return ErrStopped
	}
	return nil
}
