package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-kafka/settings"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/log"
)

// temporalTimeout bounds the Temporal check: connecting, finding the namespace and the endpoints.
var temporalTimeout = 10 * time.Second

// checkTemporal connects to Temporal with the client options p loads from env, as the program
// would, checks that the namespace exists, that the endpoint p calls exists if it calls one, and,
// if p has a task queue, that a Nexus endpoint targets the namespace and task queue.
func checkTemporal(ctx context.Context, p program, env settings.Env) check {
	c := check{Title: "Temporal"}
	opts, err := p.ClientOptions(env)
	if err != nil {
		c.Problems = append(c.Problems, "Failed to load Temporal client configuration: "+err.Error())
		return c
	}
	c.Passed = append(c.Passed, describeProfile(env, []program{p}).summary())
	opts.Logger = log.NewStructuredLogger(slog.New(slog.DiscardHandler))
	address := opts.HostPort
	if address == "" {
		address = client.DefaultHostPort
	}
	ctx, cancel := context.WithTimeout(ctx, temporalTimeout)
	defer cancel()
	cl, err := client.DialContext(ctx, opts)
	if err != nil {
		c.Problems = append(c.Problems, fmt.Sprintf("Failed to connect to Temporal at %s: %v", address, err))
		return c
	}
	defer cl.Close()
	c.Passed = append(c.Passed, "Connected to Temporal at "+address+".")

	ns := opts.Namespace
	_, err = cl.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	var nsNotFound *serviceerror.NamespaceNotFound
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &nsNotFound) || errors.As(err, &notFound):
		c.Problems = append(c.Problems, fmt.Sprintf("Namespace %q doesn't exist. Create it, or set TEMPORAL_NAMESPACE to one that does. "+
			"On a dev server: temporal operator namespace create --namespace %s", ns, ns))
		return c
	case err != nil:
		c.Problems = append(c.Problems, fmt.Sprintf("Couldn't find namespace %q: %v", ns, err))
		return c
	}
	c.Passed = append(c.Passed, fmt.Sprintf("Namespace %q exists.", ns))

	if p.CallsEndpoint != nil {
		if name := p.CallsEndpoint(env); name != "" {
			checkCalledEndpoint(ctx, cl, name, &c)
		}
	}
	if p.TaskQueue == nil {
		return c
	}
	taskQueue := p.TaskQueue(env)
	names, err := endpointsTargeting(ctx, cl, ns, taskQueue)
	switch {
	case err != nil:
		c.Warnings = append(c.Warnings, fmt.Sprintf("Couldn't list Nexus endpoints to check that one targets namespace %q and task queue %q: %v. "+
			"On Temporal Cloud, check in the Cloud UI or with tcld.", ns, taskQueue, err))
	case len(names) == 0:
		create := fmt.Sprintf("temporal operator nexus endpoint create --name %s --target-namespace %s --target-task-queue %s",
			p.EndpointName, ns, taskQueue)
		if p.EndpointDescriptionFile != "" {
			create += " --description-file " + p.EndpointDescriptionFile
		}
		c.Problems = append(c.Problems, fmt.Sprintf("No Nexus endpoint targets namespace %q and task queue %q, so no caller can reach the program. "+
			"Create one (a dev server forgets them when it restarts): %s", ns, taskQueue, create))
	default:
		for _, name := range names {
			c.Passed = append(c.Passed, fmt.Sprintf("Nexus endpoint %q targets namespace %q and task queue %q.", name, ns, taskQueue))
		}
	}
	return c
}

// checkCalledEndpoint adds to c whether the Nexus endpoint name exists, and where it leads.
func checkCalledEndpoint(ctx context.Context, cl client.Client, name string, c *check) {
	resp, err := cl.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{PageSize: 1, Name: name})
	switch {
	case err != nil:
		c.Warnings = append(c.Warnings, fmt.Sprintf("Couldn't look up Nexus endpoint %q, which the program calls: %v. "+
			"On Temporal Cloud, check in the Cloud UI or with tcld.", name, err))
	case len(resp.GetEndpoints()) == 0:
		c.Problems = append(c.Problems, fmt.Sprintf("Nexus endpoint %q, which the program calls, doesn't exist. "+
			"Create it, targeting the namespace and task queue of the worker that serves it "+
			"(a dev server forgets endpoints when it restarts): temporal operator nexus endpoint create --name %s "+
			"--target-namespace <namespace> --target-task-queue <task queue>", name, name))
	default:
		msg := fmt.Sprintf("Nexus endpoint %q, which the program calls, exists", name)
		if w := resp.GetEndpoints()[0].GetSpec().GetTarget().GetWorker(); w != nil {
			msg += fmt.Sprintf(": it targets namespace %q and task queue %q", w.GetNamespace(), w.GetTaskQueue())
		}
		c.Passed = append(c.Passed, msg+".")
	}
}

// endpointsTargeting are the names of the Nexus endpoints whose target is a worker on namespace
// and taskQueue.
func endpointsTargeting(ctx context.Context, cl client.Client, namespace, taskQueue string) ([]string, error) {
	var names []string
	var token []byte
	for {
		resp, err := cl.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{PageSize: 100, NextPageToken: token})
		if err != nil {
			return nil, err
		}
		for _, e := range resp.GetEndpoints() {
			w := e.GetSpec().GetTarget().GetWorker()
			if w.GetNamespace() == namespace && w.GetTaskQueue() == taskQueue {
				names = append(names, e.GetSpec().GetName())
			}
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return names, nil
		}
	}
}

// profileJSON is the Temporal profile file and profile an environment selects, as envconfig
// reads them, and the variables they stand in for: a variable that is set overrides the file.
type profileJSON struct {
	File    string `json:"file"`    // the file's path; "" if there is none to read
	Profile string `json:"profile"` // the profile's name
	// Variable is the variable under which the page shows where the profile comes from.
	Variable string `json:"variable"`
	Exists   bool   `json:"exists"` // whether the file exists
	Found    bool   `json:"found"`  // whether the file has the profile
	Error    string `json:"error"`  // why the file couldn't be read; "" if it could, or doesn't exist
	// Values are the non-secret settings the profile has, by the variable that overrides each,
	// and SecretsSet the secret ones it has, by the same, without their values.
	Values     map[string]string `json:"values"`
	SecretsSet map[string]bool   `json:"secretsSet"`
}

// describeProfile is the Temporal profile env selects for programs, with what it sets, or nil if
// none of programs connects to Temporal. Secrets are those of the first program that connects.
func describeProfile(env settings.Env, programs []program) *profileJSON {
	var p *settings.Program
	for _, pr := range programs {
		if pr.ClientOptions != nil {
			p = pr.Settings
			break
		}
	}
	if p == nil {
		return nil
	}
	out := &profileJSON{
		File: settings.Get(env, settings.TemporalConfigFileEnvVar), Profile: settings.Get(env, settings.TemporalProfileEnvVar),
		Variable: settings.TemporalConfigFileEnvVar, Values: map[string]string{}, SecretsSet: map[string]bool{},
	}
	// As envconfig.LoadClientConfig and LoadClientConfigProfile choose them: empty is unset.
	if out.File == "" {
		out.File = envconfig.DefaultConfigFilePath()
	}
	if out.Profile == "" {
		out.Profile = envconfig.DefaultConfigFileProfile
	}
	if out.File == "" {
		return out
	}
	_, err := os.Stat(out.File)
	out.Exists = err == nil
	conf, err := envconfig.LoadClientConfig(envconfig.LoadClientConfigOptions{ConfigFilePath: out.File})
	if err != nil {
		out.Error = err.Error()
		return out
	}
	prof := conf.Profiles[out.Profile]
	if prof == nil {
		return out
	}
	out.Found = true
	for name, value := range profileVariables(prof) {
		switch {
		case value == "":
		case p.IsSecret(name):
			out.SecretsSet[name] = true
		default:
			out.Values[name] = value
		}
	}
	return out
}

// summary says, for the Temporal check, which profile was read and what it set.
func (pj *profileJSON) summary() string {
	switch {
	case pj.File == "":
		return "No Temporal profile file (no user config directory): only the TEMPORAL_* variables apply."
	case pj.Error != "":
		return fmt.Sprintf("Couldn't read the Temporal profile file %s: %s", pj.File, pj.Error)
	case !pj.Exists:
		return fmt.Sprintf("No Temporal profile file at %s: only the TEMPORAL_* variables apply.", pj.File)
	case !pj.Found:
		return fmt.Sprintf("The Temporal profile file %s has no profile %q: only the TEMPORAL_* variables apply.", pj.File, pj.Profile)
	}
	var names []string
	for name := range pj.Values {
		names = append(names, name)
	}
	for name := range pj.SecretsSet {
		names = append(names, name)
	}
	if len(names) == 0 {
		return fmt.Sprintf("Read profile %q from %s; it sets nothing.", pj.Profile, pj.File)
	}
	slices.Sort(names)
	return fmt.Sprintf("Read profile %q from %s; it sets %s, each overridden by its variable if set.", pj.Profile, pj.File, strings.Join(names, ", "))
}

// profileVariables are prof's settings by the variable envconfig's ApplyEnvVars overrides each
// with, as the variable would set them. A setting it doesn't have is "".
func profileVariables(prof *envconfig.ClientConfigProfile) map[string]string {
	vars := map[string]string{
		"TEMPORAL_ADDRESS":          prof.Address,
		"TEMPORAL_NAMESPACE":        prof.Namespace,
		"TEMPORAL_API_KEY":          prof.APIKey,
		"TEMPORAL_CLIENT_AUTHORITY": prof.Authority,
	}
	if tls := prof.TLS; tls != nil {
		vars["TEMPORAL_TLS"] = fmt.Sprint(!tls.Disabled)
		vars["TEMPORAL_TLS_CLIENT_CERT_PATH"] = tls.ClientCertPath
		vars["TEMPORAL_TLS_CLIENT_CERT_DATA"] = string(tls.ClientCertData)
		vars["TEMPORAL_TLS_CLIENT_KEY_PATH"] = tls.ClientKeyPath
		vars["TEMPORAL_TLS_CLIENT_KEY_DATA"] = string(tls.ClientKeyData)
		vars["TEMPORAL_TLS_SERVER_CA_CERT_PATH"] = tls.ServerCACertPath
		vars["TEMPORAL_TLS_SERVER_CA_CERT_DATA"] = string(tls.ServerCACertData)
		vars["TEMPORAL_TLS_SERVER_NAME"] = tls.ServerName
		if tls.DisableHostVerification {
			vars["TEMPORAL_TLS_DISABLE_HOST_VERIFICATION"] = "true"
		}
	}
	for key, value := range prof.GRPCMeta {
		// The variable for header x-tenant is TEMPORAL_GRPC_META_X_TENANT.
		vars[settings.TemporalGRPCMetaPrefix+strings.ToUpper(strings.ReplaceAll(key, "-", "_"))] = value
	}
	return vars
}
