/*
Copyright [2014] - [2024] The Last.Backend authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sentry

import (
	"context"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/lastbackend/toolkit"
	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/runtime/logger"
)

const (
	defaultPrefix = "sentry"
)

type Plugin interface {
	toolkit.Plugin
	Client() *sentry.Hub
	Info()
}

type Options struct {
	Name string
}

type Config struct {
	DSN              string `env:"DSN" envDefault:"" comment:"The DSN to use. If the DSN is not set, the client is effectively disabled."`
	Environment      string `env:"ENVIRONMENT" envDefault:"production" comment:"The environment to be sent with events."`
	Release          string `env:"RELEASE" envDefault:"" comment:"The release to be sent with events."`
	Dist             string `env:"DIST" envDefault:"" comment:"The dist to be sent with events."`
	AttachStacktrace bool   `env:"ATTACH_STACKTRACE" envDefault:"false" comment:"Configures whether SDK should generate and attach stacktraces to pure capture message calls."`
	Debug            bool   `env:"DEBUG" envDefault:"false" comment:"In debug mode, the debug information is printed to stdout to help you understand what sentry is doing."`
}

type plugin struct {
	log     logger.Logger
	runtime runtime.Runtime

	prefix    string
	envPrefix string

	opts Config

	client *sentry.Hub

	isRunning bool
}

func (p *plugin) Client() *sentry.Hub {
	return p.client
}

func (p *plugin) Info() {
	p.runtime.Config().Print(p.opts, p.prefix)
}

func (p *plugin) PreStart(ctx context.Context) (err error) {
	err = sentry.Init(sentry.ClientOptions{
		Debug:            p.opts.Debug,
		Dsn:              p.opts.DSN,
		Environment:      p.opts.Environment,
		Release:          p.opts.Release,
		Dist:             p.opts.Dist,
		AttachStacktrace: p.opts.AttachStacktrace,
	})
	if err != nil {
		return err
	}

	p.client = sentry.CurrentHub()

	p.isRunning = true

	return nil
}

func (p *plugin) OnStop(context.Context) error {
	if !p.isRunning {
		return nil
	}
	sentry.Flush(2 * time.Second)
	return nil
}

func NewPlugin(rnt runtime.Runtime, opts *Options) Plugin {
	p := new(plugin)

	p.runtime = rnt
	p.log = rnt.Log()

	p.prefix = opts.Name
	if p.prefix == "" {
		p.prefix = defaultPrefix
	}

	if err := rnt.Config().Parse(&p.opts, p.prefix); err != nil {
		return nil
	}

	return p
}
