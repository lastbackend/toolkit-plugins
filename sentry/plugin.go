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
	"fmt"
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
	Client() *sentry.Client
	Info()
}

type Options struct {
	Name string
}

type Config struct {
	DSN         string `env:"DSN" envDefault:"" comment:"Sentry DSN"`
	Environment string `env:"ENVIRONMENT" envDefault:"production" comment:"Environment"`
}

type plugin struct {
	log     logger.Logger
	runtime runtime.Runtime

	prefix    string
	envPrefix string

	opts Config

	client *sentry.Client
}

func (p *plugin) Client() *sentry.Client {
	return p.client
}

func (p *plugin) Info() {
	p.runtime.Config().Print(p.opts, p.prefix)
}

func (p *plugin) PreStart(ctx context.Context) (err error) {
	if p.opts.DSN == "" {
		return fmt.Errorf("%s_DSN environment variable required but not set", p.prefix)
	}

	err = sentry.Init(sentry.ClientOptions{
		Dsn:         p.opts.DSN,
		Environment: p.opts.Environment,
	})
	if err != nil {
		return err
	}

	p.client = sentry.CurrentHub().Client()

	return nil
}

func (p *plugin) OnStop(context.Context) error {
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
