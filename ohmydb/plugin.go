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

package ohmydb

import (
	"context"
	"fmt"

	"github.com/lastbackend/toolkit"
	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/runtime/logger"
	omd "github.com/ohmydb/ohmydb/client"
)

const (
	defaultPrefix = "ohmydb"
)

type Plugin interface {
	toolkit.Plugin
	DB() omd.DB
	Info()
}

type Options struct {
	Name string
}

type Config struct {
	DSN  string `env:"DSN"  envDefault:"" comment:"DSN = omd://[netloc][:port] complete connection string"`
	Host string `env:"HOST" envDefault:"127.0.0.1"  comment:"The host to connect to (required)"`
	Port int32  `env:"PORT" envDefault:"7540" comment:"The port to bind to (default: 7540)"`
}

type plugin struct {
	log     logger.Logger
	runtime runtime.Runtime

	prefix    string
	envPrefix string

	opts Config

	conn omd.DB
}

func (p *plugin) DB() omd.DB {
	return p.conn
}

func (p *plugin) Info() {
	p.runtime.Config().Print(p.opts, p.prefix)
}

func (p *plugin) PreStart(ctx context.Context) (err error) {
	if p.opts.DSN == "" {
		if p.opts.Host == "" {
			return fmt.Errorf("%s_DSN or %s_Host environment variable required but not set", p.prefix, p.prefix)
		}
		p.opts.DSN = fmt.Sprintf("omd://%s:%d", p.opts.Host, p.opts.Port)
	}

	conn, err := omd.Open(p.runtime, p.opts.DSN)
	if err != nil {
		return err
	}

	p.conn = conn

	return nil
}

func (p *plugin) OnStop(context.Context) error {
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
