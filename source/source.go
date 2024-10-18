// Copyright © 2022 Meroxa, Inc. & Gophers Lab Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate mockery --name=Iterator --outpkg mocks

package source

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-redis/config"
	"github.com/conduitio-labs/conduit-connector-redis/source/iterator"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
)

type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
}

type Iterator interface {
	HasNext() bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop() error
}

// NewSource returns an instance of sdk.Source
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() cconfig.Parameters {
	return map[string]cconfig.Parameter{
		config.KeyHost: {
			Default:     "localhost",
			Description: "Host to the redis source",
		},
		config.KeyPort: {
			Default:     "6379",
			Description: "Port to the redis source",
		},
		config.KeyRedisKey: {
			Default:     "",
			Description: "Key name for connector to read",
			Validations: []cconfig.Validation{cconfig.ValidationRequired{}},
		},
		config.KeyDatabase: {
			Default:     "0",
			Description: "Database name for the redis source",
		},
		config.KeyPassword: {
			Default:     "",
			Description: "Password to the redis source",
		},
		config.KeyUsername: {
			Default:     "",
			Description: "Username to the redis source",
		},
		config.KeyMode: {
			Default:     "pubsub",
			Description: "Sets the connector's operation mode. Available modes: ['pubsub', 'stream']",
		},
		config.KeyPollingPeriod: {
			Default:     "1s",
			Description: "Time duration between successive data polling from streams",
		},
	}
}

// Configure validates the passed config and prepares the source connector
func (s *Source) Configure(ctx context.Context, cfg cconfig.Config) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Source Connector...")
	conf, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	s.config = conf
	return nil
}

// Open prepare the plugin to start reading records from the given position
func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	address := s.config.Host + ":" + s.config.Port
	dialOptions := make([]redis.DialOption, 0)
	if s.config.Password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(s.config.Password))
	}
	if s.config.Username != "" {
		dialOptions = append(dialOptions, redis.DialUsername(s.config.Username))
	}
	dialOptions = append(dialOptions, redis.DialDatabase(s.config.Database))

	redisClient, err := redis.DialContext(ctx, "tcp", address, dialOptions...)
	if err != nil {
		return fmt.Errorf("failed to connect redis client: %w", err)
	}

	switch s.config.Mode {
	case config.ModePubSub:
		s.iterator, err = iterator.NewPubSubIterator(ctx, redisClient, s.config.RedisKey)
		if err != nil {
			return fmt.Errorf("couldn't create a pubsub iterator: %w", err)
		}
	case config.ModeStream:
		s.iterator, err = iterator.NewStreamIterator(ctx, redisClient, s.config.RedisKey, s.config.PollingPeriod, position)
		if err != nil {
			return fmt.Errorf("couldn't create a stream iterator: %w", err)
		}
	default:
		return fmt.Errorf("invalid mode(%v) encountered", s.config.Mode)
	}

	return nil
}

// Read gets the next object
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	if !s.iterator.HasNext() {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
	rec, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error fetching next record: %w", err)
	}
	return rec, nil
}

// Ack is called by the conduit server after the record has been successfully processed by all destination connectors
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Debug().
		Str("position", string(position)).
		Str("mode", string(s.config.Mode)).
		Msg("position ack received")
	return nil
}

// Teardown is called by the conduit server to stop the source connector
// all the cleanup should be done in this function
func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
		s.iterator = nil
	}
	return nil
}
