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

package destination

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-redis/config"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
)

const (
	keyTypeNone   = "none"
	keyTypeStream = "stream"
)

type Destination struct {
	sdk.UnimplementedDestination

	config config.Config
	client redis.Conn
}

// NewDestination returns an instance of sdk.Destination
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (d *Destination) Parameters() cconfig.Parameters {
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
	}
}

// Configure sets up the destination by validating and parsing the config
func (d *Destination) Configure(ctx context.Context, cfg cconfig.Config) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Destination Connector...")
	conf, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	d.config = conf
	return nil
}

// Open creates a connection to redis and validates the type to key using Type <key> command
func (d *Destination) Open(ctx context.Context) error {
	address := d.config.Host + ":" + d.config.Port
	dialOptions := make([]redis.DialOption, 0)

	if d.config.Password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(d.config.Password))
	}
	if d.config.Username != "" {
		dialOptions = append(dialOptions, redis.DialUsername(d.config.Username))
	}
	dialOptions = append(dialOptions, redis.DialDatabase(d.config.Database))

	redisClient, err := redis.DialContext(ctx, "tcp", address, dialOptions...)
	if err != nil {
		return fmt.Errorf("failed to connect redis client: %w", err)
	}

	d.client = redisClient

	return d.validateKey(redisClient)
}

func (d *Destination) validateKey(client redis.Conn) error {
	switch d.config.Mode {
	case config.ModePubSub:
	// no need to verify the type or if the channel exists
	// as we can create channel with a key even if that key already exists and have some other data type

	case config.ModeStream:
		keyType, err := redis.String(client.Do("TYPE", d.config.RedisKey))
		if err != nil {
			return fmt.Errorf("error fetching type of key(%s): %w", d.config.RedisKey, err)
		}
		if keyType != keyTypeNone && keyType != keyTypeStream {
			return fmt.Errorf("invalid key type: %s, expected none or stream", keyType)
		}
	default:
		return fmt.Errorf("invalid mode(%s) encountered", string(d.config.Mode))
	}
	return nil
}

// Write receives the record to be written and based on the mode either publishes to PUB/SUB channel
// or add as key-value pair to stream using XADD, the id of the newly added key is generated automatically
func (d *Destination) Write(ctx context.Context, rec []opencdc.Record) (int, error) {
	key := d.config.RedisKey

	switch d.config.Mode {
	case config.ModePubSub:
		for i, r := range rec {
			_, err := d.doWithCtx(ctx, "PUBLISH", key, string(r.Payload.After.Bytes()))
			if err != nil {
				return i, fmt.Errorf("error publishing message to channel(%s): %w", key, err)
			}
		}

		return len(rec), nil

	case config.ModeStream:
		for i, r := range rec {
			keyValArgs, err := payloadToStreamArgs(r.Payload.After)
			if err != nil {
				return i, fmt.Errorf("invalid payload: %w", err)
			}

			args := []interface{}{
				key, "*",
			}

			args = append(args, keyValArgs...)

			_, err = d.client.Do("XADD", args...)
			if err != nil {
				return i, fmt.Errorf("error streaming message to key(%s):%w", key, err)
			}
		}

		return len(rec), nil

	default:
		return 0, fmt.Errorf("invalid mode(%s) encountered", string(d.config.Mode))
	}
}

// Teardown is called by conduit server to stop the destination connector
// the graceful shutdown is performed in this function
func (d *Destination) Teardown(_ context.Context) error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

func (d *Destination) doWithCtx(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	cwt, ok := d.client.(redis.ConnWithContext)
	if !ok {
		return d.client.Do(cmd, args)
	}
	return cwt.DoContext(ctx, cmd, args...)
}

// payloadToStreamArgs converts the payload from the record to args to be sent in redis command
func payloadToStreamArgs(payload opencdc.Data) ([]interface{}, error) {
	recMap := make(map[string]interface{})

	if err := json.Unmarshal(payload.Bytes(), &recMap); err != nil {
		return nil, fmt.Errorf("invalid json received in payload: %w", err)
	}

	keyValArgs := make([]interface{}, 0, 2*len(recMap))
	for key, val := range recMap {
		keyValArgs = append(keyValArgs, key, val)
	}
	if len(keyValArgs) == 0 {
		return nil, fmt.Errorf("no key-value pair received")
	}
	return keyValArgs, nil
}
