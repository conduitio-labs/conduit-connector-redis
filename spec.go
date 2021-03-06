/*
Copyright © 2022 Meroxa, Inc. & Gophers Lab Technologies Pvt. Ltd.

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

package redis

import (
	"github.com/conduitio-labs/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "Redis",
		Summary:     "A Redis source and destination plugin for Conduit, written in Go.",
		Description: "A Redis source and destination plugin for Conduit, written in Go. It supports Redis Pub/Sub and Streams.",
		Version:     "v0.1.0",
		Author:      "Gophers Lab Technologies Pvt Ltd",
		DestinationParams: map[string]sdk.Parameter{
			config.KeyHost: {
				Default:     "localhost",
				Required:    false,
				Description: "host to the redis source.",
			},
			config.KeyPort: {
				Default:     "6379",
				Required:    false,
				Description: "port to the redis source",
			},
			config.KeyRedisKey: {
				Default:     "",
				Required:    true,
				Description: "key name for connector to read.",
			},
			config.KeyDatabase: {
				Default:     "0",
				Required:    false,
				Description: "database name for the redis source",
			},
			config.KeyPassword: {
				Default:     "",
				Required:    false,
				Description: "Password to the redis source.",
			},
			config.KeyUsername: {
				Default:     "",
				Required:    false,
				Description: "Username to the redis source.",
			},
			config.KeyMode: {
				Default:     "pubsub",
				Required:    false,
				Description: "Sets the connector's operation mode. Available modes: ['pubsub', 'stream']",
			},
		},
		SourceParams: map[string]sdk.Parameter{
			config.KeyHost: {
				Default:     "localhost",
				Required:    false,
				Description: "host to the redis source.",
			},
			config.KeyPort: {
				Default:     "6379",
				Required:    false,
				Description: "port to the redis source",
			},
			config.KeyRedisKey: {
				Default:     "",
				Required:    true,
				Description: "key name for connector to read.",
			},
			config.KeyDatabase: {
				Default:     "0",
				Required:    false,
				Description: "database name for the redis source",
			},
			config.KeyPassword: {
				Default:     "",
				Required:    false,
				Description: "Password to the redis source.",
			},
			config.KeyUsername: {
				Default:     "",
				Required:    false,
				Description: "Username to the redis source.",
			},
			config.KeyMode: {
				Default:     "pubsub",
				Required:    false,
				Description: "Sets the connector's operation mode. Available modes: ['pubsub', 'stream']",
			},
			config.KeyPollingPeriod: {
				Default:     "1s",
				Required:    false,
				Description: "Time duration between successive data polling from streams",
			},
		},
	}
}
