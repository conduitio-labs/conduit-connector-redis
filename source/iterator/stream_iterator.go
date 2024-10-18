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

package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

const (
	keyTypeNone   = "none"
	keyTypeStream = "stream"
)

type StreamIterator struct {
	key             string
	client          redis.Conn
	tomb            *tomb.Tomb
	lastID          string
	recordsPerCall  int
	pollingInterval time.Duration
	ticker          *time.Ticker
	caches          chan []opencdc.Record
	buffer          chan opencdc.Record
}

// NewStreamIterator creates a new instance of redis stream iterator and starts polling redis stream for new changes
// using the last record id of last successful row read, in a separate go routine
func NewStreamIterator(ctx context.Context,
	client redis.Conn,
	key string,
	pollingInterval time.Duration,
	position opencdc.Position,
) (*StreamIterator, error) {
	keyType, err := redis.String(client.Do("TYPE", key))
	if err != nil {
		return nil, fmt.Errorf("error fetching type of key(%s): %w", key, err)
	}
	if keyType != keyTypeNone && keyType != keyTypeStream {
		return nil, fmt.Errorf("invalid key type: %s, expected none or stream", keyType)
	}

	tmbWithCtx, _ := tomb.WithContext(ctx)
	ticker := time.NewTicker(pollingInterval)

	lastID := string(position)
	if lastID == "" {
		// if position is empty, start from 0 record
		lastID = "0-0"
	}

	cdc := &StreamIterator{
		key:             key,
		client:          client,
		tomb:            tmbWithCtx,
		recordsPerCall:  1000, // move this to config?
		lastID:          lastID,
		pollingInterval: pollingInterval,
		ticker:          ticker,
		// keeping the buffer length as 1, so that we are not blocked by one cache
		// we have other batch of records ready once first batch is read
		caches: make(chan []opencdc.Record, 1),
		buffer: make(chan opencdc.Record, 1),
	}

	cdc.tomb.Go(cdc.startIterator(ctx))
	cdc.tomb.Go(cdc.flush)

	return cdc, nil
}

// HasNext returns whether there are any more records to be returned
func (i *StreamIterator) HasNext() bool {
	return len(i.buffer) > 0 || !i.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

// Next returns the next record in buffer and error in case there are no more records
// and there was an error leading to tomb dying or context was cancelled
func (i *StreamIterator) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case rec := <-i.buffer:
		return rec, nil
	case <-i.tomb.Dying():
		return opencdc.Record{}, i.tomb.Err()
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}
}

// Stop stops the go routines
func (i *StreamIterator) Stop() error {
	i.ticker.Stop()
	i.tomb.Kill(errors.New("iterator stopped"))
	if err := i.client.Close(); err != nil {
		return fmt.Errorf("error closing the redis client: %w", err)
	}
	i.client = nil
	return nil
}

// startIterator is the go routine function used to poll the redis stream for new changes at regular intervals
func (i *StreamIterator) startIterator(_ context.Context) func() error {
	return func() error {
		defer close(i.caches)
		for {
			select {
			case <-i.tomb.Dying():
				return i.tomb.Err()
			case <-i.ticker.C:
				resp, err := redis.Values(i.client.Do("XREAD", "COUNT", i.recordsPerCall, "STREAMS", i.key, i.lastID))
				if err != nil {
					if err == redis.ErrNil {
						continue
					}
					return fmt.Errorf("error reading data from stream: %w", err)
				}
				records, err := toRecords(resp)
				if err != nil {
					return fmt.Errorf("error converting stream data to records: %w", err)
				}

				// ensure we don't fetch and keep a lot of records in memory
				// block till flush reads current array of records
				select {
				case i.caches <- records:
					i.lastID = string(records[len(records)-1].Position)

				case <-i.tomb.Dying():
					return i.tomb.Err()
				}
			}
		}
	}
}

// flush is the go routine, responsible for getting the array of records in caches channel
// and pushing them into read buffer to be returned by Next function
func (i *StreamIterator) flush() error {
	defer close(i.buffer)
	for {
		select {
		case <-i.tomb.Dying():
			return i.tomb.Err()
		case cache := <-i.caches:
			for _, record := range cache {
				select {
				case <-i.tomb.Dying():
					return i.tomb.Err()
				case i.buffer <- record:
				}
			}
		}
	}
}

// toRecords parses the XREAD command's response and returns a slice of opencdc.Record
func toRecords(resp []interface{}) ([]opencdc.Record, error) {
	records := make([]opencdc.Record, 0)
	for _, iKey := range resp {
		key, idList, err := parseKeyData(iKey)
		if err != nil {
			return nil, err
		}

		metadata := opencdc.Metadata{
			"key": string(key),
		}

		for _, iID := range idList {
			position, fieldList, err := parsePositionData(iID)
			if err != nil {
				return nil, err
			}
			rMap, err := arrInterfaceToMap(fieldList)
			if err != nil {
				return records, fmt.Errorf("error converting the []interface{} to map: %w", err)
			}
			payload, err := json.Marshal(rMap)
			if err != nil {
				return records, fmt.Errorf("error marshaling the map: %w", err)
			}

			metadata.SetCreatedAt(getTimeFromPosition(string(position)))

			records = append(records, sdk.Util.Source.NewRecordCreate(
				position,
				metadata,
				opencdc.RawData(key),
				opencdc.RawData(payload),
			))
		}
	}
	return records, nil
}

// parseKeyData parses the data for each key received in the XREAD response
func parseKeyData(d interface{}) ([]byte, []interface{}, error) {
	keyInfo, ok := d.([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("iKey: invalid data type encountered, expected:%T, got:%T", keyInfo, d)
	}
	key, ok := keyInfo[0].([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("keyInfo[0]: invalid data type encountered, expected:%T, got:%T", key, keyInfo[0])
	}
	idList, ok := keyInfo[1].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("keyInfo[0]:invalid data type encountered, expected:%T, got:%T", idList, keyInfo[1])
	}
	return key, idList, nil
}

// parsePositionData parses the id array (multiple messages) of a key
func parsePositionData(i interface{}) ([]byte, []interface{}, error) {
	idInfo, ok := i.([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("iID:invalid data type encountered, expected:%T, got:%T", idInfo, i)
	}
	position, ok := idInfo[0].([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("idInfo[0]:error invalid id type received %T expected: %T", idInfo[0], position)
	}
	fieldList, ok := idInfo[1].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("idInfo[1]:invalid data type encountered, expected:%T, got:%T", idInfo[1], fieldList)
	}
	return position, fieldList, nil
}

// getTimeFromPosition parses the id of a stream message to retrieve the creation time, if available,
// otherwise return current time.
// Message pushed to stream with auto generated id have the following format for id: <current_time_ms-int>
func getTimeFromPosition(pos string) time.Time {
	posParts := strings.Split(pos, "-")
	ts, err := strconv.ParseInt(posParts[0], 10, 64)
	if err != nil || ts == 0 {
		// ignore the error, in case of custom id, we will use time.Now()
		ts = time.Now().UnixMilli()
	}
	return time.UnixMilli(ts)
}

// arrInterfaceToMap converts the stream key-val response to map[string]string
func arrInterfaceToMap(values []interface{}) (map[string]string, error) {
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("arrInterfaceToMap expects even number of values result, got %d", len(values))
	}

	m := make(map[string]string, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].([]byte)
		if !ok {
			return nil, fmt.Errorf("arrInterfaceToMap key[%d] not a bulk string value, got %T", i, values[i])
		}

		value, ok := values[i+1].([]byte)
		if !ok {
			return nil, fmt.Errorf("arrInterfaceToMap value[%d] not a bulk string value, got %T", i+1, values[i+1])
		}

		m[string(key)] = string(value)
	}
	return m, nil
}
