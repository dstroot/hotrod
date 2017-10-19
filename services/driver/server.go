// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/dstroot/hotrod/pkg/log"
	"github.com/dstroot/hotrod/pkg/pool"
	"github.com/dstroot/hotrod/services/config"
	"github.com/dstroot/hotrod/services/driver/thrift-gen/driver"
)

// Server implements jaeger-demo-frontend service
type Server struct {
	hostPort string
	tracer   opentracing.Tracer
	logger   log.Factory
	ch       *tchannel.Channel
	server   *thrift.Server
	redis    *Redis
	pool     *pool.Pool
}

// NewServer creates a new driver.Server
func NewServer(hostPort string, tracer opentracing.Tracer, metricsFactory metrics.Factory, logger log.Factory) *Server {
	channelOpts := &tchannel.ChannelOptions{
		Tracer: tracer,
	}
	ch, err := tchannel.NewChannel("driver", channelOpts)
	if err != nil {
		logger.Bg().Fatal("Cannot create TChannel", zap.Error(err))
	}
	server := thrift.NewServer(ch)

	return &Server{
		hostPort: hostPort,
		tracer:   tracer,
		logger:   logger,
		ch:       ch,
		server:   server,
		redis:    newRedis(metricsFactory, logger),
		pool:     pool.New(config.RouteWorkerPoolSize),
	}
}

// Run starts the Driver server
func (s *Server) Run() error {

	s.server.Register(driver.NewTChanDriverServer(s))

	if err := s.ch.ListenAndServe(s.hostPort); err != nil {
		s.logger.Bg().Fatal("Unable to start tchannel server", zap.Error(err))
	}

	peerInfo := s.ch.PeerInfo()
	s.logger.Bg().Info("TChannel listening", zap.String("hostPort", peerInfo.HostPort))

	// Run must block, but TChannel's ListenAndServe runs in the background, so block indefinitely
	select {}
}

// FindNearest implements Thrift interface TChanDriver
func (s *Server) FindNearest(ctx thrift.Context, location string) ([]*driver.DriverLocation, error) {
	// get 10 nearby drivers
	s.logger.For(ctx).Info("Searching for nearby drivers", zap.String("location", location))
	driverIDs := s.redis.FindDriverIDs(ctx, location)

	// create a slice and a WaitGroup
	driverList := make([]*driver.DriverLocation, len(driverIDs))
	var wg sync.WaitGroup

	// lookup each driver location
	for i, driverID := range driverIDs {
		wg.Add(1)

		// Maybe eventually we would need some type of "pool" of workers concept such as
		// in the frontend best_eta getRoutes function.  However we know this is always
		// just 10 drivers, and we have to pass in data to make the function work. So we
		// can run 10 parallel go routines.
		go func(i int, driverID string, ctx thrift.Context) {
			defer wg.Done() // Decrement counter when the goroutine completes.
			s.logger.For(ctx).Info("Started goroutine", zap.Int("goroutine", i), zap.String("driver_id", driverID))

			var drv Driver
			var err error

			// attempt to get driver three times
			for i := 0; i < 3; i++ {
				drv, err = s.redis.GetDriver(ctx, driverID)
				if err == nil {
					break
				}
				s.logger.For(ctx).Error("Retrying GetDriver after error", zap.Int("retry_no", i+1), zap.Error(err))
			}
			if err != nil {
				s.logger.For(ctx).Error("Failed to get driver after 3 attempts", zap.Error(err))
				return
			}

			// put the driver location into the drivers slice
			driverList[i] = &driver.DriverLocation{
				DriverID: drv.DriverID,
				Location: drv.Location,
			}

		}(i, driverID, ctx)
	}
	wg.Wait()
	s.logger.For(ctx).Info("Search successful", zap.Int("num_drivers", len(driverList)))
	return driverList, nil
}
