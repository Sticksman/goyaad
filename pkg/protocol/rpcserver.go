package protocol

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urjitbhatia/goyaad/pkg/goyaad"
)

// ErrTimeout indicates that no new jobs were ready to be consumed within the given timeout duration
var ErrTimeout = errors.New("No new jobs available in given timeout")

// RPCServer exposes a Yaad hub backed RPC endpoint
type RPCServer struct {
	hub *goyaad.Hub
}

func newRPCServer(hub *goyaad.Hub) *RPCServer {
	return &RPCServer{hub: hub}
}

// PutWithID accepts a new job and stores it in a Hub, reply is ignored
func (r *RPCServer) PutWithID(job goyaad.Job, ignoredReply *int8) error {
	return r.hub.AddJob(&job)
}

// Cancel deletes the job pointed to by the id, reply is ignored
// If the job doesn't exist, no error is returned so calls to Cancel are idempotent
func (r *RPCServer) Cancel(id string, ignoredReply *int8) error {
	return r.hub.CancelJob(id)
}

// Next sets the reply (job) to a valid job if a job is ready to be triggered
// If not job is ready yet, this call will wait (block) for the given duration and keep searching
// for ready jobs. If no job is ready by the end of the timeout, ErrTimeout is returned
func (r *RPCServer) Next(timeout time.Duration, job *goyaad.Job) error {
	// try once
	if j := r.hub.Next(); j != nil {
		*job = *j
		return nil
	}
	// if we couldn't find a ready job and timeout was set to 0
	if timeout.Seconds() == 0 {
		return ErrTimeout
	}

	waitTill := time.Now().Add(timeout)
	// wait for timeout and keep trying
	logrus.Debugf("waiting for reserve timeout: %v now: %v till: %v ", timeout, time.Now(), waitTill)
	for waitTill.After(time.Now()) {
		if j := r.hub.Next(); j != nil {
			*job = *j
			return nil
		}
		time.Sleep(time.Millisecond * 200)
		logrus.Debug("waiting for reserve finished sleep for total timeout: ", timeout)
	}

	return ErrTimeout
}

// Ping the server, sets "pong" as the reply
// useful for basic connectivity/liveness check
func (r *RPCServer) Ping(ignore int8, pong *string) error {
	logrus.Debug("Received ping from client")
	*pong = "pong"
	return nil
}

// NextID returns the next ID a client should use to create a job
// This method is only for legacy compatibility and should not be used for new integrations
func (r *RPCServer) NextID(ignore int8, id *string) error {
	*id = fmt.Sprintf("%d", goyaad.NextID())
	return nil
}

// ServeRPC starts serving hub over rpc
func ServeRPC(opts *goyaad.HubOpts, addr string) error {
	hub := goyaad.NewHub(opts)
	srv := newRPCServer(hub)
	rpc.Register(srv)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	return http.Serve(l, nil)
}
