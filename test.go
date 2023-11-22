package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/sync/errgroup"

	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/ptypes"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	tgsync "github.com/testground/sdk-go/sync"
)

// Create a new libp2p host
func createHost(ctx context.Context) (host.Host, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		return nil, err
	}

	// Don't listen yet, we need to set up networking first
	return libp2p.New(libp2p.Identity(priv), libp2p.NoListenAddrs)
}

// setupNetwork instructs the sidecar (if enabled) to setup the network for this
// test case.
func setupNetwork(ctx context.Context, runenv *runtime.RunEnv, netclient *network.Client) error {
	if !runenv.TestSidecar {
		return nil
	}

	// Wait for the network to be initialized.
	runenv.RecordMessage("Waiting for network initialization")
	err := netclient.WaitNetworkInitialized(ctx)
	if err != nil {
		return err
	}
	runenv.RecordMessage("Network init complete")

	config := &network.Config{
		Network: "default",
		Enable:  true,
		Default: network.LinkShape{
			Latency:   time.Millisecond * 10,
			Bandwidth: 1000000000, //Equivalent to 100Mps
		},
		CallbackState: "network-configured",
		RoutingPolicy: network.DenyAll,
	}

	// random delay to avoid overloading weave (we hope)
	delay := time.Duration(rand.Intn(1000)) * time.Millisecond
	<-time.After(delay)
	err = netclient.ConfigureNetwork(ctx, config)
	if err != nil {
		return err
	}

	return nil
}

// Listen on the address in the testground data network
func listenAddrs(netclient *network.Client) []multiaddr.Multiaddr {
	ip, err := netclient.GetDataNetworkIP()
	if err == network.ErrNoTrafficShaping {
		ip = net.ParseIP("0.0.0.0")
	} else if err != nil {
		panic(fmt.Errorf("error getting data network addr: %s", err))
	}

	dataAddr, err := manet.FromIP(ip)
	if err != nil {
		panic(fmt.Errorf("could not convert IP to multiaddr; ip=%s, err=%s", ip, err))
	}

	// add /tcp/0 to auto select TCP listen port
	listenAddr := dataAddr.Encapsulate(multiaddr.StringCast("/tcp/0"))
	return []multiaddr.Multiaddr{listenAddr}
}

// Called when nodes are ready to start the run, and are waiting for all other nodes to be ready
func waitForReadyState(ctx context.Context, runenv *runtime.RunEnv, client tgsync.Client) error {
	// Set a state barrier.

	state := tgsync.State("ready")
	doneCh := client.MustBarrier(ctx, state, runenv.TestInstanceCount).C

	// Signal we've entered the state.
	_, err := client.SignalEntry(ctx, state)
	if err != nil {
		return err
	}

	// Wait until all others have signalled.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-doneCh:
		if err != nil {
			return err
		}
	}

	return nil
}

func test(runenv *runtime.RunEnv, initCtx *run.InitContext) error {

	params := parseParams(runenv)

	setup := params.setup
	warmup := params.warmup
	cooldown := params.cooldown
	runTime := params.runtime
	totalTime := setup + runTime + warmup + cooldown

	ctx, cancel := context.WithTimeout(context.Background(), totalTime)
	defer cancel()

	runenv.RecordMessage("before sync.MustBoundClient")

	client := tgsync.MustBoundClient(ctx, runenv)
	defer client.Close()

	runenv.RecordMessage("after sync.MustBoundClient")

	//client := initCtx.SyncClient
	//netclient := initCtx.NetClient
	netclient := network.NewClient(client, runenv)

	// Create the hosts, but don't listen yet (we need to set up the data
	// network before listening)

	h, err := createHost(ctx)
	if err != nil {
		return err
	}

	peers := tgsync.NewTopic("nodes", &peer.AddrInfo{})

	// Get sequence number within a node type (eg honest-1, honest-2, etc)
	// signal entry in the 'enrolled' state, and obtain a sequence number.
	seq, err := client.Publish(ctx, peers, host.InfoFromHost(h))

	if err != nil {
		return fmt.Errorf("failed to write peer subtree in sync service: %w", err)
	}

	runenv.RecordMessage("before netclient.MustConfigureNetwork")

	if err := setupNetwork(ctx, runenv, netclient); err != nil {
		return fmt.Errorf("Failed to set up network: %w", err)
	}

	netclient.MustWaitNetworkInitialized(ctx)
	runenv.RecordMessage("my sequence ID: %d %s", seq, h.ID())

	peerSubscriber := NewPeerSubscriber(ctx, runenv, client, runenv.TestInstanceCount)

	var topology Topology
	topology = RandomHonestTopology{
		Count:          params.overlayParams.d,
		PublishersOnly: false,
	}

	discovery, err := NewSyncDiscovery2(h, runenv, peerSubscriber, topology)

	if err != nil {
		return fmt.Errorf("error creating discovery service: %w", err)
	}

	// Listen for incoming connections
	laddr := listenAddrs(netclient)
	runenv.RecordMessage("listening on %s", laddr)
	if err = h.Network().Listen(laddr...); err != nil {
		return nil
	}

	id := host.InfoFromHost(h).ID
	runenv.RecordMessage("Host peer ID: %s, seq %d,  addrs: %v",
		id.Loggable(), seq, h.Addrs())

	err = discovery.registerAndWait(ctx)

	runenv.RecordMessage("Peers discovered %d", len(discovery.allPeers))
	if err != nil {
		runenv.RecordMessage("Failing register and wait")
		return fmt.Errorf("error waiting for discovery service: %s", err)
	}

	rate := ptypes.Rate{Quantity: 5, Interval: time.Second}
	topic := TopicConfig{Id: "block_channel", MessageRate: rate, MessageSize: 300 * 1024}
	var topics = make([]TopicConfig, 0)
	topics = append(topics, topic)

	var pub bool
	if seq == 1 {
		pub = true
	} else {
		pub = false
	}
	tracerOut := fmt.Sprintf("%s%ctracer-output-%d", runenv.TestOutputsPath, os.PathSeparator, seq)
	tracer, err := NewTestTracer(tracerOut, h.ID(), true)

	cfg := NodeConfig{
		Publisher:       pub,
		FloodPublishing: false,
		PeerScoreParams: params.scoreParams,
		OverlayParams:   params.overlayParams,
		//PeerScoreInspect:        scoreInspectParams,
		Topics:                  topics,
		Tracer:                  tracer,
		Seq:                     seq,
		Warmup:                  time.Second * 3,
		Cooldown:                time.Second * 10,
		Heartbeat:               params.heartbeat,
		ValidateQueueSize:       params.validateQueueSize,
		OutboundQueueSize:       params.outboundQueueSize,
		OpportunisticGraftTicks: params.opportunisticGraftTicks,
	}

	p, err := createPubSubNode(ctx, runenv, seq, h, discovery, cfg)
	if err != nil {
		runenv.RecordMessage("Failing create pubsub npde")
		return fmt.Errorf("error waiting for discovery service: %s", err)
	}

	if err := waitForReadyState(ctx, runenv, client); err != nil {
		return err
	}

	errgrp, ctx := errgroup.WithContext(ctx)

	errgrp.Go(func() (err error) {
		p.Run(runTime)

		runenv.RecordMessage("Host peer ID: %s, seq %d, addrs: %v", id, seq, h.Addrs())
		if err2 := tracer.Stop(); err2 != nil {
			runenv.RecordMessage("error stopping test tracer: %s", err2)
		}
		return
	})

	runenv.RecordMessage("finishing test")
	return errgrp.Wait()
	//runenv.RecordMessage("finishing test")
	//time.Sleep(10 * time.Second)

	//return nil
}
