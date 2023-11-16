package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/testground/sdk-go/network"
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
			Latency:   time.Millisecond * 50,
			Bandwidth: 100 * 1024 * 1024,
		},
		CallbackState: "network-configured",
		RoutingPolicy: network.AllowAll,
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

/* Called when nodes are ready to start the run, and are waiting for all other nodes to be ready
func waitForReadyState(ctx context.Context, client tgsync.Client) error {
	// Set a state barrier.

	state := tgsync.State("ready")
	doneCh := client.MustBarrier(ctx, state, t.params.containerNodesTotal).C

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
}*/

func test(runenv *runtime.RunEnv, initCtx *run.InitContext) error {

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
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

	discovery, err := NewSyncDiscovery2(h, runenv, peerSubscriber)

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
	//if seq == 1 {
	for i, peer := range discovery.allPeers {
		runenv.RecordMessage("Connecting to %d %s ", i, peer.Info)
		//discovery.connectWithRetry(ctx, peer.Info)
		err := h.Connect(ctx, peer.Info)
		if err != nil {
			panic(err)
		} else {
			runenv.RecordMessage("Connection succesful")
		}

	}

	//}
	time.Sleep(10 * time.Second)

	return nil
}
