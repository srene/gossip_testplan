package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/ptypes"
	"github.com/testground/sdk-go/runtime"
	tgsync "github.com/testground/sdk-go/sync"
)

type Msg struct {
	Sender string
	Seq    int64
	Data   []byte
}

type NodeConfig struct {
	// topics to join when node starts
	Topics []TopicConfig

	// whether we're a publisher or a lurker
	Publisher bool

	FloodPublishing bool

	// pubsub event tracer
	Tracer pubsub.EventTracer

	// Test instance identifier
	Seq int64

	//How long to wait after connecting to bootstrap peers before publishing
	Warmup time.Duration

	// How long to wait for cooldown
	Cooldown time.Duration

	// Gossipsub heartbeat params
	Heartbeat HeartbeatParams

	Failure bool

	FailureDuration time.Duration
	// whether to flood the network when publishing our own messages.
	// Ignored unless hardening_api build tag is present.
	//FloodPublishing bool

	// Params for peer scoring function. Ignored unless hardening_api build tag is present.
	PeerScoreParams ScoreParams

	OverlayParams OverlayParams

	// Params for inspecting the scoring values.
	//PeerScoreInspect InspectParams

	// Size of the pubsub validation queue.
	ValidateQueueSize int

	// Size of the pubsub outbound queue.
	OutboundQueueSize int

	// Heartbeat tics for opportunistic grafting
	OpportunisticGraftTicks int
}

type TopicConfig struct {
	Id          string
	MessageRate ptypes.Rate
	MessageSize ptypes.Size
}

type topicState struct {
	cfg       TopicConfig
	nMessages int64
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	pubTicker *time.Ticker
	done      chan struct{}
}

type PubsubNode struct {
	cfg       NodeConfig
	ctx       context.Context
	shutdown  func()
	seq       int64
	runenv    *runtime.RunEnv
	h         host.Host
	ps        *pubsub.PubSub
	discovery *SyncDiscovery
	lk        sync.RWMutex
	topics    map[string]*topicState
	pubwg     sync.WaitGroup
	netclient *network.Client
	netconfig *network.Config
}

func createPubSubNode(ctx context.Context, runenv *runtime.RunEnv, seq int64, h host.Host, discovery *SyncDiscovery, netclient *network.Client, netconfig *network.Config, cfg NodeConfig) (*PubsubNode, error) {
	opts, err := pubsubOptions(cfg)
	if err != nil {
		return nil, err
	}

	// Set the heartbeat initial delay and interval
	pubsub.GossipSubHeartbeatInitialDelay = cfg.Heartbeat.InitialDelay
	pubsub.GossipSubHeartbeatInterval = cfg.Heartbeat.Interval
	pubsub.GossipSubHistoryLength = 100
	pubsub.GossipSubHistoryGossip = 50

	ps, err := pubsub.NewGossipSub(ctx, h, opts...)

	if err != nil {
		fmt.Errorf("error making new gossipsub: %s", err)
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	p := &PubsubNode{
		cfg:       cfg,
		ctx:       ctx,
		shutdown:  cancel,
		seq:       seq,
		runenv:    runenv,
		h:         h,
		ps:        ps,
		discovery: discovery,
		topics:    make(map[string]*topicState),
		netclient: netclient,
		netconfig: netconfig,
	}

	p.connectTopology(ctx, cfg.Warmup)

	return p, nil
}

func pubsubOptions(cfg NodeConfig) ([]pubsub.Option, error) {
	opts := []pubsub.Option{
		pubsub.WithEventTracer(cfg.Tracer),
	}

	if cfg.ValidateQueueSize > 0 {
		opts = append(opts, pubsub.WithValidateQueueSize(cfg.ValidateQueueSize))
	}

	if cfg.OutboundQueueSize > 0 {
		opts = append(opts, pubsub.WithPeerOutboundQueueSize(cfg.OutboundQueueSize))
	}

	// Set the overlay parameters
	if cfg.OverlayParams.d >= 0 {
		pubsub.GossipSubD = cfg.OverlayParams.d
	}
	if cfg.OverlayParams.dlo >= 0 {
		pubsub.GossipSubDlo = cfg.OverlayParams.dlo
	}
	if cfg.OverlayParams.dhi >= 0 {
		pubsub.GossipSubDhi = cfg.OverlayParams.dhi
	}

	return opts, nil
}

func (p *PubsubNode) connectTopology(ctx context.Context, warmup time.Duration) error {
	// Default to a connect delay in the range of 0s - 1s
	delay := time.Duration(rand.Intn(int(warmup.Seconds()))) * time.Second
	// Connect to other peers in the topology
	err := p.discovery.ConnectTopology(ctx, delay)
	if err != nil {
		p.runenv.RecordMessage("Error connecting to topology peer: %s", err)
	}

	return nil
}

func (p *PubsubNode) Run(runtime time.Duration) error {
	defer func() {
		// end subscription goroutines before exit
		for _, ts := range p.topics {
			ts.done <- struct{}{}
		}
		p.runenv.RecordMessage("Shutting down")
		p.shutdown()
	}()

	// Wait for all nodes to be in the ready state (including attack nodes)
	// then start connecting (asynchronously)
	/*if err := waitForReadyStateThenConnectAsync(p.ctx); err != nil {
		return err
	}*/

	// ensure we have at least enough peers to fill a mesh after warmup period
	npeers := len(p.h.Network().Peers())
	if npeers < pubsub.GossipSubDlo {
		//panic(fmt.Errorf("not enough peers after warmup period. Need at least D=%d, have %d", pubsub.GossipSubDlo, npeers))
		p.runenv.RecordMessage("not enough peers after warmup period. Need at least D=%d, have %d", pubsub.GossipSubD, npeers)
		selected := p.discovery.topology.SelectNPeers(pubsub.GossipSubD-npeers, p.h.ID(), p.discovery.allPeers)
		p.discovery.ConnectingToPeers(p.ctx, selected)
	}

	//wait for warmup time to expire
	p.runenv.RecordMessage("Wait for %s warmup time", p.cfg.Warmup)
	select {
	case <-time.After(p.cfg.Warmup):
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	if p.cfg.Failure {
		go func() {
			select {
			case <-time.After(p.cfg.Warmup * 2):
			case <-p.ctx.Done():
				return
			}
			p.runenv.RecordMessage("Node stopped !!!!!!!!!!!!!!!")
			for _, peer := range p.h.Network().Peers() {
				p.h.Network().ClosePeer(peer)
			}

			select {
			case <-time.After(p.cfg.FailureDuration):
			case <-p.ctx.Done():
				return
			}
			p.runenv.RecordMessage("Node up again !!!!!!!!!!!!!!!")

			err2 := p.discovery.ConnectTopology(p.ctx, 0)
			if err2 != nil {
				p.runenv.RecordMessage("Error connecting to topology peer: %s", err2)
			}
		}()
	}
	// join initial topics
	p.runenv.RecordMessage("Joining initial topics %d.", len(p.cfg.Topics))
	for _, t := range p.cfg.Topics {
		p.runenv.RecordMessage("Joining topic %s %d.", t.Id, t.MessageSize)
		go p.joinTopic(t, runtime)
	}

	p.runenv.RecordMessage("Starting gossipsub. Connected to %d peers.", len(p.h.Network().Peers()))
	// block until complete
	p.runenv.RecordMessage("Wait for %s run time", runtime)
	select {
	case <-time.After(runtime):
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	// if we're publishing, wait until we've sent all our messages or the context expires
	if p.cfg.Publisher {
		donech := make(chan struct{}, 1)
		go func() {
			p.pubwg.Wait()
			donech <- struct{}{}
		}()

		select {
		case <-donech:
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}

	p.runenv.RecordMessage("Run time complete, cooling down for %s", p.cfg.Cooldown)
	select {
	case <-time.After(p.cfg.Cooldown):
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	p.runenv.RecordMessage("Cool down complete")

	return nil
}

func (p *PubsubNode) joinTopic(t TopicConfig, runtime time.Duration) {
	p.lk.Lock()
	defer p.lk.Unlock()

	publishInterval := time.Duration(float64(t.MessageRate.Interval) / t.MessageRate.Quantity)
	totalMessages := int64(runtime / publishInterval)

	if p.cfg.Publisher {
		p.log("publishing to topic %s. message_rate: %.2f/%ds, publishInterval %dms, msg size %d bytes. total expected messages: %d",
			t.Id, t.MessageRate.Quantity, t.MessageRate.Interval/time.Second, publishInterval/time.Millisecond, t.MessageSize, totalMessages)
	} else {
		p.log("joining topic %s as a lurker", t.Id)
	}

	if _, ok := p.topics[t.Id]; ok {
		// already joined, ignore
		return
	}
	topic, err := p.ps.Join(t.Id)
	if err != nil {
		p.log("error joining topic %s: %s", t.Id, err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		p.log("error subscribing to topic %s: %s", t.Id, err)
		return
	}
	p.runenv.RecordMessage("Subscribed to topic %s.", t.Id)
	ts := topicState{
		cfg:       t,
		topic:     topic,
		sub:       sub,
		nMessages: totalMessages,
		done:      make(chan struct{}, 1),
	}
	p.topics[t.Id] = &ts
	go p.consumeTopic(&ts)

	if err := waitTillAllJoined(p.ctx, p.runenv, tgsync.MustBoundClient(p.ctx, p.runenv)); err != nil {
		return
	}

	if !p.cfg.Publisher {
		return
	}

	go func() {
		p.runenv.RecordMessage("Starting publisher with %s publish interval", publishInterval)
		ts.pubTicker = time.NewTicker(publishInterval)
		p.publishLoop(&ts)
	}()
}

// Called when nodes are ready to start the run, and are waiting for all other nodes to be ready
func waitTillAllJoined(ctx context.Context, runenv *runtime.RunEnv, client tgsync.Client) error {
	// Set a state barrier.

	state := tgsync.State("joined")
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

func (p *PubsubNode) consumeTopic(ts *topicState) {
	for {
		msg, err := ts.sub.Next(p.ctx)
		if err != nil /*&& err != context.Canceled*/ {
			p.log("error reading from %s: %s", ts.cfg.Id, err)
			return
		}
		//p.log("got message")
		var message Msg
		err = json.Unmarshal(msg.Data, &message)
		if err != nil /*&& err != context.Canceled*/ {
			p.log("error reading data")
			return
		}
		//p.log("Data received %s", msg.Data)
		p.log("got message %d  hops for topic %s, sent by %s\n", message.Seq, ts.cfg.Id, msg.ReceivedFrom)
		select {
		case <-ts.done:
			return
		case <-p.ctx.Done():
			return
		default:
			continue
		}
	}
}

func (p *PubsubNode) makeMessage(seq int64, size uint64) ([]byte, error) {

	data := make([]byte, size)
	rand.Read(data)

	m := &Msg{Sender: p.h.ID().String(), Seq: seq, Data: data}

	return json.Marshal(m)
}

func (p *PubsubNode) sendMsg(seq int64, ts *topicState) {
	p.runenv.RecordMessage("Publishing message %d %d %s bytes", seq, uint64(ts.cfg.MessageSize), p.h.ID().Loggable())

	msg, err := p.makeMessage(seq, uint64(ts.cfg.MessageSize))

	//p.log("makeMessage %d", len(msg))

	if err != nil {
		p.log("error making message for topic %s: %s", ts.cfg.Id, err)
		return
	}

	err = ts.topic.Publish(p.ctx, msg)
	if err != nil && err != context.Canceled {
		p.log("error publishing to %s: %s", ts.cfg.Id, err)
		return
	}
}

func (p *PubsubNode) publishLoop(ts *topicState) {
	var counter int64
	p.pubwg.Add(1)
	defer p.pubwg.Done()
	for {
		select {
		case <-ts.done:
			return
		case <-p.ctx.Done():
			p.runenv.RecordMessage("Publish loop done")
			return
		case <-ts.pubTicker.C:
			for id := range p.ps.ListPeers(ts.sub.Topic()) {
				p.runenv.RecordMessage("Connected to %d", id)
			}
			go p.sendMsg(counter, ts)
			counter++
			if counter > ts.nMessages {
				ts.pubTicker.Stop()
				return
			}
		}
	}
}

func (p *PubsubNode) log(msg string, args ...interface{}) {
	id := p.h.ID().String()
	idSuffix := id[len(id)-8:]
	prefix := fmt.Sprintf("[node %d %s] ", p.seq, idSuffix)
	p.runenv.RecordMessage(prefix+msg, args...)
}
