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
	"github.com/testground/sdk-go/ptypes"
	"github.com/testground/sdk-go/runtime"
)

type NodeConfig struct {
	// topics to join when node starts
	Topics []TopicConfig

	// whether we're a publisher or a lurker
	Publisher bool

	FloodPublishing bool

	// pubsub event tracer
	//Tracer pubsub.EventTracer

	// Test instance identifier
	Seq int64

	//How long to wait after connecting to bootstrap peers before publishing
	Warmup time.Duration

	// How long to wait for cooldown
	Cooldown time.Duration

	// Gossipsub heartbeat params
	/*Heartbeat HeartbeatParams

	// whether to flood the network when publishing our own messages.
	// Ignored unless hardening_api build tag is present.
	FloodPublishing bool

	// Params for peer scoring function. Ignored unless hardening_api build tag is present.
	PeerScoreParams ScoreParams

	OverlayParams OverlayParams

	// Params for inspecting the scoring values.
	PeerScoreInspect InspectParams

	// Size of the pubsub validation queue.
	ValidateQueueSize int

	// Size of the pubsub outbound queue.
	OutboundQueueSize int

	// Heartbeat tics for opportunistic grafting
	OpportunisticGraftTicks int*/
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

	pubwg sync.WaitGroup
}

func createNode(ctx context.Context, runenv *runtime.RunEnv, seq int64, h host.Host, discovery *SyncDiscovery, cfg NodeConfig) (*PubsubNode, error) {

	ps, err := pubsub.NewGossipSub(ctx, h)

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
	}
	/*err = p.Run(t.params.runtime, func(ctx context.Context) error {
		// wait for all other nodes to be ready
		if err := t.waitForReadyState(ctx); err != nil {
			return err
		}

		// connect topology async
		go t.connectTopology(ctx)

		return nil
	})*/
	//if seq == 1 {
	for i, peer := range discovery.allPeers {
		runenv.RecordMessage("Connecting to %d %s ", i, peer.Info.Addrs)
		err := discovery.connectWithRetry(ctx, peer.Info)
		//		err := h.Connect(ctx, peer.Info)
		if err != nil {
			fmt.Printf("error connecting to peer %s: %s\n", peer.Info.ID, err)
		} else {
			runenv.RecordMessage("Connection succesful to ", peer.Info.Addrs)
		}

	}

	//}

	return p, nil
}

func (p *PubsubNode) Run(runtime time.Duration) error {
	defer func() {
		// end subscription goroutines before exit
		for _, ts := range p.topics {
			ts.done <- struct{}{}
		}

		p.shutdown()
	}()

	// Wait for all nodes to be in the ready state (including attack nodes)
	// then start connecting (asynchronously)
	/*if err := waitForReadyStateThenConnectAsync(p.ctx); err != nil {
		return err
	}*/

	// join initial topics
	p.runenv.RecordMessage("Joining initial topics %d.", len(p.cfg.Topics))
	for _, t := range p.cfg.Topics {
		p.runenv.RecordMessage("Joining topic %s %d.", t.Id, t.MessageSize)
		go p.joinTopic(t, runtime)
	}

	/* wait for warmup time to expire
	p.runenv.RecordMessage("Wait for %s warmup time", p.cfg.Warmup)
	select {
	case <-time.After(p.cfg.Warmup):
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	// ensure we have at least enough peers to fill a mesh after warmup period
	npeers := len(p.h.Network().Peers())
	if npeers < pubsub.GossipSubD {
		panic(fmt.Errorf("not enough peers after warmup period. Need at least D=%d, have %d", pubsub.GossipSubD, npeers))
	}*/

	p.runenv.RecordMessage("Starting gossipsub. Connected to %d peers.", len(p.h.Network().Peers()))
	// block until complete
	p.runenv.RecordMessage("Wait for %s run time", runtime)
	select {
	case <-time.After(runtime):
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	// if we're publishing, wait until we've sent all our messages or the context expires
	/*if p.cfg.Publisher {
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

	p.runenv.RecordMessage("Cool down complete")*/

	return nil
}

func (p *PubsubNode) joinTopic(t TopicConfig, runtime time.Duration) {
	p.lk.Lock()
	defer p.lk.Unlock()

	publishInterval := time.Duration(float64(t.MessageRate.Interval) / t.MessageRate.Quantity)
	totalMessages := int64(runtime / publishInterval)

	/*if p.cfg.Publisher {
		p.log("publishing to topic %s. message_rate: %.2f/%ds, publishInterval %dms, msg size %d bytes. total expected messages: %d",
			t.Id, t.MessageRate.Quantity, t.MessageRate.Interval/time.Second, publishInterval/time.Millisecond, t.MessageSize, totalMessages)
	} else {
		p.log("joining topic %s as a lurker", t.Id)
	}*/

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

	if !p.cfg.Publisher {
		return
	}

	go func() {
		p.runenv.RecordMessage("Wait for %s warmup time before starting publisher", p.cfg.Warmup)
		select {
		case <-time.After(p.cfg.Warmup):
		case <-p.ctx.Done():
			p.runenv.RecordMessage("Context done before warm up time in publisher: %s", p.ctx.Err())
			return
		}

		p.runenv.RecordMessage("Starting publisher with %s publish interval", publishInterval)
		ts.pubTicker = time.NewTicker(publishInterval)
		p.publishLoop(&ts)
	}()
}

func (p *PubsubNode) consumeTopic(ts *topicState) {
	for {
		msg, err := ts.sub.Next(p.ctx)
		if err != nil && err != context.Canceled {
			p.log("error reading from %s: %s", ts.cfg.Id, err)
			return
		}
		//p.log("got message")
		p.log("got message on topic %s from %s\n", ts.cfg.Id, msg.ReceivedFrom)

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
	type msg struct {
		sender string
		seq    int64
		data   []byte
	}
	data := make([]byte, size)
	rand.Read(data)
	m := msg{sender: p.h.ID().String(), seq: seq, data: data}
	return json.Marshal(m)
}

func (p *PubsubNode) sendMsg(seq int64, ts *topicState) {
	p.runenv.RecordMessage("Publishing message %d bytes", uint64(ts.cfg.MessageSize))

	msg, err := p.makeMessage(seq, uint64(ts.cfg.MessageSize))
	if err != nil {
		p.log("error making message for topic %s: %s", ts.cfg.Id, err)
		return
	}
	var data []byte
	err = json.Unmarshal(data, msg)
	//p.runenv.RecordMessage("Publishing message %d bytes", len(data))
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
			return
		case <-ts.pubTicker.C:
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
	prefix := fmt.Sprintf("[honest %d %s] ", p.seq, idSuffix)
	p.runenv.RecordMessage(prefix+msg, args...)
}
