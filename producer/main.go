package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Risk control needs bet_amount, net_win in the same message to compute per-round net and RTP drift without joining across topics
type SlotRound struct {
	RoundID       string  `json:"round_id"`
	PlayerID      int64   `json:"player_id"`
	AgentID       int64   `json:"agent_id"`
	GameCode      string  `json:"game_code"`
	BetAmount     float64 `json:"bet_amount"`
	NetWin        float64 `json:"net_win"`
	Currency      string  `json:"currency"`
	BalanceBefore float64 `json:"balance_before"`
	BalanceAfter  float64 `json:"balance_after"`
	IsFreeSpin    bool    `json:"is_free_spin"`
	JackpotWin    float64 `json:"jackpot_win"`
	RoundTime     string  `json:"round_time"`
}

// envelope wraps each payload with an explicit Connect schema, since there's no
// Schema Registry here & the JDBC sink connector requires a typed Struct
// (not a schemaless map) to build column names/types for the SQL statement.
type envelope struct {
	Schema  json.RawMessage `json:"schema"`
	Payload SlotRound       `json:"payload"`
}

var slotRoundSchema = json.RawMessage(`{"type":"struct","name":"SlotRound","optional":false,"fields":[` +
	`{"field":"round_id","type":"string","optional":false},` +
	`{"field":"player_id","type":"int64","optional":false},` +
	`{"field":"agent_id","type":"int64","optional":false},` +
	`{"field":"game_code","type":"string","optional":false},` +
	`{"field":"bet_amount","type":"double","optional":false},` +
	`{"field":"net_win","type":"double","optional":false},` +
	`{"field":"currency","type":"string","optional":false},` +
	`{"field":"balance_before","type":"double","optional":false},` +
	`{"field":"balance_after","type":"double","optional":false},` +
	`{"field":"is_free_spin","type":"boolean","optional":false},` +
	`{"field":"jackpot_win","type":"double","optional":false},` +
	`{"field":"round_time","type":"string","optional":false}]}`)

var gameCodes = []string{"SLOT_FORTUNE_TIGER", "SLOT_GOLDEN_DRAGON", "SLOT_FRUIT_PARTY", "SLOT_MEGA_WHEEL"}

// spin simulates a settled round at roughly a 96% RTP: most spins lose, most
// wins are small multiples of the stake, and jackpots are rare but large.
func spin(rnd *rand.Rand, bet float64) (win, jackpot float64, freeSpin bool) {
	roll := rnd.Float64()
	switch {
	case roll < 0.65:
		return 0, 0, false
	case roll < 0.90:
		return bet * (0.2 + rnd.Float64()*1.8), 0, false
	case roll < 0.97:
		return bet * (1 + rnd.Float64()*3), 0, true
	case roll < 0.999:
		return bet * (5 + rnd.Float64()*45), 0, false
	default:
		j := 500 + rnd.Float64()*9500
		return j, j, false
	}
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "slot_rounds", "Kafka topic")
	rate := flag.Int("rate", 20000, "Target messages/sec")
	duration := flag.Duration("duration", 60*time.Second, "Test duration")
	workers := flag.Int("workers", 32, "Concurrent producer goroutines")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		// 500k messages to be buffered in memory before being sent to Kafka
		kgo.MaxBufferedRecords(500000),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var sent, acked, failed int64

	// Token-bucket rate limiter: refill in small batches every 5ms instead of
	// ticking once per message (a 50us ticker for 20k/s is wasteful and jittery).
	tokens := make(chan struct{}, *rate/10+1000)
	fillCtx, cancelFill := context.WithCancel(ctx)
	defer cancelFill()
	go func() {
		const tick = 5 * time.Millisecond
		perTick := int(float64(*rate) * tick.Seconds())
		if perTick < 1 {
			perTick = 1
		}
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-fillCtx.Done():
				return
			case <-t.C:
				for i := 0; i < perTick; i++ {
					select {
					case tokens <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					if time.Now().After(deadline) {
						return
					}
					// Simulate a round of slot play
					// build a SlotRound struct with random values
					bet := 1 + rnd.Float64()*99
					balanceBefore := 100 + rnd.Float64()*9900
					win, jackpot, freeSpin := spin(rnd, bet)

					r := SlotRound{
						RoundID:       fmt.Sprintf("R-%d-%d", id, rnd.Int63()),
						PlayerID:      rnd.Int63n(1000000),
						AgentID:       rnd.Int63n(500) + 1, // far fewer agents than players
						GameCode:      gameCodes[rnd.Intn(len(gameCodes))],
						BetAmount:     bet,
						NetWin:        win,
						Currency:      "CNY",
						BalanceBefore: balanceBefore,
						BalanceAfter:  balanceBefore - bet + win,
						IsFreeSpin:    freeSpin,
						JackpotWin:    jackpot,
						RoundTime:     time.Now().UTC().Format("2006-01-02 15:04:05"),
					}

					value, _ := json.Marshal(envelope{Schema: slotRoundSchema, Payload: r})
					record := &kgo.Record{Topic: *topic, Key: []byte(r.RoundID), Value: value}
					atomic.AddInt64(&sent, 1)

					// Async produce: enqueue and return immediately, record result in callback.
					// This is what lets one client sustain tens of thousands of msgs/sec.
					client.Produce(ctx, record, func(_ *kgo.Record, err error) {
						if err != nil {
							atomic.AddInt64(&failed, 1)
							fmt.Println("produce error:", err)
							return
						}
						atomic.AddInt64(&acked, 1)
					})
				}
			}
		}(w)
	}

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastAcked int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a := atomic.LoadInt64(&acked)
				fmt.Printf("sent=%d acked=%d failed=%d qps=%d\n",
					atomic.LoadInt64(&sent), a, atomic.LoadInt64(&failed), a-lastAcked)
				lastAcked = a
				if time.Now().After(deadline) {
					return
				}
			}
		}
	}()

	wg.Wait()
	cancelFill()
	client.Flush(context.Background())
	<-statsDone

	fmt.Printf("done: sent=%d acked=%d failed=%d\n",
		atomic.LoadInt64(&sent), atomic.LoadInt64(&acked), atomic.LoadInt64(&failed))
}
