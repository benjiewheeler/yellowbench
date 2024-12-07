package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/benjiewheeler/yellowbench/proto"
	"github.com/briandowns/spinner"
	"github.com/charmbracelet/log"
	"github.com/dustin/go-humanize"
	"github.com/montanaflynn/stats"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var Version string = "development"

var (
	DEFAULT_CONFIG = Config{
		GeyserUrl:   "http://node.foo.cc",
		GeyserToken: "",
		Duration:    60,
	}

	// variable for the log file; set to benchmark.log as a fallback
	LogFileName string = "benchmark.log"

	// the global config
	GlobalConfig *Config

	// the simplified logger
	simpleLogger *log.Logger
	fileLogger   *log.Logger

	// wait group
	wg sync.WaitGroup

	// start/end times
	benchStart time.Time
	benchEnd   time.Time

	// the grpc listener
	grpcListener *GeyserListener

	// delta between block time and receive time
	blockDeltas = []time.Duration{}

	// number of blocks received
	receivedBlocks uint64

	// total size of received data
	receivedSize uint64
)

type GeyserListener struct {
	client pb.GeyserClient

	ctx    context.Context
	cancel context.CancelFunc
}

func (l *GeyserListener) prepareClient() pb.GeyserClient {
	// parse the url
	u, err := url.Parse(GlobalConfig.GeyserUrl)
	if err != nil {
		log.Fatalf("Invalid GRPC address provided: %v", err)
	}

	// define the port if not provided
	port := u.Port()
	if port == "" {
		if u.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	}

	hostname := u.Hostname()
	address := hostname + ":" + port

	var opts []grpc.DialOption

	if u.Scheme == "http" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	opts = append(
		opts, grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             2 * time.Second,
				PermitWithoutStream: true,
			},
		),
	)

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024))) // 100MB limit on receiving
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(100*1024*1024))) // 100MB limit on sending

	// initialize the gRPC client
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		log.Fatalf("Error establishing connection: %v", err)
	}

	return pb.NewGeyserClient(conn)
}

func (l *GeyserListener) Start() {
	defer wg.Done()
	s := spinner.New(spinner.CharSets[9], 100*time.Millisecond)
	s.Start()
	defer s.Stop()

	// prepare the context
	l.ctx, l.cancel = context.WithTimeout(context.Background(), time.Duration(GlobalConfig.Duration)*time.Second)
	defer l.Stop()

	// prepare the client
	client := l.prepareClient()

	// add the geyser token, if any
	if GlobalConfig.GeyserToken != "" {
		md := metadata.New(map[string]string{"x-token": GlobalConfig.GeyserToken})
		l.ctx = metadata.NewOutgoingContext(l.ctx, md)
	}

	streamClient, err := client.Subscribe(l.ctx)
	if err != nil {
		log.Fatalf("Error making stream client: %v", err)
	}

	// make the subscription request
	subscription := pb.SubscribeRequest{
		Commitment: pb.CommitmentLevel_PROCESSED.Enum(),
	}

	switch GlobalConfig.Type {
	case TypeLatency:
		subscription.BlocksMeta = map[string]*pb.SubscribeRequestFilterBlocksMeta{"blocks_meta": {}}

	case TypeThroughput:
		subscription.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{"accounts": {}}
		subscription.Slots = map[string]*pb.SubscribeRequestFilterSlots{"slots": {}}
		subscription.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{"transactions": {}}
		subscription.TransactionsStatus = map[string]*pb.SubscribeRequestFilterTransactions{"transactions_status": {}}
		subscription.Blocks = map[string]*pb.SubscribeRequestFilterBlocks{"blocks": {}}
		subscription.Entry = map[string]*pb.SubscribeRequestFilterEntry{"entry": {}}

	}

	if err := streamClient.Send(&subscription); err != nil {
		log.Fatalf("Error subscribing to blocks meta: %v", err)
	}

	// set the times for the start and end
	benchStart = time.Now()
	defer func() { benchEnd = time.Now() }()

	s.Suffix = " Waiting for updates..."

	for {
		select {
		case <-l.ctx.Done():
			return

		default:
			out, err := streamClient.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				if err := l.ctx.Err(); err != nil {
					return
				}

				log.Fatalf("Error occurred in receiving update: %v", err)
			}

			if GlobalConfig.Type == TypeThroughput {
				// increment the received size
				receivedSize += uint64(proto.Size(out))

				s.Suffix = fmt.Sprintf(
					" Data received size=%s elapsed=%s",
					humanize.Bytes(receivedSize),
					time.Since(benchStart).Truncate(time.Millisecond),
				)
			}

			if meta := out.GetBlockMeta(); meta != nil {
				blockTime := time.Unix(meta.BlockTime.Timestamp, 0).UTC()
				delta := time.Since(blockTime)

				blockDeltas = append(blockDeltas, delta)
				receivedBlocks++

				s.Suffix = fmt.Sprintf(
					" Received update slot=%d blocktime=%s latency=%s",
					meta.Slot,
					blockTime.Format(time.TimeOnly),
					delta.Truncate(time.Millisecond).String(),
				)

				fileLogger.Info(
					"Received update",
					"slot", meta.Slot,
					"blocktime", blockTime.Format(time.TimeOnly),
					"latency", delta.Truncate(time.Millisecond).String(),
				)
			}
		}
	}
}

func (l *GeyserListener) Stop() {
	if l.ctx.Err() == nil {
		log.Info("Stopping geyser listener...")
		l.cancel()
	}
}

func SetupLogger() {
	LogFileName = fmt.Sprintf("yellowbench_%d.log", time.Now().UnixMilli())
	logFile, err := os.OpenFile(LogFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	multi := io.MultiWriter(os.Stdout, logFile)

	// create a simplified logger for logging the test results
	simpleLogger = log.NewWithOptions(multi, log.Options{
		ReportTimestamp: false,
	})

	// create a logger that writes to a file only
	fileLogger = log.NewWithOptions(logFile, log.Options{
		ReportTimestamp: true,
		TimeFunction:    func(time.Time) time.Time { return time.Now().UTC() },
		TimeFormat:      "15:04:05.0000",
	})

	// create a logger that writes to the terminal and a file
	log.SetDefault(log.NewWithOptions(multi, log.Options{
		ReportTimestamp: true,
		TimeFunction:    func(time.Time) time.Time { return time.Now().UTC() },
		TimeFormat:      "15:04:05.0000",
	}))
}

func ReadConfig() *Config {
	data, err := os.ReadFile("config.json")
	if err != nil {
		// if the error is that the file doesn't exist, create it, and exit
		if os.IsNotExist(err) {
			if err := WriteConfig(&DEFAULT_CONFIG); err != nil {
				log.Fatalf("error creating config file: %v", err)
			}

			log.Info("config file saved, edit the config and restart")
			os.Exit(0)
		}

		log.Fatalf("error opening config file: %v", err)
	}

	var out Config

	err = json.Unmarshal(data, &out)
	if err != nil {
		log.Fatalf("error parsing config file: %v", err)
	}

	return &out
}

func WriteConfig(config *Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("error saving config file: %v", err)
	}

	return os.WriteFile("config.json", data, 0644)
}

func main() {
	fmt.Println()
	fmt.Println(" ██╗   ██╗███████╗██╗     ██╗      ██████╗ ██╗    ██╗██████╗ ███████╗███╗   ██╗ ██████╗██╗  ██╗ ")
	fmt.Println(" ╚██╗ ██╔╝██╔════╝██║     ██║     ██╔═══██╗██║    ██║██╔══██╗██╔════╝████╗  ██║██╔════╝██║  ██║ ")
	fmt.Println("  ╚████╔╝ █████╗  ██║     ██║     ██║   ██║██║ █╗ ██║██████╔╝█████╗  ██╔██╗ ██║██║     ███████║ ")
	fmt.Println("   ╚██╔╝  ██╔══╝  ██║     ██║     ██║   ██║██║███╗██║██╔══██╗██╔══╝  ██║╚██╗██║██║     ██╔══██║ ")
	fmt.Println("    ██║   ███████╗███████╗███████╗╚██████╔╝╚███╔███╔╝██████╔╝███████╗██║ ╚████║╚██████╗██║  ██║ ")
	fmt.Println("    ╚═╝   ╚══════╝╚══════╝╚══════╝ ╚═════╝  ╚══╝╚══╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝ ")
	fmt.Printf("%95s", Version)
	fmt.Println()
	fmt.Println()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c

		fmt.Println()
		log.Info("CTRL+C detected, Force stopping the test")
		fmt.Println()

		grpcListener.Stop()
	}()

	// set up logger
	SetupLogger()

	// read the config file
	GlobalConfig = ReadConfig()

	// validate the config
	if err := GlobalConfig.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	simpleLogger.Printf("Date         : %s", time.Now().UTC().Format(time.RFC1123))
	simpleLogger.Printf("Geyser URL   : %s", GlobalConfig.GeyserUrl)
	simpleLogger.Printf("Test Timeout : %s", time.Duration(GlobalConfig.Duration)*time.Second)
	simpleLogger.Printf("Bench Type   : %s", GlobalConfig.Type)
	simpleLogger.Printf("")

	// start the websocket listener
	wg.Add(1)
	grpcListener = new(GeyserListener)
	go grpcListener.Start()
	wg.Wait()

	benchDuration := benchEnd.Sub(benchStart)

	simpleLogger.Printf("")
	simpleLogger.Printf("Geyser URL           : %s", GlobalConfig.GeyserUrl)
	simpleLogger.Printf("Test Duration        : %s", benchDuration.Truncate(time.Millisecond))

	switch GlobalConfig.Type {
	case TypeLatency:
		simpleLogger.Printf("Received Blocks      : %s", message.NewPrinter(language.English).Sprintf("%d", receivedBlocks))

		// calculate landing time results, if there was any
		if len(blockDeltas) > 0 {
			var latencies []float64
			for _, v := range blockDeltas {
				latencies = append(latencies, float64(v.Nanoseconds()))
			}

			minDelta, _ := stats.Min(latencies)
			maxDelta, _ := stats.Max(latencies)
			avg, _ := stats.Mean(latencies)
			median, _ := stats.Median(latencies)
			p90, _ := stats.Percentile(latencies, 90)
			p95, _ := stats.Percentile(latencies, 95)
			p99, _ := stats.Percentile(latencies, 99)

			simpleLogger.Printf("Min Block Latency    : %s", (time.Duration(minDelta)).Truncate(time.Millisecond))
			simpleLogger.Printf("Max Block Latency    : %s", (time.Duration(maxDelta)).Truncate(time.Millisecond))
			simpleLogger.Printf("Avg Block Latency    : %s", (time.Duration(avg)).Truncate(time.Millisecond))
			simpleLogger.Printf("Median Block Latency : %s", (time.Duration(median)).Truncate(time.Millisecond))
			simpleLogger.Printf("P90 Block Latency    : %s", (time.Duration(p90)).Truncate(time.Millisecond))
			simpleLogger.Printf("P95 Block Latency    : %s", (time.Duration(p95)).Truncate(time.Millisecond))
			simpleLogger.Printf("P99 Block Latency    : %s", (time.Duration(p99)).Truncate(time.Millisecond))
			simpleLogger.Printf("")
		}

	case TypeThroughput:
		simpleLogger.Printf("Received Data Size   : %s", humanize.Bytes(receivedSize))
		simpleLogger.Printf("Data Rate            : %s/s", humanize.Bytes(uint64(float64(receivedSize)/benchDuration.Seconds())))
	}

	fmt.Println()
	fmt.Printf("Benchmark results saved to %s\n", LogFileName)

	fmt.Println("Press 'Enter' to exit...")
	fmt.Scanln()
}
