package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/artisoft-io/jetstore/jets/awsi"
	"github.com/artisoft-io/jetstore/jets/compute_pipes/actions"
)

// Booter utility to execute cpipes (loader) in loop for each jets_partition
// Command line arguments compatible with loader/server (cpipes)

// Env variables:
// JETS_BUCKET
// JETS_DSN_SECRET
// JETS_REGION
// JETS_s3_INPUT_PREFIX
// JETS_s3_OUTPUT_PREFIX
// NBR_SHARDS default nbr_nodes of cluster
// USING_SSH_TUNNEL Connect  to DB using ssh tunnel (expecting the ssh open)
var pipelineExecKey = flag.Int("pe_key", -1, "Pipeline execution key (required)")
var fileKey = flag.String("file_key", "", "the input file_key (required)")
var sessionId = flag.String("session_id", "", "Pipeline session ID (required)")

var awsDsnSecret string
var dbPoolSize int
var usingSshTunnel bool
var awsRegion string
var awsBucket string
var dsn string
var nbrNodes int

func main() {
	fmt.Println("LOCAL TEST DRIVER CMD LINE ARGS:", os.Args[1:])
	flag.Parse()
	hasErr := false
	var errMsg []string
	var err error
	dbPoolSize = 20
	awsDsnSecret = os.Getenv("JETS_DSN_SECRET")
	if awsDsnSecret == "" {
		hasErr = true
		errMsg = append(errMsg, "Connection string must be provided using env JETS_DSN_SECRET")
	}
	awsRegion = os.Getenv("JETS_REGION")
	if awsRegion == "" {
		hasErr = true
		errMsg = append(errMsg, "aws region must be provided using env JETS_REGION")
	}
	awsBucket = os.Getenv("JETS_BUCKET")
	if awsBucket == "" {
		hasErr = true
		errMsg = append(errMsg, "Bucket must be provided using env var JETS_BUCKET")
	}
	if os.Getenv("JETS_s3_INPUT_PREFIX") == "" {
		hasErr = true
		errMsg = append(errMsg, "env var JETS_s3_INPUT_PREFIX must be provided")
	}
	if os.Getenv("JETS_s3_OUTPUT_PREFIX") == "" {
		hasErr = true
		errMsg = append(errMsg, "env var JETS_s3_OUTPUT_PREFIX must be provided")
	}

	v := os.Getenv("NBR_SHARDS")
	if v == "" {
		hasErr = true
		errMsg = append(errMsg, "env NBR_SHARDS not set")
	} else {
		nbrNodes, err = strconv.Atoi(v)
		if err != nil {
			hasErr = true
			errMsg = append(errMsg, "env NBR_SHARDS not a valid integer")
		}
	}
	_, usingSshTunnel = os.LookupEnv("USING_SSH_TUNNEL")
	if !usingSshTunnel {
		hasErr = true
		errMsg = append(errMsg, "env USING_SSH_TUNNEL expected to be set for local testing")
	}

	// Get the dsn from the aws secret
	dsn, err = awsi.GetDsnFromSecret(awsDsnSecret, usingSshTunnel, dbPoolSize)
	if err != nil {
		err = fmt.Errorf("while getting dsn from aws secret: %v", err)
		fmt.Println(err)
		hasErr = true
		errMsg = append(errMsg, err.Error())
	}

	if hasErr {
		for _, msg := range errMsg {
			fmt.Println("**", msg)
		}
		panic("Invalid argument(s)")
	}

	log.Println("CP Starter:")
	log.Println("-----------")
	log.Println("Got argument: awsBucket", awsBucket)
	log.Println("Got argument: awsDsnSecret", awsDsnSecret)
	log.Println("Got argument: dbPoolSize", dbPoolSize)
	log.Println("Got argument: awsRegion", awsRegion)
	log.Println("Got argument: nbrNodes (default)", nbrNodes)
	var b []byte

	// Start Sharding
	shardingArgs := &actions.StartComputePipesArgs{
		PipelineExecKey: *pipelineExecKey,
		FileKey:         *fileKey,
		SessionId:       *sessionId,
	}
	ctx := context.Background()
	fmt.Println("Start Sharding Arguments")
	b, _ = json.MarshalIndent(shardingArgs, "", " ")
	fmt.Println(string(b))
	cpShardingRun, err := shardingArgs.StartShardingComputePipes(ctx, dsn, nbrNodes)
	if err != nil {
		log.Fatalf("while calling StartShardingComputePipes: %v", err)
	}
	fmt.Println("Sharding Map Arguments")
	b, _ = json.MarshalIndent(cpShardingRun, "", " ")
	fmt.Println(string(b))

	// Perform Sharding
	for i := range cpShardingRun.CpipesCommands {
		fmt.Println("## Sharding Node", i)
		err = cpShardingRun.CpipesCommands[i].CoordinateComputePipes(ctx, dsn)
		if err != nil {
			log.Fatalf("while sharding node %d: %v", i, err)
		}
	}

	// Start Reducing
	cpReducingRun, err := cpShardingRun.StartReducing.StartReducingComputePipes(ctx, dsn, nbrNodes)
	if err != nil {
		log.Fatalf("while calling StartReducingComputePipes: %v", err)
	}
	fmt.Println("Reducing Map Arguments")
	b, _ = json.MarshalIndent(cpReducingRun, "", " ")
	fmt.Println(string(b))

	// Perform Reducing
	for i := range cpReducingRun.CpipesCommands {
		fmt.Println("## Reducing Node", i)
		err = cpReducingRun.CpipesCommands[i].CoordinateComputePipes(ctx, dsn)
		if err != nil {
			log.Fatalf("while reducing node %d: %v", i, err)
		}
	}
	log.Println("That's it folks!")
}