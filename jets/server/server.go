package main

import (
	// "bufio"
	"context"
	// "encoding/csv"
	"flag"
	"fmt"

	// "io"
	"log"
	"os"

	// "path/filepath"
	// "strings"

	// "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Command Line Arguments
var dsn = flag.String("dsn", "", "database connection string (required)")
var workspaceDb = flag.String("workspace_db", "", "workspace db path (required)")
var lookupDb = flag.String("lookup_db", "", "lookup data path")
var procConfigKey = flag.Int("pcKey", 0, "Process config key (required)")
var poolSize = flag.Int("poolSize", 10, "Pool size constraint")
var sessionId = flag.String("sessId", "", "Process session ID used to link entitied processed together.")

// doJob main function
func doJob() error {

	// open db connection
	dbpool, err := pgxpool.Connect(context.Background(), *dsn)
	if err != nil {
		return fmt.Errorf("while opening db connection: %v", err)
	}
	defer dbpool.Close()

	var procConfig ProcessConfig

	err = procConfig.read(dbpool, *procConfigKey)
	if err != nil {
		return fmt.Errorf("while reading process_config table: %v", err)
	}
	
	//*
	fmt.Println("Got ProcessConfig row:")
	fmt.Println("  key:", procConfig.key, "client", procConfig.client, "description", procConfig.description, "Main Type", procConfig.mainEntityRdfType)
	fmt.Println("Got ProcessInput row:")
	for _, pi := range procConfig.processInputs {
		//*
		fmt.Println("  key:", pi.key, ", processKey", pi.processKey, ", InputTable", pi.inputTable, ", rdf Type", pi.entityRdfType, ", Grouping Column", pi.groupingColumn)
		for _, pm := range pi.processInputMapping {
			fmt.Println("    InputMapping - key", pm.processInputKey, ", inputColumn:", pm.inputColumn, ", dataProperty:", pm.dataProperty, ", function:", pm.functionName.String, ", arg:", pm.argument.String, ", default:", pm.defaultValue.String)
		}
	}
	fmt.Println("Got RuleConfig rows:")
	for _, rc := range procConfig.ruleConfigs {
		fmt.Println("    procKey:", rc.processKey, ", subject", rc.subject, ", predicate", rc.predicate, ", object", rc.object)
	}
	//*

	// validation
	if len(procConfig.processInputs) != 1 {
		return fmt.Errorf("while reading ProcessInput table, currently we're supporting a single input table")
	}
	if procConfig.mainEntityRdfType != procConfig.processInputs[0].entityRdfType {
		return fmt.Errorf("while reading ProcessInput table, mainEntityRdfType must match the ProcessInput entityRdfType")
	}

	return nil
}

func main() {
	flag.Parse()
	hasErr := false
	var errMsg []string
	if *procConfigKey == 0 {
		hasErr = true
		errMsg = append(errMsg, "Process config key value (-pcKey) must be provided.")
	}
	if *dsn == "" {
		hasErr = true
		errMsg = append(errMsg, "Connection string must be provided.")
	}
	if *workspaceDb == "" {
		hasErr = true
		errMsg = append(errMsg, "Workspace db path must be provided.")
	}
	if hasErr {
		flag.Usage()
		for _, msg := range errMsg {
			fmt.Println("**", msg)
		}
		os.Exit((1))
	}
	fmt.Printf("Got procConfigKey: %d\n", *procConfigKey)
	fmt.Printf("Got poolSize: %d\n", *poolSize)
	fmt.Printf("Got sessionId: %s\n", *sessionId)
	fmt.Printf("Got workspaceDb: %s\n", *workspaceDb)
	fmt.Printf("Got lookupDb: %s\n", *lookupDb)

	err := doJob()
	if err != nil {
		flag.Usage()
		log.Fatal(err)
	}
}