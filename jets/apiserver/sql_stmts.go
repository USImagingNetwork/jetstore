package main

// This file contains static definition of sql statements for request from jets client

// Simple definition of sql statement for insert
type sqlInsertDefinition struct {
	stmt string
	columnKeys []string
}
// Note column keys are keys provided from the UI and may not
// correspond to column name.
// Important: columnKeys order MUST match order in stmt
var sqlInsertStmts = map[string]sqlInsertDefinition {
	// client registry
	"client_registry": {
		stmt: `INSERT INTO jetsapi.client_registry (client, details) VALUES ($1, $2)`,
		columnKeys: []string{"client", "details"},
	},
	// object type registry
	"object_type_registry": {
		stmt: `INSERT INTO jetsapi.object_type_registry (object_type, details) VALUES ($1, $2)`,
		columnKeys: []string{"object_type", "details"},
	},
	// source config
	"source_config": {
		stmt: `INSERT INTO jetsapi.source_config 
			(object_type, client, table_name, grouping_column, user_email) 
			VALUES ($1, $2, $3, $4, $5)`,
		columnKeys: []string{"object_type", "client", "table_name", "grouping_column", "user_email"},
	},
	// input loader status
	"input_loader_status": {
		stmt: `INSERT INTO jetsapi.input_loader_status 
			(object_type, client, table_name, file_key, session_id, status, user_email) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		columnKeys: []string{"object_type", "client", "table_name", "file_key", "session_id", "status", "user_email"},
	},
	// process input
	"process_input": {
		stmt: `INSERT INTO jetsapi.process_input 
			(client, object_type, table_name, source_type, entity_rdf_type, grouping_column, user_email) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		columnKeys: []string{"client", "object_type", "table_name", "source_type", "entity_rdf_type", "grouping_column", "user_email"},
	},
	"update/process_input": {
		stmt: "UPDATE jetsapi.process_input SET (status, user_email, last_update) = ($1, $2, DEFAULT) WHERE key = $3",
		columnKeys: []string{"status", "user_email", "key"},
	},
	// process mapping
	"delete/process_mapping": {
		stmt: `DELETE FROM jetsapi.process_mapping 
			WHERE table_name = $1`,
		columnKeys: []string{"table_name"},
	},
	"process_mapping": {
		stmt: `INSERT INTO jetsapi.process_mapping 
			(table_name, input_column, data_property, function_name, argument, default_value, error_message, user_email) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		columnKeys: []string{"table_name", "input_column", "data_property", "function_name", "argument", "default_value", "error_message", "user_email"},
	},
	// Rule Config
	"delete/rule_config": {
		stmt: `DELETE FROM jetsapi.rule_config 
			WHERE (process_config_key, process_name, client) = 
			($1, $2, $3)`,
		columnKeys: []string{"process_config_key", "process_name", "client"},
	},
	"rule_config": {
		stmt: `INSERT INTO jetsapi.rule_config 
			(process_config_key, process_name, client, subject, predicate, object, rdf_type) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		columnKeys: []string{"process_config_key", "process_name", "client", "subject", "predicate", "object", "rdf_type"},
	},
	// pipeline config
	"update/pipeline_config": {
		stmt: `UPDATE jetsapi.pipeline_config SET 
			(process_name, client, process_config_key, main_process_input_key, merged_process_input_keys, main_table_name, description, user_email, last_update) = 
			($1, $2, $3, $4, $5, $6, $7, $8, DEFAULT) 
			WHERE key = $9`,
		columnKeys: []string{"process_name", "client", "process_config_key", "main_process_input_key", "merged_process_input_keys", "main_table_name", "description", "user_email", "key"},
	},
	"pipeline_config": {
		stmt: `INSERT INTO jetsapi.pipeline_config 
			(process_name, client, process_config_key, main_process_input_key, merged_process_input_keys, main_table_name, description, user_email) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		columnKeys: []string{"process_name", "client", "process_config_key", "main_process_input_key", "merged_process_input_keys", "main_table_name", "description", "user_email"},
	},
}