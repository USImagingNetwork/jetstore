import 'package:flutter/material.dart';
import 'package:jetsclient/components/jets_form_state.dart';
import 'package:jetsclient/modules/actions/config_delegates.dart';
import 'package:jetsclient/modules/actions/process_errors_delegates.dart';
import 'package:jetsclient/modules/actions/query_tool_screen_delegates.dart';
import 'package:jetsclient/modules/actions/source_config_delegates.dart';
import 'package:jetsclient/modules/actions/user_delegates.dart';
import 'package:jetsclient/modules/user_flows/client_registry/form_config.dart';
import 'package:jetsclient/modules/user_flows/configure_files/form_config.dart';
import 'package:jetsclient/modules/user_flows/file_mapping/form_config.dart';
import 'package:jetsclient/modules/user_flows/load_files/form_config.dart';
import 'package:jetsclient/modules/user_flows/pipeline_config/form_config.dart';
import 'package:jetsclient/modules/user_flows/start_pipeline/form_config.dart';
import 'package:jetsclient/modules/user_flows/workspace_pull/form_config.dart';

import 'package:jetsclient/utils/constants.dart';
import 'package:jetsclient/models/form_config.dart';
import 'package:jetsclient/modules/workspace_ide/form_config.dart';

final Map<String, FormConfig> _formConfigurations = {
  // Home Form (actionless)
  FormKeys.home: FormConfig(
    key: FormKeys.home,
    actions: [
      // Action-less form
    ],
    formTabsConfig: [
      FormTabConfig(
          label: 'File Loader Status',
          inputField: FormDataTableFieldConfig(
              key: DTKeys.inputLoaderStatusTable,
              dataTableConfig: DTKeys.inputLoaderStatusTable)),
      FormTabConfig(
          label: 'Pipeline Execution Status',
          inputField: FormDataTableFieldConfig(
              key: DTKeys.pipelineExecStatusTable,
              dataTableConfig: DTKeys.pipelineExecStatusTable)),
      FormTabConfig(
          label: 'Data Registry',
          inputField: FormDataTableFieldConfig(
              key: DTKeys.inputRegistryTable,
              dataTableConfig: DTKeys.inputRegistryTable)),
    ],
    formValidatorDelegate: homeFormValidator,
    formActionsDelegate: homeFormActions,
  ),

  // Login Form
  FormKeys.login: FormConfig(
    key: FormKeys.login,
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.login,
          label: "Sign in",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.register,
          label: "Register",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.userEmail,
            label: "Email",
            hint: "Your email address",
            autofocus: true,
            autofillHints: [AutofillHints.email],
            obscureText: false,
            textRestriction: TextRestriction.allLower,
            maxLength: 80,
            useDefaultFont: true)
      ],
      [
        FormInputFieldConfig(
            key: FSK.userPassword,
            label: "Password",
            hint: "Your password",
            autofocus: false,
            autofillHints: [AutofillHints.password],
            obscureText: true,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true)
      ],
    ],
    formValidatorDelegate: loginFormValidator,
    formActionsDelegate: loginFormActions,
  ),
  // User Registration Form
  FormKeys.register: FormConfig(
    key: FormKeys.register,
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.register,
          label: "Register",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.userName,
            label: "Name",
            hint: "Enter your name",
            flex: 1,
            autofocus: true,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
        FormInputFieldConfig(
            key: FSK.userEmail,
            label: "Email",
            hint: "Your email address",
            flex: 1,
            autofocus: false,
            autofillHints: [AutofillHints.email],
            obscureText: false,
            textRestriction: TextRestriction.allLower,
            maxLength: 80,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.userPassword,
            label: "Password",
            hint: "Your password",
            flex: 1,
            autofocus: false,
            autofillHints: [AutofillHints.newPassword],
            obscureText: true,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
        FormInputFieldConfig(
            key: FSK.userPasswordConfirm,
            label: "Password Confirmation",
            hint: "Re-enter your password",
            flex: 1,
            autofocus: false,
            autofillHints: [AutofillHints.newPassword],
            obscureText: true,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
      ],
    ],
    formValidatorDelegate: registrationFormValidator,
    formActionsDelegate: registrationFormActions,
  ),
  // User Git Profile Form
  FormKeys.userGitProfile: FormConfig(
    key: FormKeys.userGitProfile,
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.submitGitProfileOk,
          capability: "user_profile",
          label: "Submit",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.gitName,
            label: "Name",
            hint: "Enter your name for git commits",
            flex: 1,
            autofocus: true,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
        FormInputFieldConfig(
            key: FSK.gitHandle,
            label: "Git Handle",
            hint: "Your git handle (user name) for git commit",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.allLower,
            maxLength: 60,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.gitEmail,
            label: "Email",
            hint: "Your email address for git commit",
            flex: 1,
            autofocus: false,
            autofillHints: [AutofillHints.email],
            obscureText: false,
            textRestriction: TextRestriction.allLower,
            maxLength: 80,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.gitToken,
            label: "Github Token",
            hint: "Github token to use as password",
            flex: 1,
            autofocus: false,
            obscureText: true,
            textRestriction: TextRestriction.none,
            maxLength: 120,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.gitTokenConfirm,
            label: "Github Token Confirmation",
            hint: "Re-enter your github token",
            flex: 1,
            autofocus: false,
            obscureText: true,
            textRestriction: TextRestriction.none,
            maxLength: 120,
            useDefaultFont: true),
      ],
    ],
    formValidatorDelegate: gitProfileFormValidator,
    formActionsDelegate: gitProfileFormActions,
  ),
  // User Administration Form (actionless -- user table has the actions)
  FormKeys.userAdmin: FormConfig(
    key: FormKeys.userAdmin,
    actions: [],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.usersTable,
            dataTableConfig: DTKeys.usersTable,
            tableHeight: double.infinity)
      ],
    ],
    formValidatorDelegate: alwaysValidForm,
    formActionsDelegate: userAdminFormActions,
  ),
  // User Administration Form - Edit User Profile Dialog
  FormKeys.editUserProfile: FormConfig(
    key: FormKeys.editUserProfile,
    actions: [
      FormActionConfig(
          key: ActionKeys.editUserProfileOk,
          capability: "user_profile",
          label: "Submit",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    useListView: true,
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.userName,
            label: "Name",
            hint: "User name",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            isReadOnly: true,
            useDefaultFont: true,
            maxLength: 80),
        FormInputFieldConfig(
            key: FSK.userEmail,
            label: "Email",
            hint: "User email",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            isReadOnly: true,
            useDefaultFont: true,
            maxLength: 80),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.isActive,
            items: [
              DropdownItemConfig(label: 'Select User Status...'),
              DropdownItemConfig(label: 'Active', value: '1'),
              DropdownItemConfig(label: 'Inactive', value: '0'),
            ],
            flex: 1,
            defaultItemPos: 0),
      ],
      [
        PaddingConfig(height: defaultPadding * 4),
      ],
      [
        FormDataTableFieldConfig(
            key: DTKeys.userRolesTable, dataTableConfig: DTKeys.userRolesTable)
      ],
    ],
    formValidatorDelegate: userAdminValidator,
    formActionsDelegate: userAdminFormActions,
  ),

  // Client & Org Admin
  FormKeys.clientAdmin: FormConfig(
    key: FormKeys.clientAdmin,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.clientAdminTable,
            tableHeight: double.infinity,
            dataTableConfig: DTKeys.clientAdminTable),
        FormDataTableFieldConfig(
            key: DTKeys.orgNameTable,
            tableHeight: double.infinity,
            dataTableConfig: DTKeys.orgNameTable),
      ],
    ],
    // Using source config validator and actions since no widget here
    formValidatorDelegate: sourceConfigValidator,
    formActionsDelegate: sourceConfigActions,
  ),

  // Add Client Dialog
  FormKeys.addClient: FormConfig(
    key: FormKeys.addClient,
    title: "Add Client",
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.clientOk,
          capability: "client_config",
          label: "Insert",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.client,
            label: "Client Name",
            hint: "Client name as a short name",
            flex: 1,
            autofocus: true,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 20,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.details,
            label: "Details",
            hint: "Optional notes",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
      ],
    ],
    formValidatorDelegate: sourceConfigValidator,
    formActionsDelegate: sourceConfigActions,
  ),

  // Add Organization Dialog
  FormKeys.addOrg: FormConfig(
    key: FormKeys.addOrg,
    title: "Add Organization",
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.orgOk,
          capability: "client_config",
          label: "Insert",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.org,
            label: "Organization Name",
            hint: "Organization name as a short name",
            flex: 1,
            autofocus: true,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 20,
            useDefaultFont: true),
      ],
      [
        FormInputFieldConfig(
            key: FSK.details,
            label: "Details",
            hint: "Optional notes",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 80,
            useDefaultFont: true),
      ],
    ],
    formValidatorDelegate: sourceConfigValidator,
    formActionsDelegate: sourceConfigActions,
  ),

  // Source Config
  FormKeys.sourceConfig: FormConfig(
    key: FormKeys.sourceConfig,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.sourceConfigTable,
            // tableHeight: double.infinity,
            dataTableConfig: DTKeys.sourceConfigTable)
      ],
      [
        FormDataTableFieldConfig(
            key: DTKeys.fileKeyStagingTable,
            tableHeight: double.infinity,
            dataTableConfig: DTKeys.fileKeyStagingTable),
      ],
    ],
    formValidatorDelegate: sourceConfigValidator,
    formActionsDelegate: sourceConfigActions,
  ),

  // addSourceConfig - Dialog to add/update Source Config
  FormKeys.addSourceConfig: FormConfig(
    key: FormKeys.addSourceConfig,
    title: "Add/Update Source Config",
    actions: [
      FormActionConfig(
          key: ActionKeys.addSourceConfigOk,
          capability: "client_config",
          label: "Save",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDropdownFieldConfig(
            key: FSK.client,
            items: [
              DropdownItemConfig(label: 'Select a Client'),
            ],
            dropdownItemsQuery:
                "SELECT client FROM jetsapi.client_registry ORDER BY client ASC LIMIT 200"),
        FormDropdownFieldConfig(
            key: FSK.org,
            items: [
              DropdownItemConfig(label: 'Select an Organization'),
              DropdownItemConfig(label: 'No Organization', value: ''),
            ],
            dropdownItemsQuery:
                "SELECT org FROM jetsapi.client_org_registry WHERE client = '{client}' ORDER BY org ASC LIMIT 100",
            stateKeyPredicates: [FSK.client]),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.objectType,
            returnedModelCacheKey: FSK.objectTypeRegistryCache,
            items: [
              DropdownItemConfig(label: 'Select an Object Type'),
            ],
            dropdownItemsQuery:
                "SELECT object_type, entity_rdf_type FROM jetsapi.object_type_registry ORDER BY object_type ASC LIMIT 50"),
        FormDropdownFieldConfig(
            key: FSK.automated,
            items: [
              DropdownItemConfig(label: 'Select Automation Status...'),
              DropdownItemConfig(label: 'Automated', value: '1'),
              DropdownItemConfig(label: 'Manual', value: '0'),
            ],
            flex: 1,
            defaultItemPos: 0),
      ],
      [
        PaddingConfig(),
      ],
      [
        // Instruction
        TextFieldConfig(
            label: "Enter the Domain Keys as json-encoded text,"
                " it can be a single column name, a list of column names, "
                " or a mapping of Object Type to column name (single or list of).",
            maxLines: 5,
            topMargin: defaultPadding,
            bottomMargin: defaultPadding)
      ],
      [
        FormInputFieldConfig(
            key: FSK.domainKeysJson,
            label: "Domain Key(s) (json)",
            hint: "Column(s) making the key of the Master Item",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLines: 6,
            maxLength: 51200),
      ],
      [
        FormInputFieldConfig(
            key: FSK.inputColumnsJson,
            label: "Input file column names (json)",
            hint: "Input file column names, only for headerless files",
            flex: 1,
            autofocus: false,
            obscureText: false,
            autovalidateMode: AutovalidateMode.always,
            textRestriction: TextRestriction.none,
            maxLines: 13,
            maxLength: 51200),
        FormInputFieldConfig(
            key: FSK.inputColumnsPositionsCsv,
            label: "Column names and positions (csv)",
            hint: "Input file column names, only for fixed-width files",
            flex: 1,
            autofocus: false,
            obscureText: false,
            autovalidateMode: AutovalidateMode.always,
            textRestriction: TextRestriction.none,
            maxLines: 13,
            maxLength: 51200),
      ],
      [
        // Instruction
        TextFieldConfig(
            label: "Note: Provide column names only for headerless files.",
            maxLines: 1,
            topMargin: 0,
            bottomMargin: 0),
        TextFieldConfig(
            label:
                "Note: Provide column names and position only for fixed-width files.",
            maxLines: 1,
            topMargin: 0,
            bottomMargin: 0),
      ],
      [
        FormInputFieldConfig(
            key: FSK.codeValuesMappingJson,
            label: "Code Values Mapping (csv or json)",
            hint: "Client-Specific Code Values Mapping to Canonical Codes",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLines: 10,
            maxLength: 51200),
      ],
    ],
    formValidatorDelegate: sourceConfigValidator,
    formActionsDelegate: sourceConfigActions,
  ),

  // Load All Files
  FormKeys.loadAllFiles: FormConfig(
    key: FormKeys.loadAllFiles,
    title: "Load all files within a time period",
    actions: [
      FormActionConfig(
          key: ActionKeys.loadAllFilesOk,
          capability: "run_pipelines",
          label: "Load All Files",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: FSK.fromSourcePeriodKey,
            tableHeight: double.infinity,
            dataTableConfig: FSK.fromSourcePeriodKey),
        FormDataTableFieldConfig(
            key: FSK.toSourcePeriodKey,
            tableHeight: double.infinity,
            dataTableConfig: FSK.toSourcePeriodKey),
      ],
      // [
      //   FormDataTableFieldConfig(
      //       key: DTKeys.fileKeyStagingMultiLoadTable,
      //       tableHeight: 600,
      //       dataTableConfig: DTKeys.fileKeyStagingMultiLoadTable)
      // ],
    ],
    formValidatorDelegate: loadAllFilesValidator,
    formActionsDelegate: loadAllFilesActions,
  ),

  // loadRawRows - Dialog to load / replace process mapping
  FormKeys.loadRawRows: FormConfig(
    key: FormKeys.loadRawRows,
    title: "File Mapping Intake",
    useListView: true,
    actions: [
      FormActionConfig(
          key: ActionKeys.loadRawRowsOk,
          capability: "client_config",
          label: "Save",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        // Instruction
        TextFieldConfig(
            label: "Enter the File Mapping Definition as csv/tsv-encoded text.",
            maxLines: 3,
            topMargin: defaultPadding,
            bottomMargin: defaultPadding)
      ],
      [
        FormInputFieldConfig(
            key: FSK.rawRows,
            label: "File Mapping (csv/tsv)",
            hint: "Paste from spreadsheet using JetStore template",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLines: 20,
            maxLength: 51200),
      ],
    ],
    formValidatorDelegate: processInputFormValidator,
    formActionsDelegate: processInputFormActions,
  ),
  // Process Input Form (table as actionless form)
  // Define ProcessInput and mapping definition
  FormKeys.inputSourceMapping: FormConfig(
    key: FormKeys.inputSourceMapping,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.inputSourceMapping,
            dataTableConfig: DTKeys.inputSourceMapping)
      ],
      [
        FormDataTableFieldConfig(
            key: DTKeys.processMappingTable,
            dataTableConfig: DTKeys.processMappingTable,
            tableHeight: double.infinity)
      ],
    ],
    formValidatorDelegate: processInputFormValidator,
    formActionsDelegate: processInputFormActions,
  ),
  // Process Input Form (table as actionless form)
  // Define ProcessInput Configuration
  FormKeys.processInput: FormConfig(
    key: FormKeys.processInput,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.processInputTable,
            dataTableConfig: DTKeys.processInputTable,
            tableHeight: 800)
      ],
    ],
    formValidatorDelegate: processInputFormValidator,
    formActionsDelegate: processInputFormActions,
  ),
  // addProcessInput - Dialog to add process input
  FormKeys.addProcessInput: FormConfig(
    key: FormKeys.addProcessInput,
    title: "Add/Update Process Input",
    actions: [
      FormActionConfig(
          key: ActionKeys.addProcessInputOk,
          capability: "client_config",
          label: "Save",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDropdownFieldConfig(
            key: FSK.client,
            items: [
              DropdownItemConfig(label: 'Select a Client'),
            ],
            dropdownItemsQuery:
                "SELECT client FROM jetsapi.client_registry ORDER BY client ASC LIMIT 150"),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.objectType,
            returnedModelCacheKey: FSK.objectTypeRegistryCache,
            items: [
              DropdownItemConfig(
                  label: 'Select an Pipeline Grouping Domain Key'),
            ],
            dropdownItemsQuery:
                "SELECT object_type, entity_rdf_type FROM jetsapi.object_type_registry ORDER BY object_type ASC LIMIT 100"),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.entityRdfType,
            returnedModelCacheKey: FSK.entityRdfTypeRegistryCache,
            items: [
              DropdownItemConfig(label: 'Select a Domain Class'),
            ],
            //* TODO read from workspace schema domain_classes (compilerv2)
            dropdownItemsQuery:
                '''SELECT entity_rdf_type FROM jetsapi.domain_keys_registry 
                   UNION 
                   SELECT entity_rdf_type FROM jetsapi.object_type_registry
                   UNION 
                   SELECT alias_rdf_type as entity_rdf_type FROM jetsapi.alias_rdf_type_registry
                   ORDER BY entity_rdf_type ASC LIMIT 100''',
            stateKeyPredicates: [FSK.objectType]),
        FormInputFieldConfig(
            key: FSK.lookbackPeriods,
            label: "Lookback Periods",
            hint: "Number of periods to include in the rule session",
            defaultValue: "0",
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.digitsOnly,
            maxLength: 10,
            useDefaultFont: true),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.sourceType,
            items: [
              DropdownItemConfig(label: 'Select a Source Type'),
              DropdownItemConfig(label: 'File', value: 'file'),
              DropdownItemConfig(label: 'Domain Table', value: 'domain_table'),
              DropdownItemConfig(
                  label: 'Alias Domain Table', value: 'alias_domain_table'),
            ],
            defaultItemPos: 0),
        FormDropdownFieldConfig(
            key: FSK.org,
            items: [
              DropdownItemConfig(label: 'Select an Organization'),
              DropdownItemConfig(label: 'No Organization', value: ''),
            ],
            dropdownItemsQuery:
                "SELECT org FROM jetsapi.client_org_registry WHERE client = '{client}' ORDER BY org ASC LIMIT 100",
            stateKeyPredicates: [FSK.client, FSK.sourceType],
            whereStateContains: {FSK.sourceType: 'file'}),
        FormDropdownFieldConfig(
            key: FSK.tableName,
            returnedModelCacheKey: FSK.entityRdfTypeRegistryCache,
            items: [
              DropdownItemConfig(label: 'Select a Domain Table'),
            ],
            dropdownItemsQuery:
                "SELECT entity_rdf_type FROM jetsapi.object_type_registry ORDER BY entity_rdf_type ASC LIMIT 100",
            whereStateContains: {FSK.sourceType: 'alias_domain_table'}),
      ],
    ],
    useListView: true,
    formValidatorDelegate: processInputFormValidator,
    formActionsDelegate: processInputFormActions,
  ),
  // processMapping - Dialog to mapping intake file structure to canonical model
  FormKeys.processMapping: FormConfig(
    key: FormKeys.processMapping,
    title: "File Mapping Worksheet",
    actions: [
      FormActionConfig(
          key: ActionKeys.mapperOk,
          capability: "client_config",
          label: "Save",
          enableOnlyWhenFormValid: true,
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.mapperDraft,
          capability: "client_config",
          label: "Save as Draft",
          enableOnlyWhenFormNotValid: true,
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    queries: {
      "inputFieldsQuery":
          "SELECT md.data_property, md.is_required, pm.input_column, pm.function_name, pm.argument, pm.default_value, pm.error_message FROM jetsapi.object_type_mapping_details md LEFT JOIN (SELECT * FROM jetsapi.process_mapping WHERE table_name = '{table_name}') pm ON md.data_property = pm.data_property WHERE md.object_type = '{object_type}' ORDER BY md.data_property ASC LIMIT 1000",
      "inputColumnsQuery":
          "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name NOT IN ('file_key','last_update','session_id','shard_id') ORDER BY column_name",
      "mappingFunctionsQuery":
          "SELECT function_name, is_argument_required FROM jetsapi.mapping_function_registry ORDER BY function_name ASC LIMIT 500",
    },
    inputFieldsQuery: "inputFieldsQuery",
    savedStateQuery: "inputFieldsQuery",
    dropdownItemsQueries: {
      FSK.inputColumnsDropdownItemsCache: "inputColumnsQuery",
      FSK.mappingFunctionsDropdownItemsCache: "mappingFunctionsQuery",
    },
    metadataQueries: {
      FSK.mappingFunctionDetailsCache: "mappingFunctionsQuery",
      FSK.inputColumnsCache: "inputColumnsQuery",
    },
    stateKeyPredicates: [FSK.objectType, FSK.tableName],
    inputFieldRowBuilder: (index, inputFieldRow, formState) {
      assert(inputFieldRow != null, 'error inputFieldRow should not be null');
      if (inputFieldRow == null) {
        return [];
      }
      // savedState is List<String?>? with values as per savedStateQuery
      final savedState = formState.getCacheValue(FSK.savedStateCache) as List?;
      final isRequired = inputFieldRow[1]! == '1';
      final isRequiredIndicator = isRequired ? '*' : '';
      final savedInputColumn = savedState?[index][2];
      final inputColumnList =
          formState.getCacheValue(FSK.inputColumnsCache) as List;
      final inputColumnDefault =
          inputColumnList.contains(inputFieldRow[0]) ? inputFieldRow[0] : null;
      if (isRequired) formState.setValue(index, FSK.isRequiredFlag, "1");
      // set the default values to the formState
      formState.setValue(index, FSK.dataProperty, inputFieldRow[0]);
      formState.setValue(
          index, FSK.inputColumn, savedInputColumn ?? inputColumnDefault);
      formState.setValue(index, FSK.functionName, savedState?[index][3]);
      formState.setValue(index, FSK.functionArgument, savedState?[index][4]);
      formState.setValue(index, FSK.mappingDefaultValue, savedState?[index][5]);
      formState.setValue(index, FSK.mappingErrorMessage, savedState?[index][6]);
      // print("Form BUILDER savedState row ${savedState![index]}");
      return [
        [
          // data_property
          TextFieldConfig(
              label: "$index: ${inputFieldRow[0]}$isRequiredIndicator",
              group: index,
              flex: 1,
              topMargin: 20.0)
        ],
        [
          // input_column
          FormDropdownWithSharedItemsFieldConfig(
            key: FSK.inputColumn,
            group: index,
            flex: 2,
            autovalidateMode: AutovalidateMode.always,
            dropdownMenuItemCacheKey: FSK.inputColumnsDropdownItemsCache,
            defaultItem: savedInputColumn ?? inputColumnDefault,
          ),
          // function_name
          FormDropdownWithSharedItemsFieldConfig(
            key: FSK.functionName,
            group: index,
            flex: 1,
            dropdownMenuItemCacheKey: FSK.mappingFunctionsDropdownItemsCache,
            defaultItem: savedState?[index][3],
          ),
          // argument
          FormInputFieldConfig(
            key: FSK.functionArgument,
            label: "Function Argument",
            hint:
                "Cleansing function argument, it is either required or ignored",
            group: index,
            flex: 1,
            autovalidateMode: AutovalidateMode.always,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 512,
          ),
          // default_value
          FormInputFieldConfig(
            key: FSK.mappingDefaultValue,
            label: "Default Value",
            hint:
                "Default value to use if input value is not provided or cleansing function returns null",
            group: index,
            flex: 1,
            autovalidateMode: AutovalidateMode.always,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 512,
          ),
          // error_message
          FormInputFieldConfig(
            key: FSK.mappingErrorMessage,
            label: "Error Message",
            hint:
                "Error message to raise if input value is not provided or cleansing function returns null and there is no default value",
            group: index,
            flex: 1,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 125,
          ),
        ],
      ];
    },
    formValidatorDelegate: processInputFormValidator,
    formActionsDelegate: processInputFormActions,
  ),

  // Rule Configv2 - Action-less form to select Rule Configv2 to Edit or do Add
  FormKeys.rulesConfigv2: FormConfig(
    key: FormKeys.rulesConfigv2,
    actions: [],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.ruleConfigv2Table,
            dataTableConfig: DTKeys.ruleConfigv2Table,
            tableHeight: double.infinity),
      ],
    ],
    formValidatorDelegate: ruleConfigv2FormValidator,
    formActionsDelegate: ruleConfigv2FormActions,
  ),

  // Rule Configuration v2 - Dialog to edit rule_configv2.rule_config_json
  FormKeys.rulesConfigv2Dialog: FormConfig(
    key: FormKeys.rulesConfigv2Dialog,
    title: "Rule Configuration",
    actions: [
      FormActionConfig(
          key: ActionKeys.ruleConfigv2Ok,
          capability: "client_config",
          label: "Save",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    useListView: true,
    inputFields: [
      [
        FormDropdownFieldConfig(
            key: FSK.client,
            items: [
              DropdownItemConfig(label: 'Select a Client'),
            ],
            autovalidateMode: AutovalidateMode.onUserInteraction,
            makeReadOnlyWhenHasSelectedValue: true,
            dropdownItemsQuery:
                "SELECT client FROM jetsapi.client_registry ORDER BY client ASC LIMIT 150"),
        FormDropdownFieldConfig(
            key: FSK.processName,
            returnedModelCacheKey: FSK.processConfigCache,
            items: [
              DropdownItemConfig(label: 'Select a process'),
            ],
            autovalidateMode: AutovalidateMode.onUserInteraction,
            makeReadOnlyWhenHasSelectedValue: true,
            dropdownItemsQuery:
                "SELECT process_name, key FROM jetsapi.process_config ORDER BY process_name ASC LIMIT 100"),
      ],
      [
        FormInputFieldConfig(
            key: FSK.ruleConfigJson,
            flex: 10,
            label: "Rule Configuration CSV / Json",
            hint:
                "Enter a valid json array or csv with headers of configuration objects",
            maxLines: 25,
            maxLength: 51200,
            autofocus: false,
            textRestriction: TextRestriction.none,
            defaultValue: "[]"),
      ],
    ],
    formValidatorDelegate: ruleConfigv2FormValidator,
    formActionsDelegate: ruleConfigv2FormActions,
  ),

  // ruleConfig - Dialog to enter rule config triples
  // This form is depricated, keeping here as an example of a dynamic form
  // where a row of controls is added dynamically
  FormKeys.rulesConfig: FormConfig(
    key: FormKeys.rulesConfig,
    title: "Rule Configuration",
    actions: [
      FormActionConfig(
          key: ActionKeys.ruleConfigOk,
          capability: "client_config",
          label: "Save",
          enableOnlyWhenFormValid: true,
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    queries: {
      "inputFieldsQuery":
          "SELECT subject, predicate, object, rdf_type FROM jetsapi.rule_config WHERE client = '{client}' AND process_name = '{process_name}' ORDER BY subject ASC, predicate ASC, object ASC LIMIT 300",
    },
    inputFieldsQuery: "inputFieldsQuery",
    stateKeyPredicates: [FSK.client, FSK.processName],
    formWithDynamicRows: true,
    inputFieldRowBuilder: (index, inputFieldRow, formState) {
      var isLastRow = inputFieldRow == null;
      inputFieldRow ??= List<String?>.filled(4, '');
      // set the default values to the formState
      formState.setValue(index, FSK.subject, inputFieldRow[0]);
      formState.setValue(index, FSK.predicate, inputFieldRow[1]);
      formState.setValue(index, FSK.object, inputFieldRow[2]);
      formState.setValue(index, FSK.rdfType, inputFieldRow[3]);
      // print("Form BUILDER savedState row $inputFieldRow");
      return [
        [
          // NOTE: ** if the layout of the input field row changes,
          // need to also reflect the change in config_delegate.dart
          // for the Add Row action. **
          // subject
          if (!isLastRow)
            FormInputFieldConfig(
              key: FSK.subject,
              label: "Subject",
              hint: "Rule config subject",
              group: index,
              flex: 2,
              autovalidateMode: AutovalidateMode.always,
              autofocus: false,
              obscureText: false,
              textRestriction: TextRestriction.none,
              maxLength: 512,
            ),
          if (isLastRow) TextFieldConfig(label: '', flex: 2),
          // predicate
          if (!isLastRow)
            FormInputFieldConfig(
              key: FSK.predicate,
              label: "Predicate",
              hint: "Rule config predicate",
              group: index,
              flex: 2,
              autovalidateMode: AutovalidateMode.always,
              autofocus: false,
              obscureText: false,
              textRestriction: TextRestriction.none,
              maxLength: 512,
            ),
          if (isLastRow) TextFieldConfig(label: '', flex: 2),
          // object
          if (!isLastRow)
            FormInputFieldConfig(
              key: FSK.object,
              label: "Object",
              hint: "Rule config object",
              group: index,
              flex: 2,
              autovalidateMode: AutovalidateMode.always,
              autofocus: false,
              obscureText: false,
              textRestriction: TextRestriction.none,
              maxLength: 512,
            ),
          if (isLastRow) TextFieldConfig(label: '', flex: 2),
          // rdf type
          if (!isLastRow)
            FormDropdownFieldConfig(
              key: FSK.rdfType,
              group: index,
              flex: 1,
              autovalidateMode: AutovalidateMode.always,
              items: FormDropdownFieldConfig.rdfDropdownItems,
              defaultItemPos: 0,
            ),
          if (isLastRow) TextFieldConfig(label: '', flex: 1),
          // add / delete row button
          FormActionConfig(
            key: isLastRow
                ? ActionKeys.ruleConfigAdd
                : ActionKeys.ruleConfigDelete,
            group: isLastRow ? 0 : index,
            flex: 1,
            label: isLastRow ? 'Add Row' : '',
            labelByStyle: {
              ActionStyle.alternate: 'Delete',
              ActionStyle.danger: 'Confirm',
            },
            buttonStyle:
                isLastRow ? ActionStyle.secondary : ActionStyle.alternate,
            leftMargin: defaultPadding,
            rightMargin: defaultPadding,
          ),
        ],
      ];
    },
    formValidatorDelegate: processConfigFormValidator,
    formActionsDelegate: processConfigFormActions,
  ),

  // Process & Rules Config (actionless)
  FormKeys.processConfig: FormConfig(
    key: FormKeys.processConfig,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.clientsAndProcessesTableView,
            tableHeight: 400,
            dataTableConfig: DTKeys.clientsAndProcessesTableView),
      ],
      [
        FormDataTableFieldConfig(
            key: DTKeys.ruleConfigTable,
            tableHeight: double.infinity,
            dataTableConfig: DTKeys.ruleConfigTable)
      ],
    ],
    formValidatorDelegate: processConfigFormValidator,
    formActionsDelegate: processConfigFormActions,
  ),

  // Add/Edit pipelineConfig - Form to add / edit pipeline config
  FormKeys.pipelineConfigForm: FormConfig(
    key: FormKeys.pipelineConfigForm,
    // title: "Pipeline Configuration",
    actions: [],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.pipelineConfigTable,
            dataTableConfig: DTKeys.pipelineConfigTable,
            tableHeight: double.infinity),
      ],
    ],
    formValidatorDelegate: pipelineConfigFormValidator,
    formActionsDelegate: pipelineConfigFormActions,
  ),

  // Add/Edit pipelineConfig - Dialog to add / edit pipeline config
  FormKeys.pipelineConfigEditForm: FormConfig(
    key: FormKeys.pipelineConfigEditForm,
    title: "Edit Pipeline Configuration",
    actions: [
      FormActionConfig(
          key: ActionKeys.pipelineConfigOk,
          capability: "client_config",
          label: "Save",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDropdownFieldConfig(
            key: FSK.client,
            items: [
              DropdownItemConfig(label: 'Select a Client'),
            ],
            autovalidateMode: AutovalidateMode.onUserInteraction,
            dropdownItemsQuery:
                "SELECT client FROM jetsapi.client_registry ORDER BY client ASC LIMIT 150"),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.processName,
            returnedModelCacheKey: FSK.processConfigCache,
            items: [
              DropdownItemConfig(label: 'Select a process'),
            ],
            autovalidateMode: AutovalidateMode.onUserInteraction,
            dropdownItemsQuery:
                "SELECT process_name, key FROM jetsapi.process_config ORDER BY process_name ASC LIMIT 100"),
      ],
      [
        FormInputFieldConfig(
            key: FSK.maxReteSessionSaved,
            label: "Max Rete Session Saved",
            hint: "Max Rete Session Saved per server thread",
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.digitsOnly,
            autovalidateMode: AutovalidateMode.onUserInteraction,
            maxLength: 4,
            useDefaultFont: true,
            defaultValue: '0'),
      ],
      [
        PaddingConfig(height: defaultPadding),
      ],
      [
        FormDropdownFieldConfig(
            key: FSK.sourcePeriodType,
            items: [
              DropdownItemConfig(label: 'Select execution frequency'),
              DropdownItemConfig(label: 'Monthly', value: 'month_period'),
              DropdownItemConfig(label: 'Weekly', value: 'week_period'),
              DropdownItemConfig(label: 'Daily', value: 'day_period'),
            ],
            flex: 1,
            defaultItemPos: 0),
        FormDropdownFieldConfig(
            key: FSK.automated,
            items: [
              DropdownItemConfig(label: 'Select automation mode'),
              DropdownItemConfig(label: 'Automated', value: '1'),
              DropdownItemConfig(label: 'Manual', value: '0'),
            ],
            flex: 1,
            defaultItemPos: 0),
      ],
      [
        FormInputFieldConfig(
            key: FSK.description,
            label: "Description",
            hint: "Pipeline configuration description",
            flex: 2,
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLength: 512,
            useDefaultFont: true),
      ],
      [
        PaddingConfig(height: 2 * defaultPadding),
      ],
      [
        FormDataTableFieldConfig(
            key: FSK.mainProcessInputKey,
            dataTableConfig: FSK.mainProcessInputKey),
      ],
      [
        PaddingConfig(),
      ],
      [
        FormDataTableFieldConfig(
            key: FSK.mergedProcessInputKeys,
            dataTableConfig: FSK.mergedProcessInputKeys),
      ],
      [
        PaddingConfig(),
      ],
      [
        FormDataTableFieldConfig(
            key: FSK.injectedProcessInputKeys,
            dataTableConfig: FSK.injectedProcessInputKeys),
      ],
      [
        TextFieldConfig(
            key: 'Rule_Configuration_Json_Header',
            label: "Rule Configuration CSV / Json"),
      ],
      [
        FormInputFieldConfig(
            key: FSK.ruleConfigJson,
            label: "Rule Configuration CSV / Json",
            hint:
                "Enter a valid json array or csv with headers of configuration objects",
            maxLines: 10,
            maxLength: 51200,
            autofocus: false,
            textRestriction: TextRestriction.none,
            defaultValue: "[]"),
      ],
    ],
    formValidatorDelegate: pipelineConfigFormValidator,
    formActionsDelegate: pipelineConfigFormActions,
  ),

  // Start Pipeline - Dialog
  FormKeys.startPipeline: FormConfig(
    key: FormKeys.startPipeline,
    title: "Start Pipeline",
    actions: [
      FormActionConfig(
          key: ActionKeys.startPipelineOk,
          capability: "run_pipelines",
          label: "Start",
          buttonStyle: ActionStyle.primary,
          leftMargin: defaultPadding,
          rightMargin: betweenTheButtonsPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Cancel",
          buttonStyle: ActionStyle.secondary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        // Instruction
        TextFieldConfig(
            label:
                "To start a pipeline using data previously loaded, select a Pipeline Configuration followed"
                " by the source period the file was received, and"
                " then select the Main Input Source (required) and optionally"
                " the Merge-In Input Sources.",
            maxLines: 5,
            topMargin: defaultPadding,
            bottomMargin: defaultPadding)
      ],
      [
        // Pipeline Configuration Table (note using FSK key)
        FormDataTableFieldConfig(
            key: FSK.pipelineConfigKey, dataTableConfig: FSK.pipelineConfigKey),
      ],
      [
        PaddingConfig(),
      ],
      [
        // Table to show the main process_input of the selected pipeline above
        // this is informative to the user
        FormDataTableFieldConfig(
            key: DTKeys.mainProcessInputTable,
            dataTableConfig: DTKeys.mainProcessInputTable,
            tableHeight: 225),
      ],
      [
        FormDataTableFieldConfig(
            key: FSK.mainInputRegistryKey,
            dataTableConfig: FSK.mainInputRegistryKey),
      ],
      [
        PaddingConfig(),
      ],
      [
        // Table to show the injected_process_input of the selected pipeline above
        // this is informative to the user
        FormDataTableFieldConfig(
            key: DTKeys.injectedProcessInputTable,
            dataTableConfig: DTKeys.injectedProcessInputTable,
            tableHeight: 225),
      ],
      [
        PaddingConfig(),
      ],
      [
        // Table to show the merge process_input of the selected pipeline above
        // this is informative to the user
        FormDataTableFieldConfig(
            key: DTKeys.mergeProcessInputTable,
            dataTableConfig: DTKeys.mergeProcessInputTable,
            tableHeight: 250),
      ],
      [
        FormDataTableFieldConfig(
            key: FSK.mergedInputRegistryKeys,
            dataTableConfig: FSK.mergedInputRegistryKeys),
      ],
    ],
    formValidatorDelegate: homeFormValidator,
    formActionsDelegate: homeFormActions,
  ),

  // Show Pipeline Failure Details - Dialog
  FormKeys.showFailureDetails: FormConfig(
    key: FormKeys.showFailureDetails,
    title: "Pipeline Failure Details",
    actions: [
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Close",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.failureDetails,
            label: "Failure Details",
            hint: "This is read only",
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLines: 20,
            maxLength: 4000000),
      ],
    ],
    formValidatorDelegate: homeFormValidator,
    formActionsDelegate: homeFormActions,
  ),

  // View Process Errors (table as actionless form)
  FormKeys.viewProcessErrors: FormConfig(
    key: FormKeys.viewProcessErrors,
    actions: [
      // Action-less form
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.processErrorsTable,
            dataTableConfig: DTKeys.processErrorsTable,
            tableHeight: double.infinity)
      ],
    ],
    formValidatorDelegate: alwaysValidForm,
    formActionsDelegate: processErrorsActions,
  ),

  // View Input Records for a domain key from Process Errors (table as actionless dialog)
  FormKeys.viewInputRecords: FormConfig(
    key: FormKeys.viewInputRecords,
    title: "Input Records for a Domain Key",
    actions: [
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Close",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    queries: {
      "inputFieldsQuery": """
          WITH per AS (
            SELECT 1 AS order,
              'Main Input' AS label,
              main_input_registry_key AS key
            FROM jetsapi.pipeline_execution_status pes
            WHERE pes.key = {pipeline_execution_status_key}
            UNION
            SELECT 2 AS order,
              'Merged Input' AS label,
              unnest(merged_input_registry_keys) AS key
            FROM jetsapi.pipeline_execution_status pes
            WHERE pes.key = {pipeline_execution_status_key}
          ),
          pcr AS (
            SELECT 3 AS order,
              'Injected Input' AS label,
              unnest(pc.injected_process_input_keys) AS key
            FROM jetsapi.pipeline_execution_status pe,
              jetsapi.pipeline_config pc
            WHERE pe.key = {pipeline_execution_status_key}
              AND pe.pipeline_config_key = pc.key
          )
          SELECT t.order,
            t.label,
            t.table_name,
            t.lookback_periods,
            t.session_id,
            {pipeline_execution_status_key} AS pipeline_execution_status_key,
            '{object_type}' AS object_type,
            '{domain_key}' AS domain_key
          FROM (
              SELECT per.order,
                per.label,
                pi.table_name,
                pi.lookback_periods,
                ir.session_id
              FROM jetsapi.process_input pi,
                jetsapi.input_registry ir,
                per
              WHERE ir.key = per.key
                AND ir.client = pi.client
                AND ir.org = pi.org
                AND ir.object_type = pi.object_type
                AND ir.table_name = pi.table_name
              UNION
              SELECT pcr.order,
                pcr.label,
                pi.table_name,
                pi.lookback_periods,
                NULL AS session_id
              FROM jetsapi.process_input pi,
                pcr
              WHERE pi.key = pcr.key
            ) AS t
          ORDER BY t.order ASC""",
    },
    inputFieldsQuery: "inputFieldsQuery",
    stateKeyPredicates: [
      FSK.pipelineExectionStatusKey,
      FSK.objectType,
      FSK.domainKey,
    ],
    // inputFieldRow: [order, label, table_name, lookback_periods, session_id,
    //                 pipeline_execution_status_key, object_type, domain_key]
    inputFieldRowBuilder: (index, inputFieldRow, formState) {
      assert(inputFieldRow != null,
          'viewInputRecords form builder: error inputFieldRow should not be null');
      if (inputFieldRow == null) {
        return [];
      }
      // set table predicate values to the formState
      formState.setValue(index, FSK.label, inputFieldRow[1]);
      formState.setValue(index, FSK.tableName, inputFieldRow[2]);
      formState.setValue(index, FSK.lookbackPeriods, inputFieldRow[3]);
      formState.setValue(index, FSK.sessionId, inputFieldRow[4]);
      formState.setValue(
          index, FSK.pipelineExectionStatusKey, inputFieldRow[5]);
      formState.setValue(index, FSK.objectType, inputFieldRow[6]);
      formState.setValue(index, FSK.domainKey, inputFieldRow[7]);
      formState.setValue(
          index, FSK.domainKeyColumn, '${inputFieldRow[6]}:domain_key');
      return [
        [
          FormDataTableFieldConfig(
              key: DTKeys.inputRecordsFromProcessErrorTable + index.toString(),
              group: index,
              dataTableConfig: DTKeys.inputRecordsFromProcessErrorTable,
              tableHeight: 400)
        ],
      ];
    },
    formValidatorDelegate: alwaysValidForm,
    formActionsDelegate: processErrorsActions,
  ),

  // View process_errors.rete_session_triples from Process Errors (table as actionless dialog)
  FormKeys.viewReteTriples: FormConfig(
    key: FormKeys.viewReteTriples,
    title: "Rete Session as Triples",
    actions: [
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Close",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.reteSessionTriplesTable,
            dataTableConfig: DTKeys.reteSessionTriplesTable,
            tableHeight: 1000)
      ],
    ],
    formValidatorDelegate: alwaysValidForm,
    formActionsDelegate: processErrorsActions,
  ),

  FormKeys.viewReteTriplesV2: FormConfig(
    key: FormKeys.viewReteTriplesV2,
    title: "Rete Session Explorer",
    actions: [
      FormActionConfig(
          key: ActionKeys.dialogCancel,
          label: "Close",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.reteSessionRdfTypeTable,
            flex: 1,
            dataTableConfig: DTKeys.reteSessionRdfTypeTable,
            tableHeight: double.infinity),
        FormDataTableFieldConfig(
            key: DTKeys.reteSessionEntityKeyTable,
            flex: 1,
            dataTableConfig: DTKeys.reteSessionEntityKeyTable,
            tableHeight: double.infinity),
        FormDataTableFieldConfig(
            key: DTKeys.reteSessionEntityDetailsTable,
            flex: 2,
            dataTableConfig: DTKeys.reteSessionEntityDetailsTable,
            tableHeight: double.infinity),
      ],
    ],
    formValidatorDelegate: alwaysValidForm,
    formActionsDelegate: processErrorsActions,
  ),

  // Query Tool Input Form
  FormKeys.queryToolInputForm: FormConfig(
    key: FormKeys.queryToolInputForm,
    title: "Query Tool",
    actions: [
      FormActionConfig(
          key: ActionKeys.queryToolOk,
          capability: "workspace_ide",
          label: "Submit Query",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
      FormActionConfig(
          key: ActionKeys.queryToolDdlOk,
          label: "Submit DDL",
          buttonStyle: ActionStyle.primary,
          leftMargin: betweenTheButtonsPadding,
          rightMargin: defaultPadding,
          bottomMargin: defaultPadding),
    ],
    inputFields: [
      [
        FormInputFieldConfig(
            key: FSK.rawQuery,
            label: "Query",
            hint: "Paste query",
            autofocus: false,
            obscureText: false,
            textRestriction: TextRestriction.none,
            maxLines: 10,
            maxLength: 4000000),
      ],
    ],
    formValidatorDelegate: queryToolFormValidator,
    formActionsDelegate: queryToolFormActions,
  ),

  // Query Tool Result Viewer Form
  FormKeys.queryToolResultViewForm: FormConfig(
    key: FormKeys.queryToolResultViewForm,
    actions: [],
    inputFields: [
      [
        FormDataTableFieldConfig(
            key: DTKeys.queryToolResultSetTable,
            dataTableConfig: DTKeys.queryToolResultSetTable,
            tableHeight: double.infinity)
      ],
    ],
    formValidatorDelegate: queryToolFormValidator,
    formActionsDelegate: queryToolFormActions,
  ),
};

FormConfig getFormConfig(String key) {
  var config = _formConfigurations[key];
  if (config != null) return config;
  config = getWorkspaceFormConfig(key);
  if (config != null) return config;
  config = getClientRegistryFormConfig(key);
  if (config != null) return config;
  config = getConfigureFileFormConfig(key);
  if (config != null) return config;
  config = getFileMappingFormConfig(key);
  if (config != null) return config;
  config = getPipelineConfigFormConfig(key);
  if (config != null) return config;
  config = getLoadFilesFormConfig(key);
  if (config != null) return config;
  config = getStartPipelineFormConfig(key);
  if (config != null) return config;
  config = getWorkspacePullFormConfig(key);
  if (config != null) return config;
  throw Exception(
      'ERROR: Invalid program configuration: form configuration $key not found');
}