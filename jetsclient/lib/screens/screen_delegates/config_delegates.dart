import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:jetsclient/routes/jets_router_delegate.dart';
import 'package:jetsclient/screens/components/dialogs.dart';
import 'package:jetsclient/screens/components/jets_form_state.dart';
import 'package:jetsclient/utils/constants.dart';
import 'package:jetsclient/http_client.dart';
import 'package:jetsclient/utils/form_config.dart';
import 'package:provider/provider.dart';

/// Validation and Actions delegates for the source to pipeline config forms
/// Login Form Validator
String? homeFormValidator(
    JetsFormState formState, int group, String key, dynamic v) {
  assert((v is String?) || (v is List<String>?),
      "sourceConfig Form has unexpected data type");
  switch (key) {
    case FSK.client:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      if (value != null && value.characters.length == 1) {
        return "Client name is too short.";
      }
      return "Client name must be provided.";
    case FSK.objectType:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      return "Object Type name must be selected.";
    case FSK.fileKey:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      return "File Key name must be selected.";
    case FSK.details:
      // always good
      return null;
    case FSK.groupingColumn:
      return null;
    default:
      print('Oops home form has no validator configured for form field $key');
  }
  return null;
}

void postInsertRows(BuildContext context, JetsFormState formState,
    String encodedJsonBody) async {
  var navigator = Navigator.of(context);
  var messenger = ScaffoldMessenger.of(context);
  var result = await context.read<HttpClient>().sendRequest(
      path: ServerEPs.dataTableEP,
      token: JetsRouterDelegate().user.token,
      encodedJsonBody: encodedJsonBody);

  // print("Got reply with status code ${result.statusCode}");
  if (result.statusCode == 200) {
    // Inform the user and transition
    const snackBar = SnackBar(content: Text('Record(s) successfully inserted'));
    messenger.showSnackBar(snackBar);
    // All good, let's the table know to refresh
    navigator.pop(DTActionResult.okDataTableDirty);
  } else if (result.statusCode == 400 ||
      result.statusCode == 406 ||
      result.statusCode == 422) {
    // http Bad Request / Not Acceptable / Unprocessable
    formState.setValue(
        0, FSK.serverError, "Something went wrong. Please try again.");
    navigator.pop(DTActionResult.statusError);
  } else if (result.statusCode == 409) {
    // http Conflict
    const snackBar = SnackBar(
      content: Text("Looks like the record(s) already existed, that's ok."),
    );
    messenger.showSnackBar(snackBar);
    navigator.pop();
  } else {
    formState.setValue(
        0, FSK.serverError, "Got a server error. Please try again.");
    navigator.pop(DTActionResult.statusError);
  }
}

/// Source Configuration Form Actions
void homeFormActions(BuildContext context, GlobalKey<FormState> formKey,
    JetsFormState formState, String actionKey,
    {group = 0}) async {
  switch (actionKey) {
    case ActionKeys.clientOk:
      var valid = formKey.currentState!.validate();
      if (!valid) {
        return;
      }
      var encodedJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'client_registry',
        'data': [formState.getState(0)],
      }, toEncodable: (_) => '');
      postInsertRows(context, formState, encodedJsonBody);
      break;
    case ActionKeys.loaderOk:
      var valid = formKey.currentState!.validate();
      if (!valid) {
        return;
      }
      var state = formState.getState(0);
      state['status'] = StatusKeys.submitted;
      state['user_email'] = JetsRouterDelegate().user.email;
      state['session_id'] = "${DateTime.now().millisecondsSinceEpoch}";
      state['table_name'] = state[FSK.client] + '_' + state[FSK.objectType];
      var encodedJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'input_loader_status',
        'data': [state],
      }, toEncodable: (_) => '');
      postInsertRows(context, formState, encodedJsonBody);
      break;
    case ActionKeys.dialogCancel:
      Navigator.of(context).pop();
      break;
    default:
      print('Oops unknown ActionKey for home form: $actionKey');
  }
}

/// Process Input Form / Dialog Validator
String? processInputFormValidator(
    JetsFormState formState, int group, String key, dynamic v) {
  assert((v is String?) || (v is List<String>?),
      "Process Input Form has unexpected data type");
  var isRequired = formState.getValue(group, FSK.isRequiredFlag);
  // print(
  //     "Validator Called for $group ($isRequired), $key, $v, state is ${formState.getValue(group, key)}");
  switch (key) {
    // Add Process Input Dialog Validations
    case FSK.client:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      return "Client name must be provided.";
    case FSK.objectType:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      return "Object Type name must be selected.";
    case FSK.sourceType:
      String? value = v;
      if (value != null && value.characters.length > 1) {
        return null;
      }
      return "Source Type name must be selected.";
    case FSK.groupingColumn:
      // always good
      return null;

    // Process Mapping Dialog Validation
    case FSK.inputColumn:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      if (isRequired == null || isRequired == false) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      var defaultValue =
          formState.getValue(group, FSK.mappingDefaultValue) as String?;
      if (defaultValue != null && defaultValue.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      var errorMsg =
          formState.getValue(group, FSK.mappingErrorMessage) as String?;
      if (errorMsg != null && errorMsg.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Input Column must be selected or either a default or an error message must be provided.";

    case FSK.functionName:
      return null;

    case FSK.functionArgument:
      String? value = v;
      var functionName = formState.getValue(group, FSK.functionName) as String?;
      // print("Validating argument '$value' for function $functionName");
      if (functionName == null || functionName.isEmpty) {
        if (value != null && value.isNotEmpty) {
          formState.markFormKeyAsInvalid(group, key);
          return "Remove the argument when no function is selected";
        } else {
          formState.markFormKeyAsValid(group, key);
          return null;
        }
      }
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      var mappingFunctionDetails =
          formState.getCacheValue(FSK.mappingFunctionDetailsCache) as List?;
      assert(mappingFunctionDetails != null,
          "processInputFormActions error: mappingFunctionDetails is null");
      if (mappingFunctionDetails == null) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      var row = mappingFunctionDetails.firstWhere(
        (e) => e[0] == functionName,
      );
      // check if function argument is required
      if (row[1] != "1") {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Cleansing function argument is required";

    case FSK.mappingDefaultValue:
      String? value = v;
      if (value != null && value.isEmpty) {
        value = null;
      }
      var errorMsg =
          formState.getValue(group, FSK.mappingErrorMessage) as String?;
      if (errorMsg != null && errorMsg.isEmpty) {
        errorMsg = null;
      }
      if (value != null && errorMsg == null) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      if (value == null && errorMsg != null) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      if (value != null && errorMsg != null) {
        formState.markFormKeyAsInvalid(group, key);
        return "Cannot specify both a default value and an error message";
      }
      formState.markFormKeyAsValid(group, key);
      return null;

    case FSK.mappingErrorMessage:
      return null;
    default:
      print(
          'Oops process input form has no validator configured for form field $key');
  }
  return null;
}

/// Process and Rules Config Form / Dialog Validator
String? processConfigFormValidator(
    JetsFormState formState, int group, String key, dynamic v) {
  assert((v is String?) || (v is List<String>?),
      "Process Config Form has unexpected data type");
  // print(
  //     "Validator Called for $group, $key, $v, state is ${formState.getValue(group, key)}");
  switch (key) {
    // Rule Config Dialog Validation
    case FSK.subject:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Rule Config Subject must be provided.";

    case FSK.predicate:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Rule Config Predicate must be provided.";

    case FSK.object:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Rule Config Object must be provided.";

    case FSK.rdfType:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        formState.markFormKeyAsValid(group, key);
        return null;
      }
      formState.markFormKeyAsInvalid(group, key);
      return "Rule Config Object rdf type must be provided.";

    default:
      print(
          'Oops process / rules config form has no validator configured for form field $key');
  }
  return null;
}

/// Process Input and Mapping Form Actions
void processInputFormActions(BuildContext context, GlobalKey<FormState> formKey,
    JetsFormState formState, String actionKey,
    {group = 0}) async {
  switch (actionKey) {
    case ActionKeys.addProcessInputOk:
      var valid = formKey.currentState!.validate();
      if (!valid) {
        return;
      }

      // add entity_rdf_type based on object_type
      var objectTypeRegistry =
          formState.getCacheValue(FSK.objectTypeRegistryCache) as List?;
      String objectType = formState.getValue(0, FSK.objectType) as String;
      if (objectTypeRegistry == null) {
        print("processInputFormActions error: objectTypeRegistry is null");
        return;
      }
      var row = objectTypeRegistry.firstWhere((e) => e[0] == objectType);
      if (row == null) {
        print(
            "processInputFormActions error: can't find object_type in objectTypeRegistry");
        return;
      }
      formState.setValue(0, FSK.entityRdfType, row[1]);

      // add table_name to form state based on source_type
      if (formState.getValue(0, FSK.sourceType) == 'file') {
        String client = formState.getValue(0, FSK.client) as String;
        formState.setValue(0, FSK.tableName, "${client}_$objectType");
      } else {
        // entity_rdf_type is the table_name for domain_table sources
        formState.setValue(0, FSK.tableName, row[1]);
      }
      formState.setValue(0, FSK.userEmail, JetsRouterDelegate().user.email);

      var encodedJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'process_input',
        'data': [formState.getState(0)],
      }, toEncodable: (_) => '');
      postInsertRows(context, formState, encodedJsonBody);
      break;

    // Process Mapping Dialog
    case ActionKeys.mapperOk:
    case ActionKeys.mapperDraft:
      var processInputStatus = 'saved as draft';
      if (actionKey == ActionKeys.mapperOk) {
        processInputStatus = 'configured';
        if (!formState.isFormValid()) {
          return;
        }
      }
      // Insert rows to process_mapping, if successful update process_input.status
      var tableName = formState.getValue(0, FSK.tableName);
      var processInputKey = formState.getValue(0, FSK.processInputKey);
      if (tableName == null || processInputKey == null) {
        print(
            "processInputFormActions error: save mapping - table_name or process_input.key is null");
        return;
      }
      for (var i = 0; i < formState.groupCount; i++) {
        formState.setValue(i, FSK.tableName, tableName);
        formState.setValue(i, FSK.userEmail, JetsRouterDelegate().user.email);
      }
      var navigator = Navigator.of(context);
      // first issue a delete statement to make sure we replace all mappings
      var deleteJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'delete/process_mapping',
        'data': [
          {
            FSK.tableName: tableName,
            FSK.userEmail: JetsRouterDelegate().user.email,
          }
        ],
      }, toEncodable: (_) => '');
      var deleteResult = await context.read<HttpClient>().sendRequest(
          path: ServerEPs.dataTableEP,
          token: JetsRouterDelegate().user.token,
          encodedJsonBody: deleteJsonBody);

      if (deleteResult.statusCode != 200) {
        formState.setValue(
            0, FSK.serverError, "Something went wrong. Please try again.");
        navigator.pop(DTActionResult.statusError);
        return;
      }
      // delete successfull, update process_mapping
      var encodedJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'process_mapping',
        'data': formState.getInternalState(),
      }, toEncodable: (_) => '');
      // Insert rows to process_mapping
      var result = await context.read<HttpClient>().sendRequest(
          path: ServerEPs.dataTableEP,
          token: JetsRouterDelegate().user.token,
          encodedJsonBody: encodedJsonBody);

      if (result.statusCode == 200) {
        // insert successfull, update process_input status
        var encodedJsonBody = jsonEncode(<String, dynamic>{
          'action': 'insert_rows',
          'table': 'update/process_input',
          'data': [
            {
              'key': processInputKey,
              'user_email': JetsRouterDelegate().user.email,
              'status': processInputStatus
            }
          ],
        }, toEncodable: (_) => '');
        postInsertRows(context, formState, encodedJsonBody);
        // trigger a refresh of the process_mapping table
        formState.parentFormState?.setValue(0, FSK.tableName, null);
        formState.parentFormState
            ?.setValueAndNotify(0, FSK.tableName, tableName);
      } else if (result.statusCode == 400 ||
          result.statusCode == 406 ||
          result.statusCode == 422) {
        // http Bad Request / Not Acceptable / Unprocessable
        formState.setValue(
            0, FSK.serverError, "Something went wrong. Please try again.");
        navigator.pop(DTActionResult.statusError);
      } else {
        formState.setValue(
            0, FSK.serverError, "Got a server error. Please try again.");
        navigator.pop(DTActionResult.statusError);
      }
      break;
    case ActionKeys.dialogCancel:
      Navigator.of(context).pop();
      break;
    default:
      print('Oops unknown ActionKey for process input form: $actionKey');
  }
}

/// Process and Rules Config Form Actions
void processConfigFormActions(BuildContext context,
    GlobalKey<FormState> formKey, JetsFormState formState, String actionKey,
    {required int group}) async {
  switch (actionKey) {
    // Rule Config Dialog
    case ActionKeys.ruleConfigOk:
      if (!formState.isFormValid()) {
        return;
      }
      // Insert rows to rule_config table
      var processConfigKey = formState.getValue(0, FSK.processConfigKey);
      var processName = formState.getValue(0, FSK.processName);
      var client = formState.getValue(0, FSK.client);
      if (processName == null || client == null) {
        print("processConfigFormActions error: save rule config");
        return;
      }
      for (var i = 0; i < formState.groupCount; i++) {
        formState.setValue(i, FSK.client, client);
        formState.setValue(i, FSK.processConfigKey, processConfigKey);
        formState.setValue(i, FSK.processName, processName);
        formState.setValue(i, FSK.userEmail, JetsRouterDelegate().user.email);
      }
      var stateList = formState.getInternalState();

      var deleteJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': 'delete/rule_config',
        'data': [
          {
            FSK.client: client,
            FSK.processConfigKey: processConfigKey,
            FSK.processName: processName,
            FSK.userEmail: JetsRouterDelegate().user.email,
          }
        ],
      }, toEncodable: (_) => '');
      var navigator = Navigator.of(context);
      // First delete existing rule config triples
      var deleteResult = await context.read<HttpClient>().sendRequest(
          path: ServerEPs.dataTableEP,
          token: JetsRouterDelegate().user.token,
          encodedJsonBody: deleteJsonBody);

      if (deleteResult.statusCode == 200) {
        // now insert the new triples
        var insertJsonBody = jsonEncode(<String, dynamic>{
          'action': 'insert_rows',
          'table': 'rule_config',
          'data': stateList.getRange(0, stateList.length - 1).toList(),
        }, toEncodable: (_) => '');
        postInsertRows(context, formState, insertJsonBody);
        // insert successfull
        // trigger a refresh of the rule_config table
        formState.parentFormState?.setValue(0, FSK.processName, null);
        formState.parentFormState
            ?.setValueAndNotify(0, FSK.processName, processName);
      } else if (deleteResult.statusCode == 400 ||
          deleteResult.statusCode == 406 ||
          deleteResult.statusCode == 422) {
        // http Bad Request / Not Acceptable / Unprocessable
        formState.setValue(
            0, FSK.serverError, "Something went wrong. Please try again.");
        navigator.pop(DTActionResult.statusError);
      } else {
        formState.setValue(
            0, FSK.serverError, "Got a server error. Please try again.");
        navigator.pop(DTActionResult.statusError);
      }
      break;

    // delete rule config triple
    case ActionKeys.ruleConfigDelete:
      var style = formState.getValue(group, ActionKeys.ruleConfigDelete);
      assert(style is ActionStyle?,
          'error: invalid formState value for ActionKeys.ruleConfigDelete');
      if (style == ActionStyle.alternate) {
        formState.setValueAndNotify(
            group, ActionKeys.ruleConfigDelete, ActionStyle.danger);
      } else {
        var altInputFields =
            formState.activeFormWidgetState?.alternateInputFields;
        assert(altInputFields != null);
        if (altInputFields == null) return;
        altInputFields.removeAt(group);
        for (var i = group; i < altInputFields.length; i++) {
          for (var j = 0; j < altInputFields[i].length; j++) {
            altInputFields[i][j].group = i;
          }
        }
        //* PROBLEM - Need to stop using group 0 as a special group with validation keys
        //  since removing group 0 creates a problem
        if (group == 0) {
          // Need to carry over context keys
          var processConfigKey = formState.getValue(0, FSK.processConfigKey);
          var processName = formState.getValue(0, FSK.processName);
          var client = formState.getValue(0, FSK.client);
          formState.setValue(1, FSK.client, client);
          formState.setValue(1, FSK.processConfigKey, processConfigKey);
          formState.setValue(1, FSK.processName, processName);
          formState.setValue(1, FSK.userEmail, JetsRouterDelegate().user.email);
        }
        formState.removeValidationGroup(group);
        formState.activeFormWidgetState?.markAsDirty();
        print("OK row with index $group should be deleted");
      }
      break;

    case ActionKeys.ruleConfigAdd:
      var index = formState.groupCount - 1;
      formState.resizeFormState(formState.groupCount + 1);
      formState.activeFormWidgetState?.alternateInputFields.insert(index, [
        FormInputFieldConfig(
            key: FSK.subject,
            label: 'Subject',
            hint: 'Rule config subject',
            group: index,
            flex: 2,
            autovalidateMode: AutovalidateMode.always,
            autofocus: false,
            textRestriction: TextRestriction.none,
            maxLength: 512),
        FormInputFieldConfig(
            key: FSK.predicate,
            label: 'Predicate',
            hint: 'Rule config predicate',
            group: index,
            flex: 2,
            autovalidateMode: AutovalidateMode.always,
            autofocus: false,
            textRestriction: TextRestriction.none,
            maxLength: 512),
        FormInputFieldConfig(
            key: FSK.object,
            label: 'Object',
            hint: 'Rule config object',
            group: index,
            flex: 2,
            autovalidateMode: AutovalidateMode.always,
            autofocus: false,
            textRestriction: TextRestriction.none,
            maxLength: 512),
        FormDropdownFieldConfig(
            key: FSK.rdfType,
            group: index,
            flex: 1,
            autovalidateMode: AutovalidateMode.always,
            items: FormDropdownFieldConfig.rdfDropdownItems,
            defaultItemPos: 0),
        FormActionConfig(
            key: ActionKeys.ruleConfigDelete,
            group: index,
            flex: 1,
            label: '',
            labelByStyle: {
              ActionStyle.alternate: 'Delete',
              ActionStyle.danger: 'Confirm',
            },
            buttonStyle: ActionStyle.alternate,
            leftMargin: defaultPadding,
            rightMargin: defaultPadding),
      ]);
      formState.activeFormWidgetState?.markAsDirty();
      break;

    case ActionKeys.dialogCancel:
      Navigator.of(context).pop();
      break;

    default:
      print(
          'Oops unknown ActionKey for process and rules config form: $actionKey');
  }
}

/// Pipeline Config Form / Dialog Validator
String? pipelineConfigFormValidator(
    JetsFormState formState, int group, String key, dynamic v) {
  print(
      "Validator Called for $group, $key, $v, state is ${formState.getValue(group, key)}");
  assert((v is String?) || (v is List<String>?),
      "Pipeline Config Form has unexpected data type");
  switch (key) {
    // Pipeline Config Dialog Validation
    case FSK.processName:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        return null;
      }
      return "Process name must be provided.";

    case FSK.client:
      String? value = v;
      if (value != null && value.isNotEmpty) {
        return null;
      }
      return "Client must be provided.";

    case FSK.mainProcessInputKey:
      if (v != null && v.isNotEmpty) {
        return null;
      }
      return "Main process input must be selected.";

    case FSK.mergedProcessInputKeys:
      return null;

    default:
      print(
          'Oops process / rules config form has no validator configured for form field $key');
  }
  return null;
}

/// Pipeline Config Form Actions
void pipelineConfigFormActions(BuildContext context,
    GlobalKey<FormState> formKey, JetsFormState formState, String actionKey,
    {group = 0}) async {
  switch (actionKey) {
    // Pipeline Config Dialog
    case ActionKeys.pipelineConfigOk:
      var valid = formKey.currentState!.validate();
      if (!valid) {
        return;
      }

      // Get the Pipeline Config key (case update)
      // if no key is present then it's an add
      var updateState = <String, dynamic>{};
      var updateKey = formState.getValue(0, FSK.key);
      var query = 'pipeline_config'; // case add
      if (updateKey != null) {
        query = 'update/pipeline_config';
        updateState[FSK.key] = updateKey;
      }
      var processName = formState.getValue(0, FSK.processName);
      updateState[FSK.processName] = processName;
      updateState[FSK.client] = formState.getValue(0, FSK.client);

      // add process_config_key based on process_name
      var processConfigCache =
          formState.getCacheValue(FSK.processConfigCache) as List?;
      if (processConfigCache == null) {
        print("pipelineConfigFormActions error: processConfigCache is null");
        return;
      }
      var row = processConfigCache.firstWhere((e) => e[0] == processName);
      if (row == null) {
        print(
            "pipelineConfigFormActions error: can't find process_name in processConfigCache");
        return;
      }
      updateState[FSK.processConfigKey] = row[1];

      // mainProcessInputKey & main_table_name are either pre-populated as String
      // from the data table action
      // from the selected row to update or is a List<String?> if user have selected
      // a row from the data table
      var mainProcessInputKey = formState.getValue(0, FSK.mainProcessInputKey);
      assert(mainProcessInputKey != null, "unexpected null value");
      if (mainProcessInputKey is List<String?>) {
        updateState[FSK.mainProcessInputKey] = mainProcessInputKey[0];
      } else {
        updateState[FSK.mainProcessInputKey] = mainProcessInputKey;
      }
      var mainTableName = formState.getValue(0, FSK.mainTableName);
      assert(mainTableName != null, "unexpected null value");
      if (mainTableName is List<String?>) {
        updateState[FSK.mainTableName] = mainTableName[0];
      } else {
        updateState[FSK.mainTableName] = mainTableName;
      }
      // same pattern for merged_process_input_keys
      var mergedProcessInputKeys =
          formState.getValue(0, FSK.mergedProcessInputKeys);
      if (mergedProcessInputKeys != null) {
        assert(mergedProcessInputKeys is List<String>, "Unexpected type");
        final buf = StringBuffer();
        buf.write("{");
        buf.writeAll(mergedProcessInputKeys, ",");
        buf.write("}");
        updateState[FSK.mergedProcessInputKeys] = buf.toString();
      }
      updateState[FSK.description] = formState.getValue(0, FSK.description);
      updateState[FSK.userEmail] = JetsRouterDelegate().user.email;

      var encodedJsonBody = jsonEncode(<String, dynamic>{
        'action': 'insert_rows',
        'table': query,
        'data': [updateState],
      }, toEncodable: (_) => '');
      postInsertRows(context, formState, encodedJsonBody);
      break;

    case ActionKeys.dialogCancel:
      Navigator.of(context).pop();
      break;
    default:
      print('Oops unknown ActionKey for pipeline config form: $actionKey');
  }
}