"""JetRuleValidator tests"""

import sys
import json
from typing import Dict
from absl import flags
from absl.testing import absltest
import io

from jetrule_compiler import JetRuleCompiler, InputProvider
from jetrule_context import JetRuleContext

FLAGS = flags.FLAGS

class JetRulesCompilerTest(absltest.TestCase):

  def _get_from_file(self, fname: str) -> JetRuleContext:
    in_provider = InputProvider('test_data')
    compiler = JetRuleCompiler()
    compiler.compileJetRuleFile(fname, in_provider)
    # print('Compiler working memory for import files')
    # print(compiler.imported_file_info_list)
    # print('***')
    return compiler.jetrule_ctx

  def _get_augmented_data(self, input_data: str) -> JetRuleContext:
    compiler = JetRuleCompiler()
    compiler.compileJetRule(input_data)
    jetrule_ctx = compiler.jetrule_ctx
    return jetrule_ctx


  def test_import1(self):
    jetrule_ctx = self._get_from_file("import_test1.jr")

    self.assertEqual(jetrule_ctx.ERROR, False)

    # validate the whole result
    expected = """{"resources": [{"type": "int", "id": "isTrue", "value": "1", "source_file_name": "import_test1.jr", "key": 0}, {"type": "text", "id": "NOT_IN_CONTRACT", "value": "NOT COVERED IN CONTRACT", "source_file_name": "import_test1.jr", "key": 1}, {"type": "text", "id": "EXCLUDED_STATE", "value": "STATE", "source_file_name": "import_test1.jr", "key": 2}, {"id": "acme:ProcedureLookup", "type": "resource", "value": "acme:ProcedureLookup", "source_file_name": "import_test11.jr", "key": 3}, {"id": "cPROC_RID", "type": "resource", "value": "PROC_RID", "source_file_name": "import_test11.jr", "key": 4}, {"id": "cPROC_MID", "type": "resource", "value": "PROC_MID", "source_file_name": "import_test11.jr", "key": 5}, {"id": "cPROC_DESC", "type": "resource", "value": "PROC_DESC", "source_file_name": "import_test11.jr", "key": 6}], "lookup_tables": [{"name": "acme:ProcedureLookup", "table": "acme__cm_proc_codes", "key": ["PROC_CODE"], "columns": ["PROC_RID", "PROC_MID", "PROC_DESC"], "source_file_name": "import_test11.jr", "resources": ["cPROC_RID", "cPROC_MID", "cPROC_DESC"]}], "jet_rules": [], "imports": {"import_test1.jr": ["import_test11.jr"]}}"""
    # print('GOT:',json.dumps(jetrule_ctx.jetRules, indent=2))
    # print()
    # print('COMPACT:',json.dumps(jetrule_ctx.jetRules))

    # validate the whole result
    self.assertEqual(json.dumps(jetrule_ctx.jetRules), expected)

  def test_import2(self):
    jetrule_ctx = self._get_from_file("import_test2.jr")

    # for err in jetrule_ctx.errors:
    #   print('2***', err)
    # print('2***')
    
    self.assertEqual(jetrule_ctx.ERROR, True)
    self.assertEqual(jetrule_ctx.errors[0], "Error in file 'import_test21.jr' line 8:19 no viable alternative at input 'acme:lookup_table'")
    self.assertEqual(jetrule_ctx.errors[1], "Error in file 'import_test2.jr' line 5:1 extraneous input 'bad' expecting {<EOF>, '[', '@JetCompilerDirective', 'triple', 'int', 'uint', 'long', 'ulong', 'double', 'text', 'date', 'datetime', 'resource', 'volatile_resource', 'lookup_table', COMMENT}")
    self.assertEqual(len(jetrule_ctx.errors), 2)

  def test_import3(self):
    jetrule_ctx = self._get_from_file("import_test3.jr")

    # for err in jetrule_ctx.errors:
    #   print('3***', err)
    # print('3***')
    
    self.assertEqual(jetrule_ctx.ERROR, True)

    self.assertEqual(jetrule_ctx.errors[0], "Error in file 'import_test3.jr' line 8:5 mismatched input 'true' expecting Identifier")
    self.assertEqual(jetrule_ctx.errors[1], "Error in file 'import_test31.jr' line 7:10 mismatched input 'lookup_table' expecting Identifier")
    self.assertEqual(jetrule_ctx.errors[2], "Error in file 'import_test32.jr' line 5:8 mismatched input ':' expecting {',', ']'}")
    self.assertEqual(jetrule_ctx.errors[3], "Error in file 'import_test32.jr' line 9:1 extraneous input ';' expecting {<EOF>, '[', '@JetCompilerDirective', 'triple', 'int', 'uint', 'long', 'ulong', 'double', 'text', 'date', 'datetime', 'resource', 'volatile_resource', 'lookup_table', COMMENT}")
    self.assertEqual(jetrule_ctx.errors[4], "Error in file 'import_test3.jr' line 16:1 extraneous input 'ztext' expecting {<EOF>, '[', '@JetCompilerDirective', 'triple', 'int', 'uint', 'long', 'ulong', 'double', 'text', 'date', 'datetime', 'resource', 'volatile_resource', 'lookup_table', COMMENT}")

    self.assertEqual(len(jetrule_ctx.errors), 5)


  def test_import4(self):
    jetrule_ctx = self._get_from_file("import_test4.jr")

    # for err in jetrule_ctx.errors:
    #   print('4***', err)
    # print('4***')

    self.assertEqual(jetrule_ctx.ERROR, True)

    self.assertEqual(jetrule_ctx.errors[0], "Error in file 'import_test4.jr' line 8:5 mismatched input 'true' expecting Identifier")
    self.assertEqual(jetrule_ctx.errors[1], "Error in file 'import_test41.jr' line 8:19 no viable alternative at input 'acme:lookup_table'")
    self.assertEqual(jetrule_ctx.errors[2], "Error in file 'import_test42.jr' line 7:10 mismatched input 'lookup_table' expecting Identifier")
    self.assertEqual(jetrule_ctx.errors[3], "Error in file 'import_test4.jr' line 17:1 extraneous input 'ztext' expecting {<EOF>, '[', '@JetCompilerDirective', 'triple', 'int', 'uint', 'long', 'ulong', 'double', 'text', 'date', 'datetime', 'resource', 'volatile_resource', 'lookup_table', COMMENT}")

    self.assertEqual(len(jetrule_ctx.errors), 4)


  def test_compiler1(self):
    data = """
      # =======================================================================================
      resource rdf:type = "rdf:type";
      resource acme:Claim = "acme:Claim";
      volatile_resource is_good = "is_good";
      [RuleC4]: 
        (?clm01 rdf:type acme:Claim).[?clm01]
        (?clm01 is_good ?good).[?clm01 or true]
        ->
        (?clm01 is_good true).
      ;
    """
    jetrule_ctx = self._get_augmented_data(data)

    # Validate variables
    self.assertEqual(len(jetrule_ctx.errors), 0)

    optimized_data = jetrule_ctx.jetRules
    optimized_expected = """{"resources": [{"type": "resource", "id": "rdf:type", "value": "rdf:type", "key": 0}, {"type": "resource", "id": "acme:Claim", "value": "acme:Claim", "key": 1}, {"type": "volatile_resource", "id": "is_good", "value": "is_good", "key": 2}, {"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 3, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 1, "is_antecedent": false, "var_pos": 0, "key": 4, "source_file_name": null}, {"type": "keyword", "value": "true", "inline": true, "key": 5, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 1, "is_antecedent": false, "var_pos": 0, "key": 6, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": true, "var_pos": 0, "key": 7, "source_file_name": null}, {"type": "var", "id": "?x2", "is_binded": false, "vertex": 2, "key": 8, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": false, "var_pos": 0, "key": 9, "source_file_name": null}, {"type": "keyword", "value": "true", "inline": true, "key": 10, "source_file_name": null}], "lookup_tables": [], "jet_rules": [{"name": "RuleC4", "properties": {}, "optimization": true, "salience": 100, "antecedents": [{"type": "antecedent", "isNot": false, "triple": [{"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 3, "source_file_name": null}, {"type": "identifier", "value": "rdf:type"}, {"type": "identifier", "value": "acme:Claim"}], "normalizedLabel": "(?x1 rdf:type acme:Claim).[(?x1 or true) and ?x1]", "label": "(?clm01 rdf:type acme:Claim).[(?clm01 or true) and ?clm01]", "filter": {"type": "binary", "lhs": {"type": "binary", "lhs": 4, "op": "or", "rhs": 5}, "op": "and", "rhs": 6}, "vertex": 1, "parent_vertex": 0}, {"type": "antecedent", "isNot": false, "triple": [{"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": true, "var_pos": 0, "key": 7, "source_file_name": null}, {"type": "identifier", "value": "is_good"}, {"type": "var", "id": "?x2", "is_binded": false, "vertex": 2, "key": 8, "source_file_name": null}], "normalizedLabel": "(?x1 is_good ?x2)", "label": "(?clm01 is_good ?good)", "vertex": 2, "parent_vertex": 1}], "consequents": [{"type": "consequent", "triple": [{"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": false, "var_pos": 0, "key": 9, "source_file_name": null}, {"type": "identifier", "value": "is_good"}, {"type": "keyword", "value": "true", "inline": true, "key": 10, "source_file_name": null}], "normalizedLabel": "(?x1 is_good true)", "label": "(?clm01 is_good true)", "vertex": 2}], "authoredLabel": "[RuleC4]:(?clm01 rdf:type acme:Claim).[?clm01].(?clm01 is_good ?good).[?clm01 or true] -> (?clm01 is_good true);", "source_file_name": null, "normalizedLabel": "[RuleC4]:(?x1 rdf:type acme:Claim).[(?x1 or true) and ?x1].(?x1 is_good ?x2) -> (?x1 is_good true);", "label": "[RuleC4]:(?clm01 rdf:type acme:Claim).[(?clm01 or true) and ?clm01].(?clm01 is_good ?good) -> (?clm01 is_good true);"}], "imports": {}}"""
    # print()
    # print('OPTIMIZED GOT:',json.dumps(optimized_data, indent=2))
    # print()
    # print('COMPACT:',json.dumps(optimized_data))
    self.assertEqual(json.dumps(optimized_data), optimized_expected)

  def test_compiler2(self):
    data = """
      # =======================================================================================
      resource rdf:type = "rdf:type";
      resource acme:Claim = "acme:Claim";
      volatile_resource is_good = "is_good";
      [RuleC5]: 
        (?clm01 reverse_of ?clm02).
        (?clm03 rdf:type acme:Claim).
        (?clm01 rdf:type acme:Claim).
        (?clm02 rdf:type acme:Claim)
        ->
        (?clm01 is_good true).
      ;
    """
    jetrule_ctx = self._get_augmented_data(data)

    # if jetrule_ctx.ERROR:
    #   print("GOT ERROR!")
    # for err in jetrule_ctx.errors:
    #   print('***', err)
    # print('***')

    self.assertEqual(jetrule_ctx.ERROR, True)
    self.assertEqual(jetrule_ctx.errors[0], "Error rule RuleC5: Identifier 'reverse_of' is not defined in this context '(?clm01 reverse_of ?clm02)', it must be defined.")

  def test_specialcase1(self):
    data = """
      # =======================================================================================
      resource rdf:type = "rdf:type";
      resource acme:Claim = "acme:Claim";
      resource acme:EClaim = "acme:EClaim";
      volatile_resource is_good = "is_good";
      volatile_resource related_to = "related_to";
      [RuleSC1]: 
        (?clm01 rdf:type acme:Claim).
        (?clm01 is_good ?good).[?good].
        (?clm01 related_to ?clm02)
        ->
        (?clm01 rdf:type acme:EClaim).
        (?clm02 rdf:type acme:EClaim)
      ;
    """
    jetrule_ctx = self._get_augmented_data(data)

    if jetrule_ctx.ERROR:
      print("GOT ERROR!")
    for err in jetrule_ctx.errors:
      print('***', err)
    # print('***')

    expected = """{"main_rule_file_name": null, "support_rule_file_names": null, "resources": [{"type": "resource", "id": "rdf:type", "value": "rdf:type", "key": 0}, {"type": "resource", "id": "acme:Claim", "value": "acme:Claim", "key": 1}, {"type": "resource", "id": "acme:EClaim", "value": "acme:EClaim", "key": 2}, {"type": "volatile_resource", "id": "is_good", "value": "is_good", "key": 3}, {"type": "volatile_resource", "id": "related_to", "value": "related_to", "key": 4}, {"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 5, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": true, "var_pos": 0, "key": 6, "source_file_name": null}, {"type": "var", "id": "?x2", "is_binded": false, "var_pos": 2, "vertex": 2, "key": 7, "source_file_name": null}, {"type": "var", "id": "?x2", "is_binded": true, "vertex": 2, "is_antecedent": false, "var_pos": 1, "key": 8, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": true, "var_pos": 0, "key": 9, "source_file_name": null}, {"type": "var", "id": "?x3", "is_binded": false, "var_pos": 2, "vertex": 3, "key": 10, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": false, "var_pos": 0, "key": 11, "source_file_name": null}, {"type": "var", "id": "?x3", "is_binded": true, "vertex": 3, "is_antecedent": false, "var_pos": 1, "key": 12, "source_file_name": null}], "lookup_tables": [], "rete_nodes": [{"vertex": 0, "parent_vertex": 0, "children_vertexes": [1], "type": "head_node"}, {"type": "antecedent", "isNot": false, "normalizedLabel": "(?x1 rdf:type acme:Claim)", "vertex": 1, "parent_vertex": 0, "beta_relation_vars": ["?x1"], "pruned_var": [], "beta_var_nodes": [{"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 5, "source_file_name": null}], "children_vertexes": [2], "subject_key": 5, "predicate_key": 0, "object_key": 1}, {"type": "antecedent", "isNot": false, "normalizedLabel": "(?x1 is_good ?x2).[?x2]", "filter": 8, "vertex": 2, "parent_vertex": 1, "beta_relation_vars": ["?x1", "?x2"], "pruned_var": [], "beta_var_nodes": [{"type": "var", "id": "?x1", "is_binded": true, "var_pos": 0, "vertex": 2}, {"type": "var", "id": "?x2", "is_binded": false, "var_pos": 2, "vertex": 2, "key": 7, "source_file_name": null}], "children_vertexes": [3], "subject_key": 6, "predicate_key": 3, "object_key": 7}, {"type": "antecedent", "isNot": false, "normalizedLabel": "(?x1 related_to ?x3)", "vertex": 3, "parent_vertex": 2, "beta_relation_vars": ["?x1", "?x3"], "pruned_var": ["?x2"], "beta_var_nodes": [{"type": "var", "id": "?x1", "is_binded": true, "var_pos": 0, "vertex": 3}, {"type": "var", "id": "?x3", "is_binded": false, "var_pos": 2, "vertex": 3, "key": 10, "source_file_name": null}], "children_vertexes": [], "rules": ["RuleSC1"], "salience": [100], "subject_key": 9, "predicate_key": 4, "object_key": 10}, {"type": "consequent", "normalizedLabel": "(?x1 rdf:type acme:EClaim)", "vertex": 3, "consequent_seq": 0, "consequent_for_rule": "RuleSC1", "consequent_salience": 100, "subject_key": 11, "predicate_key": 0, "object_key": 2}, {"type": "consequent", "normalizedLabel": "(?x3 rdf:type acme:EClaim)", "vertex": 3, "consequent_seq": 1, "consequent_for_rule": "RuleSC1", "consequent_salience": 100, "subject_key": 12, "predicate_key": 0, "object_key": 2}]}"""
    # print('GOT:',json.dumps(jetrule_ctx.jetReteNodes, indent=4))
    # print()
    # print('COMPACT:',json.dumps(jetrule_ctx.jetReteNodes))
    self.assertEqual(jetrule_ctx.ERROR, False)
    self.assertEqual(json.dumps(jetrule_ctx.jetReteNodes), expected)

  def test_resource_from_rule1(self):
    data = """
      # =======================================================================================
      # Defining resources from rules
      @JetCompilerDirective extract_resources_from_rules = "true";
      [RuleRFR1]: 
        (?clm01 rdf:type acme:Claim).
        (?clm01 _0:good ?good).[?good + _0:good].
        (?clm01 related_to ?clm02)
        ->
        (?clm01 rdf:type acme:EClaim)
      ;
    """
    jetrule_ctx = self._get_augmented_data(data)

    if jetrule_ctx.ERROR:
      print("GOT ERROR!")
    for err in jetrule_ctx.errors:
      print('***', err)

    expected = """{"resources": [{"id": "rdf:type", "type": "resource", "value": "rdf:type", "key": 0}, {"id": "acme:Claim", "type": "resource", "value": "acme:Claim", "key": 1}, {"id": "good", "type": "volatile_resource", "value": "_0:good", "key": 2}, {"id": "related_to", "type": "resource", "value": "related_to", "key": 3}, {"id": "acme:EClaim", "type": "resource", "value": "acme:EClaim", "key": 4}, {"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 5, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": true, "var_pos": 0, "key": 6, "source_file_name": null}, {"type": "var", "id": "?x2", "is_binded": false, "var_pos": 2, "vertex": 2, "key": 7, "source_file_name": null}, {"type": "var", "id": "?x2", "is_binded": true, "vertex": 2, "is_antecedent": false, "var_pos": 1, "key": 8, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": true, "var_pos": 0, "key": 9, "source_file_name": null}, {"type": "var", "id": "?x3", "is_binded": false, "vertex": 3, "key": 10, "source_file_name": null}, {"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": false, "var_pos": 0, "key": 11, "source_file_name": null}], "lookup_tables": [], "jet_rules": [{"name": "RuleRFR1", "properties": {}, "optimization": true, "salience": 100, "antecedents": [{"type": "antecedent", "isNot": false, "triple": [{"type": "var", "id": "?x1", "is_binded": false, "var_pos": 0, "vertex": 1, "key": 5, "source_file_name": null}, {"type": "identifier", "value": "rdf:type"}, {"type": "identifier", "value": "acme:Claim"}], "normalizedLabel": "(?x1 rdf:type acme:Claim)", "label": "(?clm01 rdf:type acme:Claim)", "vertex": 1, "parent_vertex": 0}, {"type": "antecedent", "isNot": false, "triple": [{"type": "var", "id": "?x1", "is_binded": true, "vertex": 2, "is_antecedent": true, "var_pos": 0, "key": 6, "source_file_name": null}, {"type": "identifier", "value": "good"}, {"type": "var", "id": "?x2", "is_binded": false, "var_pos": 2, "vertex": 2, "key": 7, "source_file_name": null}], "normalizedLabel": "(?x1 good ?x2).[?x2 + good]", "label": "(?clm01 good ?good).[?good + good]", "filter": {"type": "binary", "lhs": 8, "op": "+", "rhs": 2}, "vertex": 2, "parent_vertex": 1}, {"type": "antecedent", "isNot": false, "triple": [{"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": true, "var_pos": 0, "key": 9, "source_file_name": null}, {"type": "identifier", "value": "related_to"}, {"type": "var", "id": "?x3", "is_binded": false, "vertex": 3, "key": 10, "source_file_name": null}], "normalizedLabel": "(?x1 related_to ?x3)", "label": "(?clm01 related_to ?clm02)", "vertex": 3, "parent_vertex": 2}], "consequents": [{"type": "consequent", "triple": [{"type": "var", "id": "?x1", "is_binded": true, "vertex": 3, "is_antecedent": false, "var_pos": 0, "key": 11, "source_file_name": null}, {"type": "identifier", "value": "rdf:type"}, {"type": "identifier", "value": "acme:EClaim"}], "normalizedLabel": "(?x1 rdf:type acme:EClaim)", "label": "(?clm01 rdf:type acme:EClaim)", "vertex": 3}], "authoredLabel": "[RuleRFR1]:(?clm01 rdf:type acme:Claim).(?clm01 _0:good ?good).[?good + _0:good].(?clm01 related_to ?clm02) -> (?clm01 rdf:type acme:EClaim);", "source_file_name": null, "normalizedLabel": "[RuleRFR1]:(?x1 rdf:type acme:Claim).(?x1 good ?x2).[?x2 + good].(?x1 related_to ?x3) -> (?x1 rdf:type acme:EClaim);", "label": "[RuleRFR1]:(?clm01 rdf:type acme:Claim).(?clm01 good ?good).[?good + good].(?clm01 related_to ?clm02) -> (?clm01 rdf:type acme:EClaim);"}], "imports": {}}"""
    # print('GOT:',json.dumps(jetrule_ctx.jetRules, indent=4))
    # print()
    # print('COMPACT:',json.dumps(jetrule_ctx.jetRules))
    self.assertEqual(jetrule_ctx.ERROR, False)
    self.assertEqual(json.dumps(jetrule_ctx.jetRules), expected)


if __name__ == '__main__':
  absltest.main()