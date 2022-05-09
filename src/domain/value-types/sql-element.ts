export default class SQLElement {
  static FILE = 'file';

  static STATEMENT = 'statement';

  static CREATE_TABLE_STATEMENT = 'create_table_statement';

  static INSERT_STATEMENT = 'insert_statement';

  static SELECT_STATEMENT = 'select_statement';

  static JOIN_CLAUSE = 'join_clause';

  static SELECT_CLAUSE_ELEMENT = 'select_clause_element';

  static FROM_EXPRESSION_ELEMENT = 'from_expression_element';

  static JOIN_ON_CONDITION = 'join_on_condition';

  static TABLE_EXPRESSION = 'table_expression';

  static TABLE_REFERENCE = 'table_reference';

  static COLUMN_REFERENCE = 'column_reference';

  static COLUMN_DEFINITION = 'column_definition';

  static IDENTIFIER = 'identifier';

  static WILDCARD_EXPRESSION = 'wildcard_expression';

  static WILDCARD_IDENTIFIER = 'wildcard_identifier';

  static WILDCARD_IDENTIFIER_DOT = 'dot';

  static WILDCARD_IDENTIFIER_IDENTIFIER = 'identifier';

  static WILDCARD_IDENTIFIER_STAR = 'star';

  static KEYWORD = 'keyword';

  static KEYWORD_AS = 'as';

  static SET_EXPRESSION = 'set_expression';

  static COMMON_TABLE_EXPRESSION = 'common_table_expression';

  static WITH_COMPOUND_STATEMENT = 'with_compound_statement';

  static ALIAS_EXPRESSION = 'alias_expression';

  static GROUPBY_CLAUSE = 'groupby_clause';

  static FUNCTION = 'function';

  static BRACKETED = 'bracketed';

  static EXPRESSION = 'expression';

  static LITERAL = 'literal';

  static BARE_FUNCTION = 'bare_function';
}
