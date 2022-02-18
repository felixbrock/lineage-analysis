from enum import Enum
import sqlfluff
import json

fd = open('table2.sql', 'r')
sqlFile = fd.read()
# print(sqlFile)
fd.close()

# my_bad_query = "SeLEct  *, 1, blah as  fOO  from mySchema.myTable"
# query2 = "with customers as (    select * from {{ ref('stg_customers') }}), orders as (    select * from {{ ref('stg_orders') }}),customer_orders as (   select       customer_id,        min(order_date) as first_order_date,        max(order_date) as most_recent_order_date,        count(order_id) as number_of_orders    from orders group by 1), final as (    select        customers.customer_id,        customers.first_name,        customers.last_name,        customer_orders.first_order_date,        customer_orders.most_recent_order_date,        coalesce(customer_orders.number_of_orders, 0) as number_of_orders   from customers    left join customer_orders using (customer_id))select * from final"

# fix_result_1 = sqlfluff.fix(sqlFile, dialect="snowflake")
# print(fix_result_1)

parseResult = sqlfluff.parse(sqlFile, 'snowflake')

# json = json.dumps(parse_result)
# print()




class SQLElement(Enum):
  FILE = 'file'
  STATEMENT = 'statement'

  CREATE_TABLE_STATEMENT = 'create_table_statement'

  JOIN_CLAUSE = 'join_clause'
  ODERBY_CLAUSE = 'orderby_clause'

  SELECT_CLAUSE_ELEMENT = 'select_clause_element'
  FROM_EXPRESSION_ELEMENT = 'from_expression_element'
  JOIN_ON_CONDITION = 'join_on_condition'

  TABLE_REFERENCE = 'table_reference'
  COLUMN_REFERENCE = 'column_reference'

  IDENTIFIER = 'identifier'

class LineageInformation(Enum):
  TABLE_SELF = 'table_self'
  TABLE = 'table'

  COLUMN = 'column'

  DEPENDENCY_TYPE = 'dependency_type'

  TYPE_SELECT = 'select'
  TYPE_JOIN_CONDITION  = 'join_condition'
  TYPE_ORDERBY_CLAUSE = 'oderby_clause'


class DependencyAnalyzer:
  
  def __init__(self) -> None:
      self.statementDependencies = []

  def __appendPath(self, key, path):
    if not path:
      path += key
    else:
      path += f'.{key}'
    return path

  def addStatementDependencyObj(self, statementDependencyObj):
    self.statementDependencies.append(statementDependencyObj)

  def extractStatementDependencies(self, targetKey, var, path = ''):
    if hasattr(var,'items'):
      statementDependencyObj = []
      for k0, v0 in var.items():
        if k0 == targetKey:
          path = self.__appendPath(k0, path)
          statementDependencyObj.append((path,v0))
        if isinstance(v0, dict):
          dependencies = self.extractStatementDependencies(targetKey, v0, self.__appendPath(k0, path))
          for dependency in dependencies:
            statementDependencyObj.append(dependency)
        elif isinstance(v0, list):            
          if k0 == SQLElement.COLUMN_REFERENCE.value:
            valuePath = ''
            keyPath = ''
            for element in v0:
              dependencies = self.extractStatementDependencies(targetKey, element, self.__appendPath(k0, path))
              for dependency in dependencies:
                 valuePath = self.__appendPath(dependency[1], valuePath)
                 keyPath = dependency[0]
            statementDependencyObj.append((keyPath, valuePath))
          else:
            for element in v0:
              dependencies = self.extractStatementDependencies(targetKey, element, self.__appendPath(k0, path))
              for dependency in dependencies:
                statementDependencyObj.append(dependency)
      return statementDependencyObj

  def isColumnDependency(self, key):
    return SQLElement.COLUMN_REFERENCE.value in key

  def analyzeColumnDependency(self, key, value, dependencyObjIndex):

    if not (self.isColumnDependency(key)):
        return

    result = {}

    tableRef = ''
    valueRef = value

    if '.' in value:
      valuePathElements = value.split('.')
      tableRef = valuePathElements[0]
      valueRef = valuePathElements[1]

    statementDependencyObj = self.statementDependencies[dependencyObjIndex]

    if SQLElement.CREATE_TABLE_STATEMENT.value in key:
      tableSelfRef = f'{SQLElement.CREATE_TABLE_STATEMENT.value}.{SQLElement.TABLE_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'

      tableSelfSearchRes = [item for item in statementDependencyObj if tableSelfRef in item]

      if len(tableSelfSearchRes) > 1:
        raise LookupError(f'Multiple instances of {tableSelfRef} found')
      if len(tableSelfSearchRes) < 1:
        raise LookupError(f'{tableSelfRef} not found')

      result[LineageInformation.TABLE_SELF.value] = tableSelfSearchRes[0]

    if SQLElement.SELECT_CLAUSE_ELEMENT.value in key:
      if(not tableRef):
        for k,v in statementDependencyObj:
          isFromTable = all(x in k for x in [SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
          if isFromTable:
            tableRef = v
            break

      if not tableRef:
        raise LookupError(f'No table for SELECT statement found')

      result[LineageInformation.TABLE.value] = tableRef
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_SELECT.value
    elif SQLElement.JOIN_ON_CONDITION.value in key:
      if(not tableRef):
        for k,v in statementDependencyObj:
          isJoinTable = all(x in k for x in [SQLElement.JOIN_CLAUSE.value, SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
          if isJoinTable:
            tableRef = v
            break

      if not tableRef:
        raise LookupError(f'No table for JOIN statement found')

      result[LineageInformation.TABLE.value] = tableRef
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_JOIN_CONDITION.value
    elif SQLElement.ODERBY_CLAUSE.value in key:
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_ORDERBY_CLAUSE.value

    result[LineageInformation.COLUMN.value] = valueRef
    return result
      

analyzer = DependencyAnalyzer()

file = parseResult[SQLElement.FILE.value]

if isinstance(file, dict) and SQLElement.STATEMENT.value in file:
  statementDependencyObj = analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, file[SQLElement.STATEMENT.value])
  analyzer.addStatementDependencyObj(statementDependencyObj)
elif isinstance(file, list):
  for statement in file:
    if not SQLElement.STATEMENT.value in statement:
        continue
    statementDependencyObj = analyzer.extractStatementDependencies(analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, statement[SQLElement.STATEMENT.value]))
    analyzer.addStatementDependencyObj(statementDependencyObj)

lineageInfo = []

counter = 0
for statement in analyzer.statementDependencies:
  for k,v in statement:
    if not analyzer.isColumnDependency(k):
      continue

    result = analyzer.analyzeColumnDependency(k, v, counter)

    if not result:
      raise LookupError(f'No information for column reference found')

    lineageInfo.append(result)
  counter += 1




pass