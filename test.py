# import sqlfluff
# import json

# fd = open('table2.sql', 'r')
# sqlFile = fd.read()
# # print(sqlFile)
# fd.close()

# # my_bad_query = "SeLEct  *, 1, blah as  fOO  from mySchema.myTable"
# # query2 = "with customers as (    select * from {{ ref('stg_customers') }}), orders as (    select * from {{ ref('stg_orders') }}),customer_orders as (   select       customer_id,        min(order_date) as first_order_date,        max(order_date) as most_recent_order_date,        count(order_id) as number_of_orders    from orders group by 1), final as (    select        customers.customer_id,        customers.first_name,        customers.last_name,        customer_orders.first_order_date,        customer_orders.most_recent_order_date,        coalesce(customer_orders.number_of_orders, 0) as number_of_orders   from customers    left join customer_orders using (customer_id))select * from final"

# # fix_result_1 = sqlfluff.fix(sqlFile, dialect="snowflake")
# # print(fix_result_1)

# parseResult = sqlfluff.parse(sqlFile, 'snowflake')

# # json = json.dumps(parse_result)
# # print()

# def appendPath(key, path):
#   if not path:
#     path += key
#   else:
#     path += f'.{key}'
#   return path

# def dictionaryExtract(key, var, path = ''):
#   if hasattr(var,'items'):
#     results = []
#     for k, v in var.items():
#       if k == key:
#         path = appendPath(k, path)
#         results.append((path, v))
#       if isinstance(v, dict):
#         for result in dictionaryExtract(key, v, appendPath(k, path)):
#           results.append(result)
#       elif isinstance(v, list):
#         for d in v:
#           for result in dictionaryExtract(key, d, appendPath(k, path)):
#             results.append(result)
#     return results

# # enum ReferencingTypes ={]}

# class ReferenceAnalyzer:
#   def __init__(self, referencingStatements) -> None:
#       self.referencingStatements = referencingStatements

#   def analyze(self, key, statementIndex):
#     createTableStatement = 'create_table_statement'

#     match key:
#       case createTableStatement:
#         referencingStatement = self.referencingStatements[statementIndex]
#         tableToCreate = self.referencingStatements
#         print('table to create')

# referencingStatements = []

# file = parseResult['file']
# statementKey = 'statement'
# identifierKey = 'identifier'

# if isinstance(file, dict) and statementKey in file:
#   referencingStatements.append(dictionaryExtract(identifierKey, file[statementKey]))
# elif isinstance(file, list):
#   for statement in file:
#     if not statementKey in statement:
#         continue
#     referencingStatements.append(dictionaryExtract(identifierKey, statement[statementKey]))

# print(referencingStatements)

from enum import Enum
from itertools import count
from tkinter.tix import COLUMN
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

def appendPath(key, path):
  if not path:
    path += key
  else:
    path += f'.{key}'
  return path


# def dictionaryExtract(key, var, path = ''):
#   if hasattr(var,'items'):
#     results = []
#     for k, v in var.items():
#       if k == key:
#         path = appendPath(k, path)
#         results.append((path, v))
#       if isinstance(v, dict):
#         for result in dictionaryExtract(key, v, appendPath(k, path)):
#           results.append(result)
#       elif isinstance(v, list):
#         for d in v:
#           for result in dictionaryExtract(key, d, appendPath(k, path)):
#             results.append(result)
#     return results

def dictionaryExtract(key, var, path = ''):
  if hasattr(var,'items'):
    results = {}
    for k0, v0 in var.items():
      if k0 == key:
        path = appendPath(k0, path)
        results[path] = v0
      if isinstance(v0, dict):
        for k1,v1 in dictionaryExtract(key, v0, appendPath(k0, path)).items():
          results[k1] = v1
      elif isinstance(v0, list):
        for d in v0:
          for k2, v2 in dictionaryExtract(key, d, appendPath(k0, path)).items():
            results[k2] = v2
    return results

# enum ReferencingTypes ={]}

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
  TABLE_FROM = 'table_from'
  TABLE_JOIN = 'table_join'

  COLUMN = 'column'

  DEPENDENCY_TYPE = 'dependency_type'

  TYPE_SELECT = 'select'
  TYPE_JOIN_CONDITION  = 'join_condition'
  TYPE_ORDERBY_CLAUSE = 'oderby_clause'


class DependencyAnalyzer:
  def __init__(self, referencingStatements) -> None:
      self.referencingStatements = referencingStatements

  def isColumnReference(self, key):
    return SQLElement.COLUMN_REFERENCE.value in key

  # def __analyze(self, key, statementIndex):



  #         # tableToCreateRef = f'{SQLElement.CREATE_TABLE_STATEMENT}.{SQLElement.TABLE_REFERENCE}.{SQLElement.IDENTIFIER}'
  #         # if not tableToCreateRef in referencingStatement:
  #         #   raise LookupError(f'{tableToCreateRef} not found')

  #         # return {LineageInformation.TABLE_TO_CREATE: self.referencingStatements[tableToCreateRef]}

  def analyzeColumnDependency(self, key, statementIndex):

    if not (self.isColumnReference(key)):
        return

    # lineageInfo = {}

    # result = self.__analyze(key, statementIndex)

    referencingStatement = self.referencingStatements[statementIndex]

    result = {}

    if SQLElement.CREATE_TABLE_STATEMENT.value in key:
      tableToCreateRef = f'{SQLElement.CREATE_TABLE_STATEMENT.value}.{SQLElement.TABLE_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'
      if not tableToCreateRef in referencingStatement:
        raise LookupError(f'{tableToCreateRef} not found')

      result[LineageInformation.TABLE_SELF.value] = referencingStatement[tableToCreateRef]

    if SQLElement.SELECT_CLAUSE_ELEMENT.value in key:
      fromTable = ''
      for k,v in referencingStatement.items():
        isFromTable = all(x in k for x in [SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
        if isFromTable:
          fromTable = v
          break

      if not fromTable:
        raise LookupError(f'No table for SELECT statement found')

      result[LineageInformation.TABLE_FROM.value] = fromTable
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_SELECT.value
    elif SQLElement.JOIN_ON_CONDITION.value in key:
      joinTable = ''
      for k,v in referencingStatement.items():
        isJoinTable = all(x in k for x in [SQLElement.JOIN_CLAUSE.value, SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
        if isJoinTable:
          joinTable = v
          break

      if not joinTable:
        raise LookupError(f'No table for JOIN statement found')

      result[LineageInformation.TABLE_JOIN.value] = joinTable
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_JOIN_CONDITION.value
    elif SQLElement.ODERBY_CLAUSE.value in key:
      result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_ORDERBY_CLAUSE.value
        
    return result
      

referencingStatements = []

file = parseResult[SQLElement.FILE.value]

if isinstance(file, dict) and SQLElement.STATEMENT.value in file:
  referencingStatements.append(dictionaryExtract(SQLElement.IDENTIFIER.value, file[SQLElement.STATEMENT.value]))
elif isinstance(file, list):
  for statement in file:
    if not SQLElement.STATEMENT.value in statement:
        continue
    referencingStatements.append(dictionaryExtract(SQLElement.IDENTIFIER.value, statement[SQLElement.STATEMENT.value]))

print(referencingStatements)

analyzer = DependencyAnalyzer(referencingStatements)

lineageInfo = []

counter = 0
for statement in referencingStatements:
  for k,v in statement.items():
    if not analyzer.isColumnReference(k):
      continue

    result = analyzer.analyzeColumnDependency(k, counter)

    if not result:
      raise LookupError(f'No information for column reference found')

    result[LineageInformation.COLUMN.value] = v

    lineageInfo.append(result)
  counter += 1




pass