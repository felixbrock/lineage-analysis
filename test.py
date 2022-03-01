from enum import Enum
import sqlfluff
import json

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

class Table():
  def __init__(self, identifier) -> None:
      self.identifier = identifier
      self.__analyzer = self.DependencyAnalyzer()
  
  def readSQL(self):
    # TODO - REPLACE
    fd = open(self.identifier, 'r')
    sqlFile = fd.read()
    fd.close()

    return sqlFile

  def parseSQL(self, sql, dialect):
    return sqlfluff.parse(sql, dialect)

  def getStatementDependencies(self, fileObj):

    # TODO - Build into solution
    if isinstance(fileObj, dict) and SQLElement.STATEMENT.value in fileObj:
      statementDependencyObj = self.__analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, fileObj[SQLElement.STATEMENT.value])
      self.__analyzer.addStatementDependencyObj(statementDependencyObj)
    elif isinstance(fileObj, list):
      for statement in fileObj:
        if not SQLElement.STATEMENT.value in statement:
            continue
        statementDependencyObj = self.__analyzer.extractStatementDependencies(self.__analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, statement[SQLElement.STATEMENT.value]))
        self.__analyzer.addStatementDependencyObj(statementDependencyObj)

    return self.__analyzer.statementDependencies

  def analyzeColumnDependencies(self, dependencies):
    lineageInfo = []

    counter = 0
    for statementDependency in dependencies:
      for k,v in statementDependency:
        if not self.__analyzer.isColumnDependency(k):
          continue

        result = self.__analyzer.analyzeColumnDependency(k, v, counter)

        if not result:
          raise LookupError(f'No information for column reference found')

        lineageInfo.append(result)
      counter += 1

    return lineageInfo


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

        tableSelfSearchRes = [item[1] for item in statementDependencyObj if tableSelfRef in item]

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

tableSelf = Table('table2.sql')

sql = tableSelf.readSQL()

parseResult = tableSelf.parseSQL(sql, 'snowflake')

fileObj = parseResult[SQLElement.FILE.value]

statementDependencies = tableSelf.getStatementDependencies(fileObj)

dependencyAnalysisResult = tableSelf.analyzeColumnDependencies(statementDependencies)







pass