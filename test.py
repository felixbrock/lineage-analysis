from enum import Enum
from webbrowser import get
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
      self.__identifier = identifier
      self.__name = None
      self.__columns = []
      self.__parents = []
      self.__statementDependencies = []
      self.__lineageInfo = []
      self.__analyzer = self.DependencyAnalyzer(self)
  
  @property
  def statementDependencies(self):
      return self.__statementDependencies

  @property
  def lineageInfo(self):
      return self.__lineageInfo

  @property
  def name(self):
      return self.__name

  # @lineageInfo.setter
  # def lineageInfo(self, value):
  #     self._lineageInfo = value

  def __addStatementDependencyObj(self, statementDependencyObj):
    self.__statementDependencies.append(statementDependencyObj)

  def __readSQL(self):
    # TODO - REPLACE
    fd = open(self.__identifier, 'r')
    sqlFile = fd.read()
    fd.close()

    return sqlFile

  def __parseSQL(self, sql, dialect):
    return sqlfluff.parse(sql, dialect)

  def populate(self, dialect):
    sql = self.__readSQL()

    parseResult = self.__parseSQL(sql, dialect)

    fileObj = parseResult[SQLElement.FILE.value]

    self.__getStatementDependencies(fileObj)

    self.__name = self.__analyzer.getTableName()

    # TODO - read columns

    # TODO - get parents

    self.__getLineageInfo()

    # TODO - resolve analysis result to properties

  def __getStatementDependencies(self, fileObj):

    # TODO - Build into solution
    if isinstance(fileObj, dict) and SQLElement.STATEMENT.value in fileObj:
      statementDependencyObj = self.__analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, fileObj[SQLElement.STATEMENT.value])
      self.__addStatementDependencyObj(statementDependencyObj)
    elif isinstance(fileObj, list):
      for statement in fileObj:
        if not SQLElement.STATEMENT.value in statement:
            continue
        statementDependencyObj = self.__analyzer.extractStatementDependencies(self.__analyzer.extractStatementDependencies(SQLElement.IDENTIFIER.value, statement[SQLElement.STATEMENT.value]))
        self.__addStatementDependencyObj(statementDependencyObj)

  def __getLineageInfo(self):
    counter = 0
    for statementDependency in self.__statementDependencies:
      for k,v in statementDependency:
        if not self.__analyzer.isColumnDependency(k):
          continue

        result = self.__analyzer.analyzeColumnDependency(k, v, counter)

        if not result:
          raise LookupError(f'No information for column reference found')

        self.__lineageInfo.append(result)
      counter += 1


  class DependencyAnalyzer():

    def __init__(self, table) -> None:
      self.__table = table

    def __appendPath(self, key, path):
      if not path:
        path += key
      else:
        path += f'.{key}'
      return path



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

    def __flatten(self, t):
      return [item for sublist in t for item in sublist]

    def getTableName(self):
      tableSelfRef = f'{SQLElement.CREATE_TABLE_STATEMENT.value}.{SQLElement.TABLE_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'

      tableSelfSearchRes = [item[1] for item in self.__flatten(self.__table.statementDependencies) if tableSelfRef in item]

      if len(tableSelfSearchRes) > 1:
        raise LookupError(f'Multiple instances of {tableSelfRef} found')
      if len(tableSelfSearchRes) < 1:
        raise LookupError(f'{tableSelfRef} not found')

      return tableSelfSearchRes[0]

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

      statementDependencyObj = self.__table.statementDependencies[dependencyObjIndex]

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

tableSelf.populate('snowflake')




pass