export default class DependencyAnalyzer{

  #appendPath = (key: string, path: string) : string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath; 
  };
    
  public extractStatementDependencies(targetKey: string, parsedSQL: {[key: string]: string}, path = '') : [string, string][] {
    const statementDependencyObj = [];
    
    for (let key in parsedSQL){
      const value = parsedSQL[key];
      if (key === targetKey) statementDependencyObj.push([this.#appendPath(key, path), value])
    } 
    
    }

      
    
    
  
}



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

// def isColumnDependency(self, key):
//   return SQLElement.COLUMN_REFERENCE.value in key

// def __flatten(self, t):
//   return [item for sublist in t for item in sublist]

// def getTableName(self):
//   tableSelfRef = f'{SQLElement.CREATE_TABLE_STATEMENT.value}.{SQLElement.TABLE_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'

//   tableSelfSearchRes = [item[1] for item in self.__flatten(self.__table.statementDependencies) if tableSelfRef in item]

//   if len(tableSelfSearchRes) > 1:
//     raise LookupError(f'Multiple instances of {tableSelfRef} found')
//   if len(tableSelfSearchRes) < 1:
//     raise LookupError(f'{tableSelfRef} not found')

//   return tableSelfSearchRes[0]

// def getTableColumns(self):
//   columnSelfRef = f'{SQLElement.SELECT_CLAUSE_ELEMENT.value}.{SQLElement.COLUMN_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'

//   return [item[1] for item in self.__flatten(self.__table.statementDependencies) if columnSelfRef in item[0]]

// def getParentTableNames(self):
//   tableSelfRef = f'{SQLElement.CREATE_TABLE_STATEMENT.value}.{SQLElement.TABLE_REFERENCE.value}.{SQLElement.IDENTIFIER.value}'

//   return [item[1] for item in self.__flatten(self.__table.statementDependencies) if not tableSelfRef in item and SQLElement.TABLE_REFERENCE.value in item[0]]

// def analyzeColumnDependency(self, key, value, dependencyObjIndex):

//   if not (self.isColumnDependency(key)):
//       return

//   result = {}

//   tableRef = ''
//   valueRef = value

//   if '.' in value:
//     valuePathElements = value.split('.')
//     tableRef = valuePathElements[0]
//     valueRef = valuePathElements[1]

//   statementDependencyObj = self.__table.statementDependencies[dependencyObjIndex]

//   if SQLElement.SELECT_CLAUSE_ELEMENT.value in key:
//     if(not tableRef):
//       for k,v in statementDependencyObj:
//         isFromTable = all(x in k for x in [SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
//         if isFromTable:
//           tableRef = v
//           break

//     if not tableRef:
//       raise LookupError(f'No table for SELECT statement found')

//     result[LineageInformation.TABLE.value] = tableRef
//     result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_SELECT.value
//   elif SQLElement.JOIN_ON_CONDITION.value in key:
//     if(not tableRef):
//       for k,v in statementDependencyObj:
//         isJoinTable = all(x in k for x in [SQLElement.JOIN_CLAUSE.value, SQLElement.FROM_EXPRESSION_ELEMENT.value, SQLElement.TABLE_REFERENCE.value])
//         if isJoinTable:
//           tableRef = v
//           break

//     if not tableRef:
//       raise LookupError(f'No table for JOIN statement found')

//     result[LineageInformation.TABLE.value] = tableRef
//     result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_JOIN_CONDITION.value
//   elif SQLElement.ODERBY_CLAUSE.value in key:
//     result[LineageInformation.DEPENDENCY_TYPE.value] = LineageInformation.TYPE_ORDERBY_CLAUSE.value

//   result[LineageInformation.COLUMN.value] = valueRef
//   return result
