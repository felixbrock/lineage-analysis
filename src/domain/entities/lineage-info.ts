import { SQLElement } from "../value-types/sql-element";
import { Table } from "./table";

export class LineageInfo {
  #table: Table;

  #lineageInfo: { [key: string]: string }[];

  static LineageInfoElement = {
    TABLE_SELF: 'table_self',
    TABLE: 'table',

    COLUMN: 'column',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
    TYPE_ORDERBY_CLAUSE: 'oderby_clause',
  };

  get table(): Table {
    return this.#table;
  }

  get lineageInfo(): { [key: string]: string }[] {
    return this.#lineageInfo;
  }

  #isColumnDependency = (key: string): boolean =>
    key.includes(SQLElement.COLUMN_REFERENCE);

  #analyzeColumnDependency = (
    key: string,
    value: string,
    dependencyObjIndex: number
  ) => {
    if (!this.#isColumnDependency(key)) return;

    const result: { [key: string]: string } = {};
    let tableRef = '';
    let valueRef = value;

    if (value.includes('.')) {
      const valuePathElements = value.split('.');
      tableRef = valuePathElements[0];
      valueRef = valuePathElements[1];
    }

    const statementDependencyObj =
      this.#table.statementDependencies[dependencyObjIndex];

    if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
      if (!tableRef) {
        const tableRefs = statementDependencyObj.filter((element) => 
          [
            SQLElement.FROM_EXPRESSION_ELEMENT,
            SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring))        );
        tableRef = tableRefs[0][1]
      }

      if (!tableRef)
        throw ReferenceError(`No table for SELECT statement found`);

      result[LineageInfo.LineageInfoElement.TABLE] = tableRef;
      result[LineageInfo.LineageInfoElement.DEPENDENCY_TYPE] = LineageInfo.LineageInfoElement.TYPE_SELECT;
    } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
      if (!tableRef) {
        Object.entries(statementDependencyObj).forEach((element) => {
          const isJoinTable = [
            SQLElement.JOIN_CLAUSE,
            SQLElement.FROM_EXPRESSION_ELEMENT,
            SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring));
          if (isJoinTable) {
            tableRef = value;
            return;
          }
        });

        if (!tableRef)
          throw ReferenceError(`No table for JOIN statement found`);
      }

      result[LineageInfo.LineageInfoElement.TABLE] = tableRef;
      result[LineageInfo.LineageInfoElement.DEPENDENCY_TYPE] =
        LineageInfo.LineageInfoElement.TYPE_JOIN_CONDITION;
    } else if (key.includes(SQLElement.ODERBY_CLAUSE))
      result[LineageInfo.LineageInfoElement.DEPENDENCY_TYPE] =
        LineageInfo.LineageInfoElement.TYPE_ORDERBY_CLAUSE;

    result[LineageInfo.LineageInfoElement.COLUMN] = valueRef;
    return result;
  };

  #populateLineageInfo = () => {
    let counter = 0;
    this.#table.statementDependencies.forEach((element) => {
      element
        .filter((dependency) => this.#isColumnDependency(dependency[0]))
        .forEach((dependency) => {
          const result = this.#analyzeColumnDependency(
            dependency[0],
            dependency[1],
            counter
          );

          if (!result)
            throw new ReferenceError(
              'No information for column reference found'
            );

          this.#lineageInfo.push(result);
        });
      counter += 1;
    });
  };

  private constructor(table: Table) {
    this.#table = table;
    this.#lineageInfo = [];
  }

  static create(table: Table): LineageInfo {
    if (!table) throw new TypeError('LineageInfo object requires table object');
    
    const lineageInfo = new LineageInfo(table);

    lineageInfo.#populateLineageInfo();

    return lineageInfo;
  }
}
