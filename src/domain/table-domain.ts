import { ReadTable } from './table/read-table';

export default class TableDomain {
  #readTable: ReadTable;

  get readTable(): ReadTable {
    return this.#readTable;
  }

  constructor(readTable: ReadTable) {
    this.#readTable = readTable;
  }
}
