import { Column } from "../../entities/column";
import { Table } from "../../entities/table";

export interface LineageProperties {
  table: Table;
  columns: Column[];
}

export class Lineage {
  #table: Table;
  #columns: Column[];

  get table(): Table {
    return this.#table;
  }

  get columns(): Column[] {
    return this.#columns;
  }

  private constructor(properties: LineageProperties) {
    this.#table = properties.table;
    this.#columns = properties.columns;
  }

  static create(properties: LineageProperties): Lineage {
    if (!properties.table)
      throw new TypeError('Lineage object must have table');
    if (!properties.columns)
      throw new TypeError('Lineage object must have columns');

    const lineage = new Lineage(properties);

    return lineage;
  }
}
