import fs from 'fs';
import { SQLElement } from "../value-types/sql-element";

export interface TableProperties {
  id: string,
  name: string,
  columns: string[],
  parentNames: string[],
  statementDependencies: [string, string][][]
}

export class Table {
  #id: string;

  #name: string;

  #columns: string[];

  #parentNames: string[];

  #statementDependencies: [string, string][][];

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get columns(): string[] {
    return this.#columns;
  }

  get parentNames(): string[] {
    return this.#parentNames;
  }

  get statementDependencies(): [string, string][][] {
    return this.#statementDependencies;
  }

  private constructor(properties: TableProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#columns = properties.columns;
    this.#parentNames = properties.parentNames;
    this.#statementDependencies = properties.statementDependencies;
  }

  static create(properties: TableProperties): Table {
    if (!properties.id) throw new TypeError('Table must have id');
    if (!properties.name) throw new TypeError('Table must have name');

    const table = new Table(properties);

    return table;
  }
}
