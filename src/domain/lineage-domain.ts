import { CreateColumn } from './column/create-column';
import { ReadColumns } from './column/read-columns';
import { CreateDependency } from './dependency/create-dependency';
import { ReadDependencies } from './dependency/read-dependencies';
import { CreateLineage } from './lineage/create-lineage';
import { CreateModel } from './model/create-model';
import { ReadModels } from './model/read-models';
import { CreateTable } from './table/create-table';
import { ReadTable } from './table/read-table';
import { ReadTables } from './table/read-tables';

export default class LineageDomain {
  #createLineage: CreateLineage;

  #createModel: CreateModel;

  #createTable: CreateTable;

  #createColumn: CreateColumn;

  #createDependency: CreateDependency;

  #readTable: ReadTable;

  #readModels: ReadModels;

  #readTables: ReadTables;

  #readColumns: ReadColumns;

  #readDependencies: ReadDependencies;

  get createLineage(): CreateLineage {
    return this.#createLineage;
  }

  get createModel(): CreateModel {
    return this.#createModel;
  }

  get createTable(): CreateTable {
    return this.#createTable;
  }

  get createColumn(): CreateColumn {
    return this.#createColumn;
  }

  get createDependency(): CreateDependency {
    return this.#createDependency;
  }

  get readTable(): ReadTable {
    return this.#readTable;
  }

  get readModels(): ReadModels {
    return this.#readModels;
  }

  get readTables(): ReadTables {
    return this.#readTables;
  }

  get readColumns(): ReadColumns {
    return this.#readColumns;
  }

  get readDependencies(): ReadDependencies {
    return this.#readDependencies;
  }

  constructor(
    createLineage: CreateLineage,
    createModel: CreateModel,
    createTable: CreateTable,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    readTable: ReadTable,
    readModels: ReadModels,
    readTables: ReadTables,
    readColumns: ReadColumns,
    readDependencies: ReadDependencies
  ) {
    this.#createLineage = createLineage;
    this.#createModel = createModel;
    this.#createTable = createTable;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#readTable = readTable;
    this.#readModels = readModels;
    this.#readTables = readTables;
    this.#readColumns = readColumns;
    this.#readDependencies = readDependencies;
  }
}
