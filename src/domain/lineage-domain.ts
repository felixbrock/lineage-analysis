import { CreateColumn } from './column/create-column';
import { ReadColumns } from './column/read-columns';
import { CreateDependency } from './dependency/create-dependency';
import { ReadDependencies } from './dependency/read-dependencies';
import { CreateLineage } from './lineage/create-lineage';
import { CreateLogic } from './logic/create-logic';
import { ReadLogics } from './logic/read-logics';
import { CreateMaterialization } from './materialization/create-materialization';
import { ReadMaterialization } from './materialization/read-materialization';
import { ReadMaterializations } from './materialization/read-materializations';

export default class LineageDomain {
  #createLineage: CreateLineage;

  #createLogic: CreateLogic;

  #createMaterialization: CreateMaterialization;

  #createColumn: CreateColumn;

  #createDependency: CreateDependency;

  #readMaterialization: ReadMaterialization;

  #readLogics: ReadLogics;

  #readMaterializations: ReadMaterializations;

  #readColumns: ReadColumns;

  #readDependencies: ReadDependencies;

  get createLineage(): CreateLineage {
    return this.#createLineage;
  }

  get createLogic(): CreateLogic {
    return this.#createLogic;
  }

  get createMaterialization(): CreateMaterialization {
    return this.#createMaterialization;
  }

  get createColumn(): CreateColumn {
    return this.#createColumn;
  }

  get createDependency(): CreateDependency {
    return this.#createDependency;
  }

  get readMaterialization(): ReadMaterialization {
    return this.#readMaterialization;
  }

  get readLogics(): ReadLogics {
    return this.#readLogics;
  }

  get readMaterializations(): ReadMaterializations {
    return this.#readMaterializations;
  }

  get readColumns(): ReadColumns {
    return this.#readColumns;
  }

  get readDependencies(): ReadDependencies {
    return this.#readDependencies;
  }

  constructor(
    createLineage: CreateLineage,
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    readMaterialization: ReadMaterialization,
    readLogics: ReadLogics,
    readMaterializations: ReadMaterializations,
    readColumns: ReadColumns,
    readDependencies: ReadDependencies
  ) {
    this.#createLineage = createLineage;
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#readMaterialization = readMaterialization;
    this.#readLogics = readLogics;
    this.#readMaterializations = readMaterializations;
    this.#readColumns = readColumns;
    this.#readDependencies = readDependencies;
  }
}
