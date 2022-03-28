import { Dependency, DependencyProperties } from '../value-types/dependency';

interface ColumnProperties {
  id: string;
  name: string;
  tableId: string;
  dependencies: Dependency[];
  lineageId: string;
}

export interface ColumnPrototype {
  id: string;
  name: string;
  tableId: string;
  dependencyPrototypes: DependencyProperties[];
  lineageId: string;
}

export class Column {
  #id: string;

  #name: string;

  #tableId: string;

  #dependencies: Dependency[];

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get tableId(): string {
    return this.#tableId;
  }

  get dependencies(): Dependency[] {
    return this.#dependencies;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#tableId = properties.tableId;
    this.#dependencies = properties.dependencies;
    this.#lineageId = properties.lineageId;
  }

  static create(prototype: ColumnPrototype): Column {
    if (!prototype.id) throw new TypeError('Column must have id');
    if (!prototype.name) throw new TypeError('Column must have name');
    if (!prototype.tableId) throw new TypeError('Column must have tableId');
    if (!prototype.lineageId)
      throw new TypeError('Column must have lineage version');

    const dependencies = prototype.dependencyPrototypes.map((element) =>
      Dependency.create(element)
    );

    const column = new Column({
      id: prototype.id,
      name: prototype.name,
      tableId: prototype.tableId,
      dependencies,
      lineageId: prototype.lineageId,
    });

    return column;
  }
}
