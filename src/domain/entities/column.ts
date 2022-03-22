import { DependencyDto } from '../column/dependency-dto';
import { Dependency } from '../value-types/dependency';

export interface ColumnProperties {
  id: string;
  name: string;
  tableId: string;
  dependencies: DependencyDto[];
}

export class Column {
  #id: string;

  #name: string;

  #tableId: string;

  #dependencies: Dependency[];

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

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#tableId = properties.tableId;
    this.#dependencies = properties.dependencies.map((dependency) =>
      Dependency.create({
        type: dependency.type,
        columnId: dependency.columnId,
        direction: dependency.direction,
      })
    );
  }

  static create(properties: ColumnProperties): Column {
    if (!properties.id) throw new TypeError('Column must have id');
    if (!properties.name) throw new TypeError('Column must have name');
    if (!properties.tableId) throw new TypeError('Column must have tableId');

    const column = new Column(properties);

    return column;
  }
}
