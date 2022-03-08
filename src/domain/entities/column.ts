import { Dependency } from '../value-types/dependency';

export interface ColumnProperties {
  id: string;
  name: string;
  tableId: string;
  upstreamDependencies: Dependency[];
  downstreamDependencies: Dependency[];
}

export class Column {
  #id: string;

  #name: string;

  #tableId: string;

  #upstreamDependencies: Dependency[];
  #downstreamDependencies: Dependency[];

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get tableId(): string {
    return this.#tableId;
  }

  get upstreamDependencies(): Dependency[] {
    return this.#upstreamDependencies;
  }

  get downstreamDependencies(): Dependency[] {
    return this.#downstreamDependencies;
  }

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#tableId = properties.tableId;
    this.#upstreamDependencies = properties.upstreamDependencies;
    this.#downstreamDependencies = properties.downstreamDependencies;
  }

  static create(properties: ColumnProperties): Column {
    if (!properties.id) throw new TypeError('Column must have id');
    if (!properties.name) throw new TypeError('Column must have name');
    if (!properties.tableId) throw new TypeError('Column must have tableId');

    const column = new Column(properties);

    return column;
  }
}
