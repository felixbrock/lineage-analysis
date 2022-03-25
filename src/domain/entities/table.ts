export interface TableProperties {
  id: string;
  name: string;
  modelId: string;
  lineageId: string;
}

export class Table {
  #id: string;

  #name: string;

  #modelId: string;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get modelId(): string {
    return this.#modelId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: TableProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#modelId = properties.modelId;
    this.#lineageId = properties.lineageId;
  }

  static create(properties: TableProperties): Table {
    if (!properties.id) throw new TypeError('Table must have id');
    if (!properties.name) throw new TypeError('Table must have name');
    if (!properties.modelId) throw new TypeError('Table must have modelId');
    if (!properties.lineageId) throw new TypeError('Table must have lineageId');

    const table = new Table(properties);

    return table;
  }
}
