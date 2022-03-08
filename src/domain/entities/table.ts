import { Model } from "../value-types/model";

export interface TableProperties {
  id: string,
  name: string,
  model: Model
}

export class Table {
  #id: string;

  #name: string;

  #model: Model;

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get model(): Model {
    return this.#model;
  }

  private constructor(properties: TableProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#model = properties.model;
  }

  static create(properties: TableProperties): Table {
    if (!properties.id) throw new TypeError('Table must have id');
    if (!properties.name) throw new TypeError('Table must have name');
    if (!properties.model) throw new TypeError('Table must have model');

    const table = new Table(properties);

    return table;
  }
}
