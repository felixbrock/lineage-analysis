export interface ColumnProperties {
  id: string;
  dbtModelId: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
}

export class Column {
  #id: string;

  #dbtModelId: string;

  #name: string;

  #index: string;

  #type: string;

  #materializationId: string;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get dbtModelId(): string {
    return this.#dbtModelId;
  }

  get name(): string {
    return this.#name;
  }

  get index(): string {
    return this.#index;
  }

  get type(): string {
    return this.#type;
  }

  get materializationId(): string {
    return this.#materializationId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#dbtModelId = properties.dbtModelId;
    this.#name = properties.name;
    this.#index = properties.index;
    this.#type = properties.type;
    this.#materializationId = properties.materializationId;
    this.#lineageId = properties.lineageId;
  }

  static create = (properties: ColumnProperties): Column => {
    if (!properties.id) throw new TypeError('Column must have id');
    if (!properties.dbtModelId)
      throw new TypeError('Column must have dbtModelId');
    if (!properties.name) throw new TypeError('Column must have name');
    if (!properties.index) throw new TypeError('Column must have index');
    if (!properties.type) throw new TypeError('Column must have type');
    if (!properties.materializationId)
      throw new TypeError('Column must have materializationId');
    if (!properties.lineageId)
      throw new TypeError('Column must have lineage version');

    const column = new Column({
      id: properties.id,
      dbtModelId: properties.dbtModelId,
      name: properties.name,
      index: properties.index,
      type: properties.type,
      materializationId: properties.materializationId,
      lineageId: properties.lineageId,
    });

    return column;
  };
}
