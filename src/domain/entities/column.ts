export interface ColumnProperties {
  id: string;
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  organizationId: string;
}

export class Column {
  #id: string;

  #relationName: string;

  #name: string;

  #index: string;

  #type: string;

  #materializationId: string;

  #lineageId: string;

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get relationName(): string {
    return this.#relationName;
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

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: ColumnProperties) {
    this.#id = properties.id;
    this.#relationName = properties.relationName;
    this.#name = properties.name;
    this.#index = properties.index;
    this.#type = properties.type;
    this.#materializationId = properties.materializationId;
    this.#lineageId = properties.lineageId;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: ColumnProperties): Column => {
    if (!properties.id) throw new TypeError('Column must have id');
    if (!properties.relationName)
      throw new TypeError('Column must have relationName');
    if (!properties.name) throw new TypeError('Column must have name');
    if (!properties.index) throw new TypeError('Column must have index');
    if (!properties.type) throw new TypeError('Column must have type');
    if (!properties.materializationId)
      throw new TypeError('Column must have materializationId');
    if (!properties.lineageId)
      throw new TypeError('Column must have lineage id');
    if (!properties.organizationId)
      throw new TypeError('Column must have organizationId');

    const column = new Column({
      id: properties.id,
      relationName: properties.relationName,
      name: properties.name,
      index: properties.index,
      type: properties.type,
      materializationId: properties.materializationId,
      lineageId: properties.lineageId,
      organizationId: properties.organizationId
    });

    return column;
  };
}
