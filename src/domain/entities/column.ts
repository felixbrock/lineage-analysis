export interface ColumnProperties {
  id: string;
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageIds: string[];
  organizationId: string;
}

export interface ColumnPrototype {
  id: string;
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  organizationId: string;
}

type ColumnDto = ColumnProperties;

export class Column {
  #id: string;

  #relationName: string;

  #name: string;

  #index: string;

  #type: string;

  #materializationId: string;

  #lineageIds: string[];

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

  get lineageIds(): string[] {
    return this.#lineageIds;
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
    this.#lineageIds = properties.lineageIds;
    this.#organizationId = properties.organizationId;
  }

  static create = (prototype: ColumnPrototype): Column => {
    if (!prototype.id) throw new TypeError('Column must have id');
    if (!prototype.relationName)
      throw new TypeError('Column must have relationName');
    if (!prototype.name) throw new TypeError('Column must have name');
    if (!prototype.index) throw new TypeError('Column must have index');
    if (!prototype.type) throw new TypeError('Column must have type');
    if (!prototype.materializationId)
      throw new TypeError('Column must have materializationId');
    if (!prototype.lineageId)
      throw new TypeError('Column must have lineage id');
    if (!prototype.organizationId)
      throw new TypeError('Column must have organizationId');

    const column = Column.build({
      ...prototype,
      lineageIds: [prototype.lineageId],
    });

    return column;
  };

  static build = (props: ColumnProperties): Column => {
    if (!props.id) throw new TypeError('Column must have id');
    if (!props.relationName)
      throw new TypeError('Column must have relationName');
    if (!props.name) throw new TypeError('Column must have name');
    if (!props.index) throw new TypeError('Column must have index');
    if (!props.type) throw new TypeError('Column must have type');
    if (!props.materializationId)
      throw new TypeError('Column must have materializationId');
    if (!props.lineageIds.length)
      throw new TypeError('Column must have lineage ids');
    if (!props.organizationId)
      throw new TypeError('Column must have organizationId');

    return new Column(props);
  };

  toDto = (): ColumnDto => ({
    id: this.#id,
    relationName: this.#relationName,
    name: this.#name,
    index: this.#index,
    type: this.#type,
    materializationId: this.#materializationId,
    lineageIds: this.#lineageIds,
    organizationId: this.#organizationId,
  });
}
