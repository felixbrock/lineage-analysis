export enum MaterializationType {
  TABLE = 'BASE TABLE',
  VIEW = 'VIEW',
}

export interface MaterializationProperties {
  id: string;
  modelId: string;
  name: string;
  schemaName: string;
  databaseName: string;
  materializationType: MaterializationType;
  logicId: string;
  lineageId: string;
  organizationId: string; 
}

export class Materialization {
  #id: string;

  #modelId: string;

  #name: string;

  #schemaName: string;

  #databaseName: string;

  #materializationType: MaterializationType;

  #logicId: string;

  #lineageId: string;

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get modelId(): string {
    return this.#modelId;
  }

  get name(): string {
    return this.#name;
  }

  get schemaName(): string {
    return this.#schemaName;
  }

  get databaseName(): string {
    return this.#databaseName;
  }

  get materializationType(): MaterializationType {
    return this.#materializationType;
  }

  get logicId(): string {
    return this.#logicId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: MaterializationProperties) {
    this.#id = properties.id;
    this.#modelId = properties.modelId;
    this.#name = properties.name;
    this.#schemaName = properties.schemaName;
    this.#databaseName = properties.databaseName;
    this.#materializationType = properties.materializationType;
    this.#logicId = properties.logicId;
    this.#lineageId = properties.lineageId;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: MaterializationProperties): Materialization => {
    if (!properties.id) throw new TypeError('Materialization must have id');
    if (!properties.modelId)
      throw new TypeError('Materialization must have modelId');
    if (!properties.name) throw new TypeError('Materialization must have name');
    if (!properties.schemaName)
      throw new TypeError('Materialization must have schema name');
    if (!properties.databaseName)
      throw new TypeError('Materialization must have database name');
    if (!properties.materializationType)
      throw new TypeError('Materialization must have materialization type');
    if (!properties.logicId) throw new TypeError('Materialization must have logicId');
    if (!properties.lineageId) throw new TypeError('Materialization must have lineageId');
    if (!properties.organizationId) throw new TypeError('Materialization must have organization id');

    const materialization = new Materialization(properties);

    return materialization;
  };
}
