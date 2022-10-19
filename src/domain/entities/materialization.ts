export const materializationTypes = ['base table', 'view'] as const;
export type MaterializationType = typeof materializationTypes[number];

export const parseMaterializationType = (
  materializationType: string
): MaterializationType => {
  const identifiedElement = materializationTypes.find(
    (element) => element.toLowerCase() === materializationType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

export interface MaterializationProperties {
  id: string;
  relationName: string;
  name: string;
  schemaName: string;
  databaseName: string;
  materializationType: MaterializationType;
  logicId?: string;
  lineageIds: string[];
  organizationId: string;
}

export interface MaterializationPrototype {
  id: string;
  relationName: string;
  name: string;
  schemaName: string;
  databaseName: string;
  materializationType: MaterializationType;
  logicId?: string;
  lineageId: string;
  organizationId: string;
}

export type MaterializationDto = MaterializationProperties;

export class Materialization {
  #id: string;

  #relationName: string;

  #name: string;

  #schemaName: string;

  #databaseName: string;

  #materializationType: MaterializationType;

  #logicId?: string;

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

  get schemaName(): string {
    return this.#schemaName;
  }

  get databaseName(): string {
    return this.#databaseName;
  }

  get materializationType(): MaterializationType {
    return this.#materializationType;
  }

  get logicId(): string | undefined {
    return this.#logicId;
  }

  get lineageIds(): string[] {
    return this.#lineageIds;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: MaterializationProperties) {
    this.#id = properties.id;
    this.#relationName = properties.relationName;
    this.#name = properties.name;
    this.#schemaName = properties.schemaName;
    this.#databaseName = properties.databaseName;
    this.#materializationType = properties.materializationType;
    this.#logicId = properties.logicId;
    this.#lineageIds = properties.lineageIds;
    this.#organizationId = properties.organizationId;
  }

  static create = (prototype: MaterializationPrototype): Materialization => {
    if (!prototype.id) throw new TypeError('Materialization must have id');
    if (!prototype.relationName)
      throw new TypeError('Materialization must have relationName');
    if (!prototype.name) throw new TypeError('Materialization must have name');
    if (!prototype.schemaName)
      throw new TypeError('Materialization must have schema name');
    if (!prototype.databaseName)
      throw new TypeError('Materialization must have database name');
    if (!prototype.materializationType)
      throw new TypeError('Materialization must have materialization type');
    if (!prototype.lineageId)
      throw new TypeError('Materialization must have lineageId');
    if (!prototype.organizationId)
      throw new TypeError('Materialization must have organization id');

    const materialization = new Materialization({
      ...prototype,
      lineageIds: [prototype.lineageId],
    });

    return materialization;
  };

  static build = (props: MaterializationProperties): Materialization => {
    if (!props.id) throw new TypeError('Materialization must have id');
    if (!props.relationName)
      throw new TypeError('Materialization must have relationName');
    if (!props.name) throw new TypeError('Materialization must have name');
    if (!props.schemaName)
      throw new TypeError('Materialization must have schema name');
    if (!props.databaseName)
      throw new TypeError('Materialization must have database name');
    if (!props.materializationType)
      throw new TypeError('Materialization must have materialization type');
    if (!props.lineageIds.length)
      throw new TypeError('Materialization must have lineageIds');
    if (!props.organizationId)
      throw new TypeError('Materialization must have organization id');

    return new Materialization(props);
  };

  toDto = (): MaterializationDto => ({
    id: this.#id,
    relationName: this.#relationName,
    materializationType: this.#materializationType,
    name: this.#name,
    schemaName: this.#schemaName,
    databaseName: this.#databaseName,
    logicId: this.#logicId,
    lineageIds: this.#lineageIds,
    organizationId: this.#organizationId,
  });
}
