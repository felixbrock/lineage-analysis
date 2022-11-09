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
  type: MaterializationType;
  lineageIds: string[];
  organizationId: string;
  logicId?: string ;
  ownerId?: string ;
  isTransient?: boolean ;
  comment?: string ;
}

export interface MaterializationPrototype {
  id: string;
  relationName: string;
  databaseName: string;
  schemaName: string;
  name: string;
  type: MaterializationType;
  lineageId: string;
  organizationId: string;
  logicId?: string;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
}

export type MaterializationDto = MaterializationProperties;

export class Materialization {
  #id: string;

  #relationName: string;

  #name: string;

  #schemaName: string;

  #databaseName: string;

  #type: MaterializationType;

  #lineageIds: string[];

  #organizationId: string;

  #logicId: string | undefined;

  #ownerId: string | undefined;

  #isTransient: boolean | undefined;

  #comment: string | undefined;

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

  get type(): MaterializationType {
    return this.#type;
  }

  get lineageIds(): string[] {
    return this.#lineageIds;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  get logicId(): string | undefined {
    return this.#logicId;
  }

  get ownerId(): string | undefined {
    return this.#ownerId;
  }

  get isTransient(): boolean | undefined {
    return this.#isTransient;
  }

  get comment(): string | undefined {
    return this.#comment;
  }

  private constructor(props: MaterializationProperties) {
    this.#id = props.id;
    this.#relationName = props.relationName;
    this.#name = props.name;
    this.#schemaName = props.schemaName;
    this.#databaseName = props.databaseName;
    this.#type = props.type;
    this.#lineageIds = props.lineageIds;
    this.#organizationId = props.organizationId;
    this.#logicId = props.logicId;
    this.#ownerId = props.ownerId;
    this.#isTransient = props.isTransient;
    this.#comment = props.comment;
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
    if (!prototype.type)
      throw new TypeError('Materialization must have materialization type');
    if (!prototype.lineageId)
      throw new TypeError('Materialization must have lineageId');
    if (!prototype.organizationId)
      throw new TypeError('Materialization must have organization id');

    const materialization = new Materialization({
      ...prototype,
      logicId: prototype.logicId,
      ownerId: prototype.ownerId,
      isTransient: prototype.isTransient,
      comment: prototype.comment,
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
    if (!props.type)
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
    type: this.#type,
    name: this.#name,
    schemaName: this.#schemaName,
    databaseName: this.#databaseName,
    lineageIds: this.#lineageIds,
    organizationId: this.#organizationId,
    logicId: this.#logicId,
    ownerId: this.#ownerId,
    isTransient: this.#isTransient,
    comment: this.#comment,
  });
}
