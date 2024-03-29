export const columnDataTypes = [
  'number',
  'decimal',
  'numeric',
  'int',
  'integer',
  'bigint',
  'smallint',
  'tinyint',
  'byteint',
  'float',
  'float4',
  'float8',
  'double',
  'double precision',
  'real',
  'varchar',
  'character',
  'char',
  'string',
  'text',
  'binary',
  'varbinary',
  'boolean',
  'date',
  'datetime',
  'time',
  'timestamp',
  'timestamp_ltz',
  'timestamp_ntz',
  'timestamp_tz',
  'variant',
  'object',
  'array',
  'geography',
] as const;
export type ColumnDataType = typeof columnDataTypes[number];

export const parseColumnDataType = (columnDataType: string): ColumnDataType => {
  const identifiedElement = columnDataTypes.find(
    (element) => element.toLowerCase() === columnDataType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};
export interface ColumnProps {
  id: string;
  relationName: string;
  name: string;
  index: string;
  dataType: ColumnDataType;
  materializationId: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

export interface ColumnPrototype {
  id: string;
  relationName: string;
  name: string;
  index: string;
  dataType: ColumnDataType;
  materializationId: string;
  isIdentity?: boolean;
  isNullable?: boolean;
  comment?: string;
}

type ColumnDto = ColumnProps;

export class Column {
  #id: string;

  #relationName: string;

  #name: string;

  #index: string;

  #dataType: ColumnDataType;

  #materializationId: string;

  #isIdentity?: boolean;

  #isNullable?: boolean;

  #comment?: string;

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

  get dataType(): string {
    return this.#dataType;
  }

  get materializationId(): string {
    return this.#materializationId;
  }

  get isIdentity(): boolean | undefined {
    return this.#isIdentity;
  }

  get isNullable(): boolean | undefined {
    return this.#isNullable;
  }

  get comment(): string | undefined {
    return this.#comment;
  }

  private constructor(props: ColumnProps) {
    this.#id = props.id;
    this.#relationName = props.relationName;
    this.#name = props.name;
    this.#index = props.index;
    this.#dataType = props.dataType;
    this.#materializationId = props.materializationId;
    this.#isIdentity = props.isIdentity;
    this.#isNullable = props.isNullable;
    this.#comment = props.comment;
  }

  static create = (prototype: ColumnPrototype): Column => {
    if (!prototype.id) throw new TypeError('Column must have id');
    if (!prototype.relationName)
      throw new TypeError('Column must have relationName');
    if (!prototype.name) throw new TypeError('Column must have name');
    if (!prototype.index) throw new TypeError('Column must have index');
    if (!prototype.dataType) throw new TypeError('Column must have type');
    if (!prototype.materializationId)
      throw new TypeError('Column must have materializationId');

      const column = Column.build({
      ...prototype,
      isIdentity: prototype.isIdentity,
      isNullable: prototype.isNullable,
      comment: prototype.comment,
    });

    return column;
  };

  static build = (props: ColumnProps): Column => {
    if (!props.id) throw new TypeError('Column must have id');
    if (!props.relationName)
      throw new TypeError('Column must have relationName');
    if (!props.name) throw new TypeError('Column must have name');
    if (!props.index) throw new TypeError('Column must have index');
    if (!props.dataType) throw new TypeError('Column must have type');
    if (!props.materializationId)
      throw new TypeError('Column must have materializationId');

    return new Column(props);
  };

  toDto = (): ColumnDto => ({
    id: this.#id,
    relationName: this.#relationName,
    name: this.#name,
    index: this.#index,
    dataType: this.#dataType,
    materializationId: this.#materializationId,
    isIdentity: this.#isIdentity,
    isNullable: this.#isNullable,
    comment: this.#comment,
  });
}
