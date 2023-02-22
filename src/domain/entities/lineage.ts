export interface LineagePrototype {
  id: string;
  createdAt?: string;
  diff?: string;
  dbCoveredNames?: string[];
}

export interface LineageProps extends Omit<LineagePrototype, 'createdAt'> {
  createdAt: string;
  creationState: LineageCreationState;
  dbCoveredNames: string[];
  diff?: string;
}

type LineageDto = LineageProps;

export const lineagecreationstateTypes = [
  'pending',
  'wh-resources-done',
  'internal-lineage-done',
  'completed',
] as const;
export type LineageCreationState = typeof lineagecreationstateTypes[number];

export const parseLineageCreationState = (
  creationState: unknown
): LineageCreationState => {
  if (typeof creationState !== 'string')
    throw new Error('Provision of invalid type');
  const identifiedElement = lineagecreationstateTypes.find(
    (element) => element.toLowerCase() === creationState.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

export class Lineage {
  #id: string;

  #createdAt: string;

  #dbCoveredNames: string[];

  #diff?: string;

  #creationState: LineageCreationState;

  get id(): string {
    return this.#id;
  }

  get creationState(): LineageCreationState {
    return this.#creationState;
  }

  get dbCoveredNames(): string[] {
    return this.#dbCoveredNames;
  }

  get diff(): string | undefined {
    return this.#diff;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  private constructor(props: LineageProps) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#dbCoveredNames = props.dbCoveredNames;
    this.#diff = props.diff;
    this.#creationState = props.creationState;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');

    const lineage = Lineage.build({
      ...prototype,
      createdAt: prototype.createdAt || new Date().toISOString(),
      dbCoveredNames: prototype.dbCoveredNames || [],
      diff: prototype.diff,
      creationState: 'pending',
    });

    return lineage;
  };

  static build = (props: LineageProps): Lineage =>
    new Lineage({
      id: props.id,
      createdAt: props.createdAt,
      dbCoveredNames: props.dbCoveredNames,
      diff: props.diff,
      creationState: props.creationState,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    dbCoveredNames: this.#dbCoveredNames,
    diff: this.#diff,
    creationState: this.#creationState,
  });
}
