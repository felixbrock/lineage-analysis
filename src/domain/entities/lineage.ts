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

  #creationState: LineageCreationState;

  #dbCoveredNames: string[];

  #diff?: string;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
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

  private constructor(props: LineageProps) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#creationState = props.creationState;
    this.#dbCoveredNames = props.dbCoveredNames;
    this.#diff = props.diff;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');

    const lineage = Lineage.build({
      ...prototype,
      createdAt: prototype.createdAt || new Date().toISOString(),
      creationState: 'pending',
      dbCoveredNames: prototype.dbCoveredNames || [],
      diff: prototype.diff,
    });

    return lineage;
  };

  static build = (props: LineageProps): Lineage =>
    new Lineage({
      id: props.id,
      createdAt: props.createdAt,
      creationState: props.creationState,
      dbCoveredNames: props.dbCoveredNames,
      diff: props.diff,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    creationState: this.#creationState,
    dbCoveredNames: this.#dbCoveredNames,
    diff: this.#diff,
  });
}
