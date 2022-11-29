export interface LineagePrototype {
  id: string;
  createdAt?: string;
  diff?: string;
}

export interface LineageProps extends Omit<LineagePrototype, 'createdAt'> {
  createdAt: string;
  completed: boolean;
  diff?: string;
}

type LineageDto = LineageProps;

export class Lineage {
  #id: string;

  #createdAt: string;

  #completed: boolean;

  #diff?: string;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  get completed(): boolean {
    return this.#completed;
  }

  get diff(): string | undefined {
    return this.#diff;
  }

  private constructor(props: LineageProps) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#completed = props.completed;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');

    const lineage = Lineage.build({
      ...prototype,
      createdAt: prototype.createdAt || new Date().toISOString(),
      completed: false,
      diff: prototype.diff,
    });

    return lineage;
  };

  static build = (props: LineageProps): Lineage =>
    new Lineage({
      id: props.id,
      createdAt: props.createdAt,
      completed: props.completed,
      diff: props.diff,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    completed: this.#completed,
    diff: this.#diff,
  });
}
