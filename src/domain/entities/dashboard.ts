export interface DashboardProps {
  id: string;
  url?: string;
  name?: string;
  materializationName: string;
  columnName: string;
  columnId: string;
  materializationId: string;
}

export interface DashboardPrototype {
  id: string;
  url?: string;
  name?: string;
  materializationName: string;
  columnName: string;
  columnId: string;
  materializationId: string;
}

type DashboardDto = DashboardProps;

export class Dashboard {
  #url?: string;

  #name?: string;

  #materializationName: string;

  #materializationId: string;

  #columnName: string;

  #columnId: string;

  #id: string;

  get url(): string | undefined {
    return this.#url;
  }

  get name(): string | undefined {
    return this.#name;
  }

  get materializationName(): string {
    return this.#materializationName;
  }

  get columnName(): string {
    return this.#columnName;
  }

  get id(): string {
    return this.#id;
  }

  get columnId(): string {
    return this.#columnId;
  }

  get materializationId(): string {
    return this.#materializationId;
  }


  private constructor(properties: DashboardProps) {
    this.#url = properties.url;
    this.#name = properties.name;
    this.#materializationName = properties.materializationName;
    this.#columnName = properties.columnName;
    this.#id = properties.id;
    this.#columnId = properties.columnId;
    this.#materializationId = properties.materializationId;
  }

  static create = (prototype: DashboardPrototype): Dashboard => {
    if (!prototype.materializationName)
      throw new TypeError('Dashboard must have materialisation');
    if (!prototype.columnName)
      throw new TypeError('Dashboard must have column');
    if (!prototype.id) throw new TypeError('Dashboard must have id');
    if (!prototype.columnId)
      throw new TypeError('Dashboard must have columnId');
    if (!prototype.materializationId)
      throw new TypeError('Dashboard must have materializationId');

    const dashboard = new Dashboard({
      ...prototype,
    });

    return dashboard;
  };

  static build = (props: DashboardProps): Dashboard => {
    if (!props.materializationName)
      throw new TypeError('Dashboard must have materialisation');
    if (!props.columnName) throw new TypeError('Dashboard must have column');
    if (!props.id) throw new TypeError('Dashboard must have id');
    if (!props.columnId) throw new TypeError('Dashboard must have columnId');
    if (!props.materializationId)
      throw new TypeError('Dashboard must have materializationId');

    return new Dashboard(props);
  };

  toDto = (): DashboardDto => ({
    url: this.#url,
    name: this.#name,
    materializationName: this.#materializationName,
    columnName: this.#columnName,
    id: this.#id,
    columnId: this.#columnId,
    materializationId: this.#materializationId,
  });
}
