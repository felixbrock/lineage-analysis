export interface DashboardProperties {
  url?: string;
  name?: string;
  materializationName: string;
  columnName: string;
  id: string;
  lineageIds: string[];
  columnId: string;
  materializationId: string;
  organizationId: string;
}

type DashboardDto = DashboardProperties;

export class Dashboard {
  #url?: string;

  #name?: string;

  #materializationName: string;

  #materializationId: string;

  #columnName: string;

  #columnId: string;

  #id: string;

  #lineageIds: string[];

  #organizationId: string;

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

  get lineageIds(): string[] {
    return this.#lineageIds;
  }

  get columnId(): string {
    return this.#columnId;
  }

  get materializationId(): string {
    return this.#materializationId;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: DashboardProperties) {
    this.#url = properties.url;
    this.#name = properties.name;
    this.#materializationName = properties.materializationName;
    this.#columnName = properties.columnName;
    this.#id = properties.id;
    this.#lineageIds = properties.lineageIds;
    this.#columnId = properties.columnId;
    this.#materializationId = properties.materializationId;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: DashboardProperties): Dashboard => {
    if (!properties.materializationName)
      throw new TypeError('Dashboard must have materialisation');
    if (!properties.columnName)
      throw new TypeError('Dashboard must have column');
    if (!properties.id) throw new TypeError('Dashboard must have id');
    if (!properties.lineageIds.length)
      throw new TypeError('Dashboard must have lineageId');
    if (!properties.columnId)
      throw new TypeError('Dashboard must have columnId');
    if (!properties.materializationId)
      throw new TypeError('Dashboard must have materializationId');
    if (!properties.organizationId)
      throw new TypeError('Dashboard must have organizationId');

    const dashboard = new Dashboard(properties);

    return dashboard;
  };

  toDto = (): DashboardDto => ({
    url: this.#url,
    name: this.#name,
    materializationName: this.#materializationName,
    columnName: this.#columnName,
    id: this.#id,
    lineageIds: this.#lineageIds,
    columnId: this.#columnId,
    materializationId: this.#materializationId,
    organizationId: this.#organizationId,
  });
}
