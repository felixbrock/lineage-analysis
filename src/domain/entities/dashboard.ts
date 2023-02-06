export interface DashboardProps {
  id: string;
  url?: string;
  name?: string;
}

export interface DashboardPrototype {
  id: string;
  url?: string;
  name?: string;
}

type DashboardDto = DashboardProps;

export class Dashboard {
  #url?: string;

  #name?: string;

  #id: string;

  get url(): string | undefined {
    return this.#url;
  }

  get name(): string | undefined {
    return this.#name;
  }

  get id(): string {
    return this.#id;
  }

  private constructor(properties: DashboardProps) {
    this.#url = properties.url;
    this.#name = properties.name;
    this.#id = properties.id;
  }

  static create = (prototype: DashboardPrototype): Dashboard => {
    if (!prototype.id) throw new TypeError('Dashboard must have id');

    const dashboard = new Dashboard({
      ...prototype,
    });

    return dashboard;
  };

  static build = (props: DashboardProps): Dashboard => {
    if (!props.id) throw new TypeError('Dashboard must have id');

    return new Dashboard(props);
  };

  toDto = (): DashboardDto => ({
    url: this.#url,
    name: this.#name,
    id: this.#id,
  });
}
