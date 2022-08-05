export interface DashboardProperties {
    url?: string;
    name?: string;
    materializationName: string;
    columnName: string; 
    id: string;
    lineageId: string;
    columnId: string,
    materializationId: string;
  }

export class Dashboard {
    
    #url?: string;
    
    #name?: string;
    
    #materializationName: string;

    #materializationId: string;
    
    #columnName: string;

    #columnId: string;

    #id: string;
  
    #lineageId: string;
    
    
    get url(): string|undefined {
        return this.#url;
    }

    get name(): string|undefined {
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
  
    get lineageId(): string {
      return this.#lineageId;
    }

    get columnId(): string {
        return this.#columnId;
    }

    get materializationId(): string {
        return this.#materializationId;
    }
        
  
    private constructor(properties: DashboardProperties) {
        this.#url = properties.url;
        this.#name = properties.name;
        this.#materializationName = properties.materializationName;
        this.#columnName = properties.columnName;
        this.#id = properties.id;
        this.#lineageId = properties.lineageId;
        this.#columnId = properties.columnId;
        this.#materializationId = properties.materializationId;
    }
  
    static create = (properties: DashboardProperties): Dashboard => {

        if(!properties.materializationName) throw new TypeError('Dashboard must have materialisation');
        if(!properties.columnName) throw new TypeError('Dashboard must have column');
        if(!properties.id) throw new TypeError('Dashboard must have id');
        if(!properties.lineageId) throw new TypeError('Dashboard must have lineageId');
        if(!properties.columnId) throw new TypeError('Dashboard must have columnId');
        if(!properties.materializationId) throw new TypeError('Dashboard must have materializationId');
  
  
      const dashboard = new Dashboard(properties);
  
      return dashboard;
    };
  }
  