export interface DashboardProperties {
    url?: string;
    name?: string;
    materialisation: string;
    column: string; 
    id: string;
    lineageId: string;
    columnId: string,
    matId: string;
  }

export class Dashboard {
    
    #url?: string;
    
    #name?: string;
    
    #materialisation: string;

    #matId: string;
    
    #column: string;

    #columnId: string;

    #id: string;
  
    #lineageId: string;
    
    
    get url(): string|undefined {
        return this.#url;
    }

    get name(): string|undefined {
        return this.#name;
    }

    get materialisation(): string {
        return this.#materialisation;
    }

    get column(): string {
        return this.#column;
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

    get matId(): string {
        return this.#matId;
    }
        
  
    private constructor(properties: DashboardProperties) {
        this.#url = properties.url;
        this.#name = properties.name;
        this.#materialisation = properties.materialisation;
        this.#column = properties.column;
        this.#id = properties.id;
        this.#lineageId = properties.lineageId;
        this.#columnId = properties.columnId;
        this.#matId = properties.matId;
    }
  
    static create = (properties: DashboardProperties): Dashboard => {

        if(!properties.materialisation) throw new TypeError('Dashboard must have materialisation');
        if(!properties.column) throw new TypeError('Dashboard must have column');
        if(!properties.id) throw new TypeError('Dashboard must have id');
        if(!properties.lineageId) throw new TypeError('Dashboard must have lineageId');
        if(!properties.columnId) throw new TypeError('Dashboard must have columnId');
        if(!properties.matId) throw new TypeError('Dashboard must have matId');
  
  
      const dashboard = new Dashboard(properties);
  
      return dashboard;
    };
  }
  