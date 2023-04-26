import express, { Application } from 'express';
import compression from 'compression';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import v1Router from './routes/v1';
import Dbo from '../persistence/db/mongo-db';
import iocRegister from '../ioc-register';

interface AppConfig {
  port: number;
  mode: string;
}

export default class ExpressApp {
  #expressApp: Application;

  #config: AppConfig;

  constructor(config: AppConfig) {
    this.#expressApp = express();
    this.#config = config;
  }

  async start(runningLocal: boolean): Promise<Application> {
    const dbo: Dbo = iocRegister.resolve('dbo');

    try {
      await dbo.connectToServer();

      this.configApp();

      if (runningLocal)
        this.#expressApp.listen(this.#config.port, () => {
          console.log(
            `App running under pid ${process.pid} and listening on port: ${
              this.#config.port
            } in ${this.#config.mode} mode`
          );
        });

      return this.#expressApp;
    } catch (error: any) {
      throw new Error(error);
    }
  }

  private configApp(): void {
    this.#expressApp.use(express.json({ limit: '10mb' }));
    this.#expressApp.use(express.urlencoded({ extended: true, limit: '10mb' }));
    this.#expressApp.use(cors());
    this.#expressApp.use(compression());
    this.#expressApp.use(morgan('combined'));
    this.#expressApp.use(helmet());
    this.#expressApp.use(v1Router);
  }
}
