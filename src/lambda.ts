import serverlessExpress from '@vendia/serverless-express';
import { Application } from 'express';
import ExpressApp from './infrastructure/api/express-app';
import { appConfig } from './config';

interface InvokeEvent {
  req: {
    catalog?: string;
    manifest?: string;
    targetOrgId: string;
    biType?: string;
  };
  auth: { jwt: string };
  internalInvokeType: string;
  [key: string]: unknown;
}

let serverlessExpressInstance: any;

const asyncTask = (): Promise<Application> => {
  const expressApp = new ExpressApp(appConfig.express);

  return expressApp.start(false);
};

const setup = async (event: InvokeEvent, context: unknown): Promise<any> => {
  const app = await asyncTask();
  serverlessExpressInstance = serverlessExpress({
    app,
  });
  return serverlessExpressInstance(event, context);
};

const getServerlessExpressInstance = async (
  event: InvokeEvent,
  context: unknown
): Promise<any> => {
  if (serverlessExpressInstance)
    return Promise.resolve(serverlessExpressInstance(event, context));

  return setup(event, context);
};

// eslint-disable-next-line import/prefer-default-export
export const handler = async (
  event: InvokeEvent,
  context: unknown
): Promise<any> => {
  const apiInstance = await getServerlessExpressInstance(event, context);
  return apiInstance;
};
