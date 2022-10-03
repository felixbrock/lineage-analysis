import { Router } from 'express';
import app from '../../ioc-register';
import ReadExternalResourcesController from '../controllers/read-external-resources-controller';

const externalResourcesRoutes = Router();

const readExternalResourcesController = new ReadExternalResourcesController(
  app.resolve('readExternalResources'),
  app.resolve('getAccounts'),
  app.resolve('dbo')
);

externalResourcesRoutes.get('/', (req, res) => {
  readExternalResourcesController.execute(req, res);
});

export default externalResourcesRoutes;
