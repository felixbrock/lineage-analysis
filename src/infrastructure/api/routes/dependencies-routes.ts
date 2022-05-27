import { Router } from 'express';
import app from '../../ioc-register';
import ReadDependenciesController from '../controllers/read-dependencies-controller';

const dependenciesRoutes = Router();

const readDependenciesController = new ReadDependenciesController(
  app.resolve('readDependencies'),
  app.resolve('getAccounts'),
  app.resolve('db')
);

dependenciesRoutes.get('/', (req, res) => {
  readDependenciesController.execute(req, res);
});

export default dependenciesRoutes;
