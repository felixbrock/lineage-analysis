import { Router } from 'express';
import app from '../../ioc-register';
import CreateLineageController from '../controllers/create-lineage-controller';
import ReadLineageController from '../controllers/read-lineage-controller';

const lineageRoutes = Router();

const createLineageController = new CreateLineageController(
  app.resolve('createLineage'),
  app.resolve('getAccounts')
);

const readLineageController = new ReadLineageController(
  app.resolve('readLineage'),
  app.resolve('getAccounts')
);

lineageRoutes.post('/', (req, res) => {
  createLineageController.execute(req, res);
});

lineageRoutes.get('/:id', (req, res) => {
  readLineageController.execute(req, res);
});

export default lineageRoutes;
