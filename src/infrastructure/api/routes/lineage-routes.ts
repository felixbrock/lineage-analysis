import { Router } from 'express';
import app from '../../ioc-register';
import CreateLineageController from '../controllers/create-lineage-controller';
import ReadLineageSnapshotController from '../controllers/read-lineage-snapshot-controller';

const lineageRoutes = Router();

const createLineageController = new CreateLineageController(
  app.resolve('createLineage'),
  app.resolve('getAccounts')
);

const readLineageController = new ReadLineageSnapshotController(
  app.resolve('readLineageSnapshot'),
  app.resolve('getAccounts')
);

lineageRoutes.post('/', (req, res) => {
  createLineageController.execute(req, res);
});

lineageRoutes.get('/snapshot/:lineageId', (req, res) => {
  readLineageController.execute(req, res);
});

export default lineageRoutes;
