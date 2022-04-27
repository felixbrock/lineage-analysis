import { Router } from 'express';
import app from '../../ioc-register';
import CreateLineageController from '../controllers/create-lineage-controller';

const lineageRoutes = Router();

const createLineageController = new CreateLineageController(
  app.resolve('createLineage'),
  app.resolve('getAccounts')
);

lineageRoutes.get('/', (req, res) => {
  createLineageController.execute(req, res);
});

export default lineageRoutes;
