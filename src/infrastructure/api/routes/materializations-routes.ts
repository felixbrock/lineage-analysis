import { Router } from 'express';
import app from '../../ioc-register';
import ReadMaterializationsController from '../controllers/read-materializations-controller';

const materializationsRoutes = Router();

const readMaterializationsController = new ReadMaterializationsController(
  app.resolve('readMaterializations'),
  app.resolve('getAccounts'),
);

materializationsRoutes.get('/', (req, res) => {
  readMaterializationsController.execute(req, res);
});

export default materializationsRoutes;
