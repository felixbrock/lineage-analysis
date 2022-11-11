import { Router } from 'express';
import app from '../../ioc-register';
import ReadLogicController from '../controllers/read-logic-controller';

const logicRoutes = Router();

const readLogicController = new ReadLogicController(
  app.resolve('readLogic'),
  app.resolve('getAccounts'),
);

logicRoutes.get('/:id', (req, res) => {
  readLogicController.execute(req, res);
});

export default logicRoutes;
