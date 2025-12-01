const express = require('express');
const app = express();

app.use(express.json());

const orderController = require('../controllers/orderController.js')

app.post('/orders', orderController.createOrder);

app.listen(8080, () => {
    console.log("producer running on port 8080");
})