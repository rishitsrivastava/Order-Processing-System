const orderService = require('../services/orderService');

exports.createOrder = async (req, res) => {
    try {
        const result = await orderService.createOrder(req.body);
        res.status(201).json(result);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: "internal server error" })
    }
};