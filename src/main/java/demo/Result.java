package demo;

public class Result {
    int order_id;
    double unit_price;
    int quantity;
    double discount;


    public Result(int order_id, double unit_price, int quantity, double discount) {
        this.order_id = order_id;
        this.unit_price = unit_price;
        this.quantity = quantity;
        this.discount = discount;
    }
}
