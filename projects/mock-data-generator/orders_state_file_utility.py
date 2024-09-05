import os


class OrdersStateFileUtility:
    STATE_FILE = "orders_state.txt"

    @staticmethod
    def read_last_order_id():
        if os.path.exists(OrdersStateFileUtility.STATE_FILE):
            with open(OrdersStateFileUtility.STATE_FILE, 'r') as file:
                return int(file.read().strip())

    @staticmethod
    def update_last_order_id(new_id):
        with open(OrdersStateFileUtility.STATE_FILE, 'w') as file:
            file.write(str(new_id))
        print(OrdersStateFileUtility.read_last_order_id)


if __name__ == '__main__':
    print(OrdersStateFileUtility.read_last_order_id())
    OrdersStateFileUtility.update_last_order_id(20)
    print(OrdersStateFileUtility.read_last_order_id())
    OrdersStateFileUtility.update_last_order_id(50)
    print(OrdersStateFileUtility.read_last_order_id())