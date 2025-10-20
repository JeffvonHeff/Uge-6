import requests
import pandas as pd


Orders = "https://etl-server.fly.dev/orders"
Order_Items = "https://etl-server.fly.dev/order_items"
Customers = "https://etl-server.fly.dev/customers"

list_of_urls = [Orders, Order_Items, Customers]

for url in list_of_urls:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if url == Orders:
            df = pd.DataFrame(data)
            df.to_csv("orders.csv", index=False)
            print("Orders data extracted and saved to orders.csv")
        elif url == Order_Items:
            df = pd.DataFrame(data)
            df.to_csv("order_items.csv", index=False)
            print("Order items data extracted and saved to order_items.csv")
        elif url == Customers:
            df = pd.DataFrame(data)
            df.to_csv("customers.csv", index=False)
            print("Customers data extracted and saved to customers.csv")
    else:
        print(f"Failed to retrieve data from {url}")
