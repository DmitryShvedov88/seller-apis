import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token)-> dict:
    """Get a list of ozone store products
    Args:
        last_id (str): last_id in OZON
        client_id (str): client_id in OZON
        seller_token (str): seller_token in OZON

    Returns:
        dict: list of items from the OZON store

    Examples:
          >>> get_product_list("", client_id, seller_token)
        json file
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token)-> list:
    """Get article numbers for ozone store products
    Args:
        client_id (str): client_id in OZON
        seller_token (str): seller_token in OZON

    Returns:
        list: R list of items from the OZON store

    Examples:
         >>> get_offer_ids("client_id_55", "seller_token_55")
        ['00001', '00002', ...]
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token)-> dict:
    """update watch price
    Args:
        prices (str): prices in OZON
        client_id (str): client_id in OZON
        seller_token (str): seller_token in OZON

    Returns:
        dict: list of items from the OZON store
    
    Raises:
        HTTPError: If the API call fails.        

    Examples:
         >>> update_price([{'offer_id': '00001', 'Model1': 10}, ...], "client_id_55", "seller_token_55")
        {"status": "success", "message": "Stocks updated successfully"}
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token)-> dict:
    """Update balances
    Args:
        stocks (list): list of products to update on the site OZON
        client_id (str): client_id in OZON
        seller_token (str): seller_token in OZON

    Returns:
        dict: JSON file

    Raises:
        HTTPError: If the API call fails.

    Examples:
         >>> update_stocks("client_id_55", "seller_token_55")
          {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }

    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock()-> list:
    """Download remnants file from casio website
    Args:
        It doesnt have any args

    Returns:
        list: Returns a list of dictionaries of the remaining warehouses of the Casio store

    Examples:
         >>> download_stock()
        [{Model1: 10}, {Model_F: 50}, ...]

    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids)-> list:
    """
    The function creates a list of products to update on the site OZON

    Args:
        watch_remnants (list): list of remnants watches.
        offer_ids (list): list of article numbers.

    Returns:
        list: list of remnants watches to update in OZON storage.

    Examples:
        >>> create_stocks([{Model1: 10}, {Model_F: 50}, ...], ['00001', '00002', ...])
        [{'offer_id': '00001', 'Model1': 10}, ...]
    """

    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids)-> list:
    """
    This function create list of prices to upload on the site OZON

    Args:
        watch_remnants (list): list of remnants watches.
        offer_ids (list): list of article numbers.

    Returns:
        list: list of prices watches to update in OZON storage.

    Examples:
        >>> create_stocks([{Model1: 10}, {Model_F: 50}, ...], ['00001', '00002', ...])
        [
        {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }, ...
            ]
    """
    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """A function that converts the price from the format "xxxx.xx руб" to the format "XXXX" 
    Converts a price string into a numerical format. The function will find numbers in a string in an array
    of characters and leave only them,  having previously combined them into one string

    Args:
        price (str): price.

    Converted str:
        str:  Converted str.

    Correct execution of the function:
    Example:
        \>>> price = "xxxx.xx руб"
        \>>> price_conversion(price)
        XXXX

    Uncorrect:
        \>>> price = "xxxx.xx руб"
        \>>> price_conversion(price)
        xxxx.xx руб
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int) -> list:
    """Split list lst into parts of n elements
    Args:
        lst (list): [{'offer_id': '00001', 'Model1': 10}, ...]
        n (int): 10
    Returns:
        list: list of remnants watches to update in OZON storage.

    Examples:
        >>> create_stocks([{Model1: 10}, {Model_F: 50}, ...], ['00001', '00002', ...])
        [[0, 9], [1, 10], ...]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token)-> list:
    """upload_prices in OZON STORE
    Args:
        watch_remnants (list): list of remnants watches.
        client_id (str):  client_id in OZON
        seller_token (str): The seller token for authentication.

    Returns:
        list: A list of prices.

    Example:
        >>> upload_prices(watch_remnants, "client123", "token123")

    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token)-> tuple:
    """Split list lst into parts of n elements
    Args:
        watch_remnants (list): list of remnants watches.
        client_id (str): client_id in OZON
        seller_token (str): seller_token in OZON
    Returns:
        tuple: two lists with items in stock and with all items.

    Examples:
        >>> upload_stocks([{'Model1': 10}, ...],  [{'Model10': 0}, ...], client_id, seller_token)
        
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
