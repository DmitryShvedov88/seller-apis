## Seller-apis
We are opening an online store selling watches on the OZON marketplace on Yandex market.
Each marketplace has its own market.py files for Yandex and seller.py for OZON.

### seller.py
#### Short description
The script checks balances on the website of the Casio watch online store. Receives articles for the OZON marketplace. Updates stock and product range. Updates prices according to the offer from the website of the Casio watch supplier, after which it sets prices in our store on the market place.
#### Script functions
1. Obtaining a list of SKUs of your products on the marketplace
2. Updating the stock at the warehouse of the product supplier
3. Creates a product range in your store in the marketplace
4. Updates product prices based on supplier prices and your markup
5. The script processes errors in the operation of sites and will tell you, if necessary, what the error is

### market.py
#### Short description
This script can also check balances on the Casio website. Receives articles for the Yandex Market marketplace. Updates balances and nomenclature. Updates prices according to the offer from the website of the Casio watch supplier, after which it sets prices in our store on the market place. However, there are two operating models: FBS and DBS.
#### Script functions
1. Receiving a list of articles of your products that participate in the promotion
2. Updating the stock at the warehouse of the product supplier
3. Updates product prices based on supplier prices and your markup (FBS and DBS)
4. The script processes errors in the operation of sites and will tell you, if necessary, what the error is

#### How to use
These programs are designed for the average user and do not require serious knowledge of programming and information technology.

1. You need to prepare an .ENV file with the following environment variables
The following environment variables are required for seller.py
                            
        CLIENT_ID - Ozon marketplace client ID.
        SELLER_TOKEN - API - Ozon Seller key.

market.py requires the following environment variables
    
        MARKET_TOKEN — Yandex.Market API token.
        FBS_ID — ID of the Yandex Market seller using the FBS system.
        WAREHOUSE_FBS_ID — ID of the seller’s warehouse, according to the FBS system.
        DBS_ID — Yandex Market seller ID using the DBS system.
        WAREHOUSE_DBS_ID — ID of the seller’s warehouse, according to the DBS system.

2. Also, to launch you will need an Excel file packed in the ostatki.zip archive. Where information about product balances is contained.
3. Run the scripts with the following commands:

        python seller.py
        python marker.py
    