# node-shopify-stock-insert
NodeJS function to insert stock from a CSV file.

**Environment Variables**

**STORE_DBURL**: MongoDB Url\
**STORE_DBNAME**: MongoDB Database name\
**STORE_COLLECTIONNAME**: MongoDB Database Collection name for Store Stock

**MongoDB Store Stock table definition**

```json
{
    "sku": {
        "$numberInt": "0000"
    },
    "descr": "Title",
    "qty": "1",
    "price": "1.00",
    "promo": "0.00"
}
```
