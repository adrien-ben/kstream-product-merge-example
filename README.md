# KStream product merge example

In this example we are going to merge product information coming from multiple topics.
Only product with the minimal required information (id, name and brand) will be outputted.

## Input

We consume the product part from 4 different topics:

- `product_details` contains general information about a product:

```json
{
  "name": "Wonderful thing",
  "description": "That's a wonderful thing, trust me...",
  "brand": "ShadyGuys"
}
```

The key of the message in Kafka will be the identifier of the product.

- `sku_details` contains general information about products variations.

```json
{
  "skuId": "S1P1",
  "productId": "P1",
  "name": "Blue wonderful thing",
  "description": "That's a wonderful thing, trust me..., and this one is blue !"
}
```

The key does not matter for our example.

- `offer_details` contains general information about skus offers.

```json
{
  "offerId": "O1S1P1",
  "productId": "P1",
  "skuId": "S1P1",
  "name": "Refurbished blue wonderful thing",
  "description": "That's a wonderful thing, trust me..., and this one is blue ! It should work too."
}
```

The key does not matter for our example.

- `prices` contains price information of the offers.

```json
{
  "offerId": "O1S1P1",
  "productId": "P1",
  "skuId": "S1P1",
  "amount": 19999.99
}
```

## Output

The merged products we be sent into the `products` topic.

```json
{
  "id": "P1",
  "name": "Wonderful thing",
  "description": "That's a wonderful thing, trust me...",
  "brand": "ShadyGuys",
  "skus": [
    {
      "id": "S1P1",
      "name": "Blue wonderful thing",
      "description": "That's a wonderful thing, trust me..., and this one is blue !",
      "offers": [
        {
          "id": "O1S1P1",
          "name": "Refurbished blue wonderful thing",
          "description": "That's a wonderful thing, trust me..., and this one is blue ! It should work too.",
          "price": 19999.99
        }
      ]
    }
  ]
}
```

## Implementation

One approach could be to join the multiple topics two-by-two.
This appraoch is not optimal because it would make our application generate multiple state store for each intermediate join operation.
And one changelog topic for each of them. This is not our appraoch.

We choose to stream each input topic and use the transformer API to manage only one state store that will contained the merged product.
The transformer will be responsible for:

- initiating the product when receiving the first part
- merging the received part in the product
- outputting the product if it's complete

The transformer will be generic inplementation that takes a merging function use to merge one of the part into the product.
We will create one instance of this transformer by input to merg in the final product.

To do that we fist need to re-key the various input by product id and send them through an intermidiate topic
to ensure proper partitioning of the message.

## Topology

![Topology](topology.png)

## Run the app 

```sh
# Run app
./mvnw spring-boot:run

# Run tests (note the clean to ensure the state store is cleaned up)
./mvnw clean test
```
