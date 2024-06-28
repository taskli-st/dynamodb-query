# dynamodb-query

A thin layer around DynamoDb.DocumentClient() that supports limit and pagination as you would expect it to work

## What "problem" does this package solve?

The `Limit` attribute limits the scanned items in a query, not the returned items. 

This package is very helpful to you when:
- you need to 
