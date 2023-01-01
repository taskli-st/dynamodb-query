import * as DynamoDb from "aws-sdk/clients/dynamodb.js";

// eslint-disable-next-line unicorn/no-null
let client: DynamoDb.DocumentClient | null = null;

const getDocumentClient = (): DynamoDb.DocumentClient => {
    if (!client) {
        client = new DynamoDb.DocumentClient();
    }
    return client;
};

const paginateQuery = async (
    params: Omit<DynamoDb.DocumentClient.QueryInput, "Limit"> & { Limit: number },
    tempItems: DynamoDb.DocumentClient.ItemList = [],
): Promise<DynamoDb.DocumentClient.QueryOutput> => {
    const response = await getDocumentClient().query(params).promise();
    const items = [...tempItems, ...(response.Items || [])];
    return items.length < params.Limit && response.LastEvaluatedKey
        ? paginateQuery(
              {
                  ...params,
                  ExclusiveStartKey: response.LastEvaluatedKey,
              },
              items,
          )
        : {
              ...response,
              Items: items,
          };
};

const query = async ({
    queryType,
    Limit,
    ...params
}: DynamoDb.DocumentClient.QueryInput & {
    queryType?: "stream" | "paginate";
}): Promise<DynamoDb.DocumentClient.QueryOutput> => {
    const client = getDocumentClient();
    if (Limit) {
        return paginateQuery({ Limit, ...params });
    } else {
        return client.query(params).promise();
    }
};

export default query;
