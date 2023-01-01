import * as DynamoDb from "aws-sdk/clients/dynamodb.js";
import JSONStream from "JSONStream";
import AWS from "aws-sdk/index.js";

// eslint-disable-next-line unicorn/no-null
let client: DynamoDb.DocumentClient | null = null;

const getDocumentClient = (): DynamoDb.DocumentClient => {
    if (!client) {
        client = new DynamoDb.DocumentClient();
    }
    return client;
};

type QueryInput = Omit<DynamoDb.DocumentClient.QueryInput, "Limit"> & { Limit: number };

const streamQuery = (query: QueryInput): Promise<DynamoDb.DocumentClient.QueryOutput> =>
    new Promise((resolve, reject) => {
        const records: DynamoDb.DocumentClient.ItemList = [];
        let isResolved = false;
        const stream = getDocumentClient().query(query).createReadStream();

        stream
            .pipe(JSONStream.parse("Items.*"))
            .on("data", (record) => {
                if (records.length < query.Limit) {
                    records.push(AWS.DynamoDB.Converter.unmarshall(record));
                }
                if (record.length === query.Limit && !isResolved) {
                    isResolved = true;
                    resolve({ Items: records });
                }
            })
            .on("error", (err) => reject(err))
            .on("end", () => {
                if (!isResolved) {
                    resolve({ Items: records });
                }
            });
    });

const paginateQuery = async (
    params: QueryInput,
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
        if (queryType === "stream") {
            return streamQuery({ Limit, ...params });
        } else {
            return paginateQuery({ Limit, ...params });
        }
    } else {
        return client.query(params).promise();
    }
};

export default query;
