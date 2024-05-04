import {SNSEvent, SNSHandler} from 'aws-lambda';
import {DynamoDBClient} from '@aws-sdk/client-dynamodb';
import {DynamoDBDocumentClient, GetCommand, UpdateCommand} from "@aws-sdk/lib-dynamodb";

const ddb = createDDbDocClient();

export const handler: SNSHandler = async (event: SNSEvent) => {
    for (const record of event.Records) {
        const message = JSON.parse(record.Sns.Message);
        const attributes = record.Sns.MessageAttributes;

        if (attributes.comment_type && attributes.comment_type.Value === 'Caption') {
            const key = message.name
            const newDescription = message.description;

            const getItemParams = {
                TableName: 'ImageTable',
                Key: {ImageName: key},
            };
            try {
                console.log("Get Item Begin")
                const {Item} = await ddb.send(new GetCommand(getItemParams));
                console.log("get Item")
                if (Item) {
                    console.log("get Item success")
                    const params = {
                        TableName: 'ImageTable',
                        Key: {
                            ImageName: key
                        },
                        UpdateExpression: 'set Description = :description',
                        ExpressionAttributeValues: {
                            ':description': newDescription
                        },
                    };
                    try {
                        // Attempt to update the item in DynamoDB
                        const result = await ddb.send(new UpdateCommand(params));
                        console.log(`Updated item with key ${key}:`, result);
                    } catch (error) {
                        console.error("Error updating item in DynamoDB: ", error);
                        throw new Error(`Unable to update item with key ${key} in DynamoDB.`);
                    }
                }
            } catch (error) {
                console.error("Error updating item in DynamoDB: ", error);
                throw new Error(`Image "${key}" not exists.`);
            }
        }
    }

};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true, removeUndefinedValues: true, convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}