/* eslint-disable import/extensions, import/no-absolute-path */
import {SQSHandler} from "aws-lambda";
import {
    GetObjectCommand,
    PutObjectCommandInput,
    GetObjectCommandInput,
    S3Client,
    PutObjectCommand,
} from "@aws-sdk/client-s3";
import {DynamoDBClient, PutItemCommand} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";

const s3 = new S3Client();
const ddb = createDDbDocClient();

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME || 'ImageTable';

export const handler: SQSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const recordBody = JSON.parse(record.body);  // Parse SQS message
        const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message

        if (snsMessage.Records) {
            console.log("Record body ", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcBucket = s3e.bucket.name;
                // Object key may have spaces or unicode non-ASCII characters.
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));

                // Check file extension
                if (!srcKey.endsWith('.jpeg') && !srcKey.endsWith('.png')) {
                    throw new Error(`Unsupported file type for file ${srcKey}`);
                }
                try {
                    // Download the image from the S3 source bucket if the file type is supported.
                    const params: GetObjectCommandInput = {
                        Bucket: srcBucket,
                        Key: srcKey,
                    };
                    await s3.send(new GetObjectCommand(params));
                    // After processing, write to DynamoDB
                    await ddb.send(new PutItemCommand({
                        TableName: TABLE_NAME,
                        Item: {
                            "ImageName": {S: srcKey}, // Use the image name as the primary key
                            'Bucket': {S: srcBucket},
                            'CreatedAt': {S: new Date().toISOString()}
                        }
                    }));
                } catch (error) {
                    console.error("Error processing image or writing to DynamoDB:", error);
                    throw error;
                }
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