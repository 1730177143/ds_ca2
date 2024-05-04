import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import {Construct} from "constructs";
import {StreamViewType} from "aws-cdk-lib/aws-dynamodb";
import {DynamoEventSource} from "aws-cdk-lib/aws-lambda-event-sources";
import {StartingPosition} from "aws-cdk-lib/aws-lambda";

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const imagesBucket = new s3.Bucket(this, "images", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            publicReadAccess: false,
        });
        // create table
        const imageTable = new dynamodb.Table(this, 'ImageTable', {
            stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            partitionKey: {name: "ImageName", type: dynamodb.AttributeType.STRING},
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            tableName: 'ImageTable',
        });
        // Integration infrastructure
        //DLQ
        const processImageDLQ = new sqs.Queue(this, "ProcessImageDLQ", {
            queueName: "ProcessImageDLQ",
            receiveMessageWaitTime: cdk.Duration.seconds(10),
        });
        const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
            receiveMessageWaitTime: cdk.Duration.seconds(10),
            deadLetterQueue: {
                queue: processImageDLQ,
                maxReceiveCount: 1
            }
        });

        // Lambda functions

        const processImageFn = new lambdanode.NodejsFunction(
            this,
            "ProcessImageFn",
            {
                runtime: lambda.Runtime.NODEJS_18_X,
                entry: `${__dirname}/../lambdas/processImage.ts`,
                timeout: cdk.Duration.seconds(15),
                memorySize: 128,
                environment: {
                    TABLE_NAME: imageTable.tableName,
                    REGION: 'eu-west-1',
                }
            }
        );

        const mailerFn = new lambdanode.NodejsFunction(this, "mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/mailer.ts`,
        });

        const rejectionMailerFn = new lambdanode.NodejsFunction(this, "rejection-mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
        });

        const processDeleteFn = new lambdanode.NodejsFunction(this, "process-delete-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/processDelete.ts`,
        });

        const updateTableFn = new lambdanode.NodejsFunction(this, "update-table-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/updateTable.ts`,
        });
        const deleteMailerFn = new lambdanode.NodejsFunction(this, "delete-mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/deleteMailer.ts`,
        });
        //sns topic
        const ImageTopic = new sns.Topic(this, "ImageTopic", {
            displayName: "Image topic",
        });
        // S3 --> SNS
        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.SnsDestination(ImageTopic)
        );

        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_REMOVED,
            new s3n.SnsDestination(ImageTopic)
        );

        ImageTopic.addSubscription(
            new subs.SqsSubscription(imageProcessQueue, {
                filterPolicyWithMessageBody: {
                    Records: sns.FilterOrPolicy.policy({
                        eventName: sns.FilterOrPolicy.filter(
                            sns.SubscriptionFilter.stringFilter({
                                matchPrefixes: ["ObjectCreated:Put"],
                            })
                        ),
                    })
                }
            })
        );

        ImageTopic.addSubscription(new subs.LambdaSubscription(mailerFn, {
                filterPolicyWithMessageBody: {
                    Records: sns.FilterOrPolicy.policy({
                        eventName: sns.FilterOrPolicy.filter(
                            sns.SubscriptionFilter.stringFilter({
                                allowlist: ["ObjectCreated:Put"],
                            })
                        ),
                    })
                }
            })
        );

        ImageTopic.addSubscription(
            new subs.LambdaSubscription(processDeleteFn, {
                filterPolicyWithMessageBody: {
                    Records: sns.FilterOrPolicy.policy({
                        eventName: sns.FilterOrPolicy.filter(
                            sns.SubscriptionFilter.stringFilter({
                                allowlist: ["ObjectRemoved:Delete"],
                            })
                        ),
                    })
                }
            })
        );
        ImageTopic.addSubscription(
            new subs.LambdaSubscription(updateTableFn, {
                filterPolicy: {
                    comment_type: sns.SubscriptionFilter.stringFilter({
                        allowlist: ['Caption']
                    }),
                },
            })
        );

        // SQS --> Lambda
        const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });

        const newRejectionMailEventSource = new events.SqsEventSource(processImageDLQ, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });

        processImageFn.addEventSource(newImageEventSource);

        rejectionMailerFn.addEventSource(newRejectionMailEventSource);
        deleteMailerFn.addEventSource(new DynamoEventSource(imageTable, {
            startingPosition: StartingPosition.TRIM_HORIZON,
            batchSize: 5,
            bisectBatchOnError: true,
        }));
        // Permissions

        imagesBucket.grantRead(processImageFn);
        imageTable.grantWriteData(processImageFn);
        imageTable.grantReadWriteData(processDeleteFn);
        imageTable.grantReadWriteData(updateTableFn);

        processImageFn.addToRolePolicy(new iam.PolicyStatement({
            actions: ["sqs:SendMessage"],
            resources: [processImageDLQ.queueArn]
        }));

        mailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );
        rejectionMailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );
        deleteMailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );

        // Output

        new cdk.CfnOutput(this, "bucketName", {
            value: imagesBucket.bucketName,
        });

        new cdk.CfnOutput(this, "deleteAndUpdateTopic", {
            value: ImageTopic.topicArn,
        });
    }
}
