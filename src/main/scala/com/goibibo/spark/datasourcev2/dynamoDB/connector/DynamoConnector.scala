
package com.goibibo.spark.datasourcev2.dynamoDB.connector

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, ItemCollection, ScanOutcome}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import org.apache.spark.sql.sources.Filter

private[dynamodb] trait DynamoConnector {

    def getDynamoDB(region: Option[String] = None, roleArn: Option[String] = None): DynamoDB = {
        val client: AmazonDynamoDB = getDynamoDBClient(region, roleArn)
        new DynamoDB(client)
    }

    private def getDynamoDBClient(region: Option[String] = None, roleArn: Option[String] = None): AmazonDynamoDB = {
        val chosenRegion = region.getOrElse(sys.env.getOrElse("aws.dynamodb.region", "us-east-1"))
        val credentials = getCredentials(chosenRegion, roleArn)

        Option(System.getProperty("aws.dynamodb.endpoint")).map(endpoint => {
            AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentials)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
                .build()
        }).getOrElse(
            AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(chosenRegion)
                .build()
        )
    }
    def getDynamoDBAsyncClient(region: Option[String] = None, roleArn: Option[String] = None): AmazonDynamoDBAsync = {
        val chosenRegion = region.getOrElse(sys.env.getOrElse("aws.dynamodb.region", "us-east-1"))
        val credentials = getCredentials(chosenRegion, roleArn)

        Option(System.getProperty("aws.dynamodb.endpoint")).map(endpoint => {
            AmazonDynamoDBAsyncClientBuilder.standard()
                .withCredentials(credentials)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
                .build()
        }).getOrElse(
            AmazonDynamoDBAsyncClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(chosenRegion)
                .build()
        )
    }

    /**
      * Get credentials from a passed in arn or from profile or return the default credential provider
      **/
    private def getCredentials(chosenRegion: String, roleArn: Option[String]) = {
        roleArn.map(arn => {
            val stsClient = AWSSecurityTokenServiceClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain)
                .withRegion(chosenRegion)
                .build()
            val assumeRoleResult = stsClient.assumeRole(
                new AssumeRoleRequest()
                    .withRoleSessionName("DynamoDBAssumed")
                    .withRoleArn(arn)
            )
            val stsCredentials = assumeRoleResult.getCredentials
            val assumeCreds = new BasicSessionCredentials(
                stsCredentials.getAccessKeyId,
                stsCredentials.getSecretAccessKey,
                stsCredentials.getSessionToken
            )
            new AWSStaticCredentialsProvider(assumeCreds)
        }).orElse(Option(System.getProperty("aws.profile")).map(new ProfileCredentialsProvider(_)))
            .getOrElse(new DefaultAWSCredentialsProviderChain)
    }

    val keySchema: KeySchema

    val readLimit: Double

    val itemLimit: Int

    val totalSizeInBytes: Long

    def scan(segmentNum: Int, columns: Seq[String], filters: Seq[Filter]): ItemCollection[ScanOutcome]

    def isEmpty: Boolean = itemLimit == 0

    def nonEmpty: Boolean = !isEmpty

}
