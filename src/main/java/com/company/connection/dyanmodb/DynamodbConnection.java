package com.company.connection.dyanmodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class DynamodbConnection {

	
	public static DynamoDbClient getDynamodbClient() {
		DynamoDbClient ddb = DynamoDbClient.builder().region(Region.US_EAST_1).build();
		return ddb;
	}

	public int queryTable(String tableName, String partitionKeyName, String partitionKeyVal) {
		DynamoDbClient ddb = getDynamodbClient();	
		QueryRequest queryReq = QueryRequest.builder().tableName(tableName).scanIndexForward(true).build();
		try {
			QueryResponse response = ddb.query(queryReq);
			return response.count();
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return -1;
	}
	
	public static String createTable( String tableName, String key) {
		
		DynamoDbClient ddb = getDynamodbClient();	
        DynamoDbWaiter dbWaiter = ddb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10))
                        .build())
                .tableName(tableName)
                .build();

        String newTable ="";
        try {
            CreateTableResponse response = ddb.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Wait until the Amazon DynamoDB table is created
            WaiterResponse<DescribeTableResponse> waiterResponse =  dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);

            newTable = response.tableDescription().tableName();
            return newTable;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
       return "";
    }

	
	
	public static void putItemInTable( String tableName, String key, String keyVal,
			String albumTitle, String albumTitleValue, String awards, String awardVal, String songTitle,
			String songTitleVal) {
		DynamoDbClient ddb = getDynamodbClient();
		HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();

// Add all content to the table
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(songTitle, AttributeValue.builder().s(songTitleVal).build());
		itemValues.put(albumTitle, AttributeValue.builder().s(albumTitleValue).build());
		itemValues.put(awards, AttributeValue.builder().s(awardVal).build());

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			ddb.putItem(request);
			System.out.println(tableName + " was successfully updated");

		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
	
	


	    // snippet-start:[dynamodb.java2.get_item.main]
	    public static void getDynamoDBItem(String tableName,String key,String keyVal ) {
	    	DynamoDbClient ddb = getDynamodbClient();
	        HashMap<String,AttributeValue> keyToGet = new HashMap<String,AttributeValue>();

	        keyToGet.put(key, AttributeValue.builder()
	                .s(keyVal).build());

	        GetItemRequest request = GetItemRequest.builder()
	                .key(keyToGet)
	                .tableName(tableName)
	                .build();

	        try {
	            Map<String,AttributeValue> returnedItem = ddb.getItem(request).item();

	            if (returnedItem != null) {
	                Set<String> keys = returnedItem.keySet();
	                System.out.println("Amazon DynamoDB table attributes: \n");

	                for (String key1 : keys) {
	                    System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
	                }
	            } else {
	                System.out.format("No item found with the key %s!\n", key);
	            }
	        } catch (DynamoDbException e) {
	            System.err.println(e.getMessage());
	            System.exit(1);
	        }
	    }
}
