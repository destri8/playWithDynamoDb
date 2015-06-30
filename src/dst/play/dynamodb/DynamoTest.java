package dst.play.dynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.*;

/**
 * Created by Destri on 6/30/15.
 */
public class DynamoTest {

    public static void main(String args[]) {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
        client.setEndpoint("http://localhost:8000");

//        createTables(client);
//        listTables(client);
//        putItem(client);
//        getBatch(client);
        
    }

    private static void getBatch(AmazonDynamoDBClient client) {


        Map<String, KeysAndAttributes> requestMap = new HashMap<>();
        requestMap.put("content",
                new KeysAndAttributes().withAttributesToGet("content_id", "type", "content_text", "resource")
                    .withKeys(buildKeysToGet("1", "2", "3"))
        );

        BatchGetItemResult result = client.batchGetItem(new BatchGetItemRequest()
                .withRequestItems(requestMap));

        System.out.println("result " + result);
    }

    private static List<Map<String, AttributeValue>> buildKeysToGet(String... keys) {
        List<Map<String, AttributeValue>> list = new ArrayList<>();


        for (String key : keys) {
            Map<String, AttributeValue> map = new HashMap<>();
            map.put("content_id", new AttributeValue(key));
            list.add(map);
        }

        return list;
    }

    private static void putItem(AmazonDynamoDBClient client) {
        PutItemResult result = client.putItem(new PutItemRequest()
                .withTableName("content")
                .withItem(buildMapTextStory("1", "content text story 1")));

        System.out.println("result " + result.toString());

        PutItemResult result2 = client.putItem(new PutItemRequest()
                .withTableName("content")
                .withItem(buildMapTextStory("2", "content text story 2")));

        System.out.println("result " + result2.toString());

        PutItemResult result3 = client.putItem(new PutItemRequest()
                .withTableName("content")
                .withItem(buildMapTextStory("3", "content text story 3")));

        System.out.println("result " + result3.toString());

        PutItemResult result4 = client.putItem(new PutItemRequest()
                .withTableName("content")
                .withItem(buildMapPictureStory("4", "content text story 4", "http://www.google.com/notexist.jpg")));

        System.out.println("result " + result4.toString());

    }

    private static Map<String, AttributeValue> buildMapTextStory(String id, String content) {
        Map<String, AttributeValue> map = new HashMap<>();
        map.put("content_id", new AttributeValue(id));
        map.put("type", new AttributeValue("text"));
        map.put("content_text", new AttributeValue(content));
        return map;
    }

    private static Map<String, AttributeValue> buildMapPictureStory(String id, String content, String urlResource) {
        Map<String, AttributeValue> map = new HashMap<>();
        map.put("content_id", new AttributeValue(id));
        map.put("type", new AttributeValue("picture"));
        map.put("content_text", new AttributeValue(content));
        map.put("resource", new AttributeValue(urlResource));
        return map;
    }

    private static void createTables(AmazonDynamoDBClient client) {
        CreateTableResult result = client.createTable(new CreateTableRequest()
                        .withTableName("content")
                        .withAttributeDefinitions(
                                new AttributeDefinition("content_id", ScalarAttributeType.S)
                        )
                        .withKeySchema(new KeySchemaElement("content_id", KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(10l, 10l)
                        )
        );

        System.out.println("result " + result.toString());

    }



    private static void listTables(AmazonDynamoDBClient client) {
        ListTablesResult result = client.listTables();

        System.out.println("table count " + result.getTableNames().size());
        System.out.println("table " + Arrays.toString(result.getTableNames().toArray()));

        System.out.println("finish");

    }
}
