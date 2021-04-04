package example.dynamodb;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * Get an item from a DynamoDB table.
 * <p>
 * Takes the name of the table and the name of the item to retrieve from it.
 * <p>
 * The primary key searched is "DATABASE_NAME", and the value contained by the field "Greeting" will
 * be returned.
 * <p>
 * This code expects that you have AWS credentials set up per: http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public class DynamoDemo implements RequestHandler<PersonRequest, PersonResponse> {

  private DynamoDB dynamoDb;
  private final String DYNAMODB_TABLE_NAME = "demo-person";
  private final Regions REGION = Regions.US_EAST_1;

  @Override
  public PersonResponse handleRequest(PersonRequest input, Context context) {

    this.initDynamoDbClient();

    persistData(input);

    PersonResponse personResponse = new PersonResponse();
    personResponse.setMessage("Saved Successfully");
    return personResponse;
  }

  private void initDynamoDbClient() {
    // TODO: 後で書き換える
    AmazonDynamoDBClient client = new AmazonDynamoDBClient();
    client.setRegion(Region.getRegion(REGION));
    this.dynamoDb = new DynamoDB(client);
  }

  private PutItemOutcome persistData(PersonRequest personRequest)
      throws ConditionalCheckFailedException {

    return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
        .putItem(
            new PutItemSpec().withItem(new Item()
                .withString("person_id", personRequest.getPerson_id())
                .withString("name", personRequest.getName())));
  }
}