package example.dynamodb;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class DynamoDemo implements RequestHandler<PersonRequest, PersonResponse> {

  private DynamoDB dynamoDb;
  private final static String DYNAMODB_TABLE_NAME = "demo-person";
  private final static Regions REGION = Regions.US_EAST_1;
  private final static String CALL_FUNCTION_NAME = "name-to-upper-case";
  private final static ObjectMapper mapper = new ObjectMapper();

  @Override
  public PersonResponse handleRequest(PersonRequest input, Context context) {

    // Lambda call
    InvokeRequest invokeRequest = new InvokeRequest()
      .withFunctionName(CALL_FUNCTION_NAME)
      .withPayload("{\n" +
        " \"person_id\": \"" + input.getPerson_id() + "\",\n" +
        " \"name\": \"" + input.getName() + "\"\n" +
        "}");
    System.out.println("invokeRequest");
    System.out.println(invokeRequest);
    InvokeResult invokeResult;
    PersonRequest upperRequest = null;
    try {
      AWSLambda awsLambda = AWSLambdaClientBuilder.standard()
        .withRegion(REGION).build();

      invokeResult = awsLambda.invoke(invokeRequest);
      System.out.println("invokeResult");
      System.out.println(invokeResult);

      String resultJson = new String(invokeResult.getPayload().array(), StandardCharsets.UTF_8);
      //write out the return value
      System.out.println("resultJson");
      System.out.println(resultJson);
      upperRequest = mapper.readValue(resultJson, PersonRequest.class);
      System.out.println("upperRequest");
      System.out.println(upperRequest);

    } catch (ServiceException | IOException e) {
      System.out.println(e);
    }


    PersonResponse personResponse = new PersonResponse();

    if (upperRequest == null) {
      personResponse.setMessage("Saved Unsuccessfully");
    } else {
      // DynamoDB commit
      this.initDynamoDbClient();
      persistData(upperRequest);
      personResponse.setPerson_id(upperRequest.getPerson_id());
      personResponse.setName(upperRequest.getName());
      personResponse.setMessage("Saved Successfully");
    }

    return personResponse;
  }

  private void initDynamoDbClient() {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
      .withRegion(REGION)
      .build();
    this.dynamoDb = new DynamoDB(client);
  }

  private void persistData(PersonRequest personRequest)
    throws ConditionalCheckFailedException {

    this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
      .putItem(
        new PutItemSpec().withItem(new Item()
          .withString("person_id", personRequest.getPerson_id())
          .withString("name", personRequest.getName())));
  }
}
