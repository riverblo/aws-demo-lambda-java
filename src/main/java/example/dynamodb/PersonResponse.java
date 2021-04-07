package example.dynamodb;

import lombok.Data;

@Data
public class PersonResponse {

  private String person_id;
  private String name;
  private String message;
}
