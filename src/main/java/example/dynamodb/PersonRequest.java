package example.dynamodb;

import lombok.Data;

@Data
public class PersonRequest {

  private String person_id;
  private String name;
}