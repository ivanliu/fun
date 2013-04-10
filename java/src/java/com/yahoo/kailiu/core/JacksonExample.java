package com.yahoo.kailiu.core;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
//import org.codehaus.jackson.annotate.JsonMethod;
//import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

public class JacksonExample {
    public static void main(String[] args) {

        User user = new User();
        ObjectMapper mapper = new ObjectMapper();
        //mapper.setVisibility(JsonMethod.FIELD, Visibility.ANY);  -- only available in Jackson 1.9.2 or later

        try {

            // convert user object to json string, and save to a file
            mapper.writeValue(new File("user.json"), user);

            // display to console
            System.out.println(mapper.writeValueAsString(user));

        } catch (JsonGenerationException e) {

            e.printStackTrace();

        } catch (JsonMappingException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

}
