package com.cobub.es.dao;

import com.cobub.es.common.ClientFactory;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by feng.wei on 2015/12/4.
 */
public class UpdateIndex {


    public static boolean isExist() {
        return true;
    }

    public static void main(String[] args) throws IOException {
        Client client = ClientFactory.getClient();
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("tag");
        updateRequest.type("PeopleProperties");
        updateRequest.id("2bbeaa35d4794cf8ab85e1259ee30af9");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("phoneNumber", "11111111111")
                .endObject());

        client.update(updateRequest);

    }

}
