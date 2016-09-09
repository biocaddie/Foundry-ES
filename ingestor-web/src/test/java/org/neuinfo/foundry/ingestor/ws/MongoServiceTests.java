package org.neuinfo.foundry.ingestor.ws;

import junit.framework.Assert;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neuinfo.foundry.common.model.Organization;

/**
 * Created by bozyurt on 10/16/14.
 */
public class MongoServiceTests {

    @After
    public void tearDown() throws Exception {
        MongoService.getInstance().shutdown();
    }

    @Test
    public void saveFindOrganization() throws Exception {
        ObjectId id = MongoService.getInstance().saveOrganization("UCSD");

        System.out.println("inserted id:" + id);

        Organization org = MongoService.getInstance().findOrganization(null, id.toHexString());

        Assert.assertNotNull(org);
        System.out.println(org);

    }

    @Ignore
    public void removeOrganization() throws Exception {
        MongoService.getInstance().removeOrganization("UCSD", null);
    }
}
