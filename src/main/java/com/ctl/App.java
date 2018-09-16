package com.ctl;

import com.google.auth.Credentials;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.collect.Lists;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;


/**
 * Hello world!
 *
 */

public class App 
{
    final Logger logger = LoggerFactory.getLogger(App.class);
    final static String jsonPath = "/home/scott/src/getting-started-python/simple/simple01-scott2-2gen.json";

    final static String PROJECT_ID = "simple01-216520";
    final static String INSTANCE_ID = "scott-bigtable01";
    final static String USER_AGENT = "scott2@simple01-216520.iam.gserviceaccount.com";
    final static String COLUMN_FAMILY_NAME = "familyone";
    final static String TABLE_NAME = "tableone";

    private Storage getStorage() throws IOException {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        return storage;
    }

    public void listStorage() throws IOException {
        logger.info("hi");
        Storage storage = getStorage();
        Page<Bucket> bucketPage =  storage.list();
        for (Bucket bucket: bucketPage.iterateAll()) {
            logger.info(bucket.toString());
        }
    }

    public CreateTableRequest createTableRequest() {
        Table.Builder tableBuilder = Table.newBuilder();
        tableBuilder.putColumnFamilies(COLUMN_FAMILY_NAME, ColumnFamily.newBuilder().build());

        CreateTableRequest ctr = CreateTableRequest.newBuilder()
                .setParent("projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID)
                .setTableId(TABLE_NAME)
                .setTable(tableBuilder.build())
                .build();
        return ctr;
    }

    public void toBigTable() throws IOException, GeneralSecurityException {

        Credentials cred = CredentialFactory.getInputStreamCredential(new FileInputStream(jsonPath));

        try {
            BigtableSession session = new BigtableSession(new BigtableOptions.Builder()
                    .setUserAgent(USER_AGENT)
                    .setProjectId(PROJECT_ID)
                    .setInstanceId(INSTANCE_ID)
                    .setCredentialOptions(CredentialOptions.credential(cred)
                    ).build());

            //Connection connection = BigtableConfiguration.connect("simple01-216520", "scott-bigtable01");
//        Configuration config = BigtableConfiguration.configure(projectId, instanceId);
//        config.set("google.bigtable.auth.json.keyfile", jsonPath);
//        Connection conn = BigtableConfiguration.connect(config);
            logger.info(session.getClusterName().toString());


            try {
                BigtableTableAdminClient admin = session.getTableAdminClient();
                admin.createTable(createTableRequest());
            } catch(io.grpc.StatusRuntimeException sre) {
                logger.warn("possibly the table already exist",sre);
            }

//            BulkMutation mutation = session.createBulkMutation();
        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            throw e;
        }
        logger.warn("connection made");
    }
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        App a = new App();
        try {
            a.listStorage();
            a.toBigTable();
        } catch (Exception e) {

            System.out.println(e.getMessage());
        }

    }

}
