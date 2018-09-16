package com.ctl;

import com.google.auth.Credentials;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.collect.Lists;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;


/**
 * Hello world!
 *
 */

public class App 
{
    final static Logger logger = LoggerFactory.getLogger(App.class);
    final static String jsonPath = "/home/scott/src/getting-started-python/simple/simple01-scott2-2gen.json";

    final static String PROJECT_ID = "simple01-216520";
    final static String INSTANCE_ID = "scott-bigtable01";
    final static String USER_AGENT = "scott2@simple01-216520.iam.gserviceaccount.com";
    final static String COLUMN_FAMILY_NAME = "familyone";
    final static String TABLE_ID = "tabletwo";
    final static String PROJECT_INSTANCE = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID;




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
                .setParent(PROJECT_INSTANCE)
                .setTableId(TABLE_ID)
                .setTable(tableBuilder.build())
                .build();
        return ctr;
    }

    public BigtableSession getSession() throws IOException, GeneralSecurityException {

        Credentials cred = CredentialFactory.getInputStreamCredential(new FileInputStream(jsonPath));

        try {
            BigtableSession session = new BigtableSession(new BigtableOptions.Builder()
                    .setUserAgent(USER_AGENT)
                    .setProjectId(PROJECT_ID)
                    .setInstanceId(INSTANCE_ID)
                    .setCredentialOptions(CredentialOptions.credential(cred)
                    ).build());


            logger.info(session.getClusterName().toString());
            return session;

        } catch(Exception e) {
            logger.error(e.getMessage(),e);
            throw e;
        }
    }

    public void loadData() throws IOException, GeneralSecurityException, InterruptedException {
        BigtableSession session = getSession();
        try {
            BigtableTableAdminClient admin = session.getTableAdminClient();
            //admin.createTable(createTableRequest());
        } catch(io.grpc.StatusRuntimeException sre) {
            logger.warn("possibly the table already exist",sre);
        }

        BigtableTableName name = new BigtableTableName(PROJECT_INSTANCE + "/tables/" + TABLE_ID);
        logger.info("Table name " + name.toString());


        BulkMutation bulkMutation = session.createBulkMutation(name);

        ByteString column1 = ByteString.copyFrom("column1","utf-8");
        ByteString column2 = ByteString.copyFrom("column2","utf-8");
        ByteString column3 = ByteString.copyFrom("column3","utf-8");

        for(int i = 0; i < 50; i++) {
            byte[] uniqueValue = new byte[7];
            new Random().nextBytes(uniqueValue);
            String uniqueKey =  Long.toString(System.currentTimeMillis()) + "-" + Integer.toHexString(i);

            ByteString key = ByteString.copyFrom("scott01-" +uniqueKey, "utf-8");
            ByteString value1 = ByteString.copyFrom("abcef" + uniqueValue,"utf-8");
            ByteString value2 = ByteString.copyFrom("ghijkl" + uniqueValue,"utf-8");
            ByteString value3 = ByteString.copyFrom("mauve_" + uniqueValue,"utf-8");

            Mutation.SetCell setCell1 = Mutation.SetCell.newBuilder()
                    .setFamilyName(COLUMN_FAMILY_NAME)
                    .setColumnQualifier(column1)
                    .setValue(value1).build();

            Mutation.SetCell setCell2 = Mutation.SetCell.newBuilder()
                    .setFamilyName(COLUMN_FAMILY_NAME)
                    .setColumnQualifier(column2)
                    .setValue(value2).build();

            Mutation.SetCell setCell3 = Mutation.SetCell.newBuilder()
                    .setFamilyName(COLUMN_FAMILY_NAME)
                    .setColumnQualifier(column3)
                    .setValue(value3).build();

            ArrayList<Mutation> mutations = new ArrayList<>();
            mutations.add(Mutation.newBuilder().setSetCell(setCell1).build());
            mutations.add(Mutation.newBuilder().setSetCell(setCell2).build());
            mutations.add(Mutation.newBuilder().setSetCell(setCell3).build());

            MutateRowRequest request = MutateRowRequest.newBuilder()
                    .setTableName(TABLE_ID)
                    .setRowKey(key)
                    .addAllMutations(mutations)
                    .build();
            bulkMutation.add(request);
            logger.info("added " + key.toString("utf-8"));
        }
        bulkMutation.flush();
    }

    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        App a = new App();
        try {
            a.listStorage();
            a.loadData();
        } catch (Exception e) {
            logger.error("failure in process " + e.getMessage(), e);
        }

    }

}
