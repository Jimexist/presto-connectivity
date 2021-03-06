package com.madadata.eval.prestoconnectivity.resource;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableMap;
import com.facebook.presto.jdbc.internal.guava.collect.ImmutableSet;
import com.facebook.presto.jdbc.internal.guava.collect.Maps;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.hibernate.validator.constraints.NotEmpty;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jiayu on 7/12/16.
 */
@Path("/meta")
public class MyResource {

    private final DBI jdbi;

    public MyResource(DBI jdbi) {
        this.jdbi = jdbi;
    }

    private List<Map<String, String>> query(String sql, String... fields) throws SQLException {
        Set<String> fieldSet = ImmutableSet.copyOf(fields);
        try (Handle handle = jdbi.open()) {
            Statement statement = handle.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<Map<String, String>> results = Lists.newArrayList();
            while (resultSet.next()) {
                results.add(ImmutableMap.copyOf(Maps.asMap(fieldSet, input -> {
                    try {
                        return resultSet.getString(input);
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                })));
            }
            return results;
        }
    }
   /**
    * does not work well, error info:
    * com.facebook.presto.jdbc.NotImplementedException: Method Connection.prepareStatement is not yet implemented
    */
    private List<Map<String, Object>> createQuery(String sql, String... fields) throws SQLException {
        Set<String> fieldSet = ImmutableSet.copyOf(fields);
        try (Handle handle = jdbi.open()) {
            Query<Map<String, Object>> querySet = handle.createQuery(sql);
            return querySet.list();
        }
    }


   /**
    * does not work well, error info:
    * com.facebook.presto.jdbc.NotImplementedException: Method Connection.prepareStatement is not yet implemented
    */
    private int jdbiCreate(String sql) throws SQLException {
        try (Handle handle = jdbi.open()) {
            handle.execute(sql);
            return 0;
        }
    }


   /**
    * work well
    * BUT must set schema in config otherwise will "com.facebook.presto.jdbc.internal.client.FailureInfo$FailureException: line 1:1: Schema must be specified when session schema is not set"
    */
    private int executeSQL(String sql)throws SQLException {
        try (Handle handle = jdbi.open()) {
            Statement statement = handle.getConnection().createStatement();
            return statement.executeUpdate(sql);
        }
    }

   /**
    * work well
    * id start with 1
    */
    private Map<String, String> queryMetadata(String sql) throws SQLException {
        try (Handle handle = jdbi.open()) {
            Statement statement = handle.getConnection().createStatement();
            ResultSetMetaData metaDataSet = statement.executeQuery(sql).getMetaData();
            Map<String, String> metadata = Maps.newHashMap();
            for (int i = 1; i < metaDataSet.getColumnCount() + 1; i++){
                metadata.put(metaDataSet.getColumnName(i), "" + metaDataSet.getColumnTypeName(i));
            }
            return metadata;
        }
    }

    @GET
    @Path("/views")
    @Produces(MediaType.APPLICATION_JSON)
    public Object viewMeta() throws Exception {
        return query("select * from hive.information_schema.views",
                "table_catalog", "table_schema", "table_name", "view_definition");
    }

    @GET
    @Path("/tables")
    @Produces(MediaType.APPLICATION_JSON)
    public Object tableMeta() throws Exception {
        return query("select * from hive.information_schema.tables",
                "table_catalog", "table_schema", "table_name", "table_type");
    }

   /**
    * doesn't work
    */
    @GET
    @Path("/create")
    @Produces(MediaType.APPLICATION_JSON)
    public int create(@QueryParam("name") @NotEmpty String name) throws Exception {
        return jdbiCreate(String.format("create table %s (id int primary key, name varchar(100))", name));
    }

   /**
    * doesn't work
    */
    @GET
    @Path("/createquery")
    @Produces(MediaType.APPLICATION_JSON)
    public Object createQuery() throws Exception {
        return createQuery("select * from hive.information_schema.tables",
                "table_catalog", "table_schema", "table_name", "table_type");
    }

    @GET
    @Path("/create-table")
    @Produces(MediaType.APPLICATION_JSON)
    public int createTable(@QueryParam("name") @NotEmpty String name) throws Exception {
        return executeSQL(String.format("create table %s (id integer, name varchar(100))", name));
    }

    @GET
    @Path("/insert")
    @Produces(MediaType.APPLICATION_JSON)
    public Object insert() throws Exception {
        return executeSQL("insert into madatest values (3, 'quheng_many')");
    }

   /**
    * doesn't work
    * I do not found any support about UPDATE in  https://prestodb.io/docs/current/
    */
    @GET
    @Path("/update")
    @Produces(MediaType.APPLICATION_JSON)
    public Object update() throws Exception {
        return executeSQL("update madatest set id = 4 where name = quheng_many" );
    }

    @GET
    @Path("/groupby")
    @Produces(MediaType.APPLICATION_JSON)
    public Object groupby() throws Exception {
        return query("select sum(id) as sum , name from madatest group by name", "name", "sum");
    }

    @GET
    @Path("/orderby")
    @Produces(MediaType.APPLICATION_JSON)
    public Object orderby() throws Exception {
        return query("select id, name from madatest order by id desc", "id", "name");
    }

   /**
    * doesn't work
    * Access Denied
    *
    * all operator about ALTER will access denied.
    */
    @GET
    @Path("/drop")
    @Produces(MediaType.APPLICATION_JSON)
    public Object drop() throws Exception {
        return query("drop table testint");
    }

    /**
     *
     * @return {"name":"VARCHAR(100)","id":"INTEGER"}
     * @throws Exception
     */
    @GET
    @Path("/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadata() throws Exception {
        return queryMetadata("select * from testtable");
    }

    /**
     *
     * @return {"id":"INTEGER"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-item")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataItem() throws Exception {
        return queryMetadata("select id from testtable");
    }

    /**
     *
     * @return {"a":"INTEGER","b":"INTEGER","_col2":"INTEGER"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-plus")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataPlus() throws Exception {
        return queryMetadata("select a, b, a+b+c from testplus");
    }

    /**
     *
     * @return {"name":"VARCHAR(100)","_col1":"BIGINT"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-count")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataCount() throws Exception {
        return queryMetadata("select name, count(*) from madatest group by name");
    }

    /**
     *
     * @return {"_col0":"BOOLEAN"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-comparison")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataComparison() throws Exception {
        return queryMetadata("SELECT 'Paul' BETWEEN 'John' AND 'Ringo'");
    }

    /**
     *
     * @return {"_col0":"DOUBLE","_col2":"DOUBLE","_col1":"DOUBLE","_col4":"DOUBLE","_col3":"DOUBLE","_col5":"INTEGER"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-math")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataMath() throws Exception {
        return queryMetadata("SELECT infinity(),e(),pi(),cos(a),sqrt(b),abs(c) FROM testplus");
    }

    /**
     *
     * @return {"_col0":"VARBINARY"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-string")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataString() throws Exception {
        return queryMetadata(" select to_utf8('中文测试')");
    }

    /**
     *
     * @return {"_col0":"VARBINARY"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-binary")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataBinary() throws Exception {
        return queryMetadata("select from_base64url('JiMyMDAxMzsmIzI1OTkxOyYjMjc5Nzk7JiMzNTc5NzsNCg==')");
    }

    /**
     *
     * @return {"_col0":"BIGINT"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-time")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataTime() throws Exception {
        return queryMetadata("select day(time) from testinsert limit 10");
    }

    /**
     *
     * @return {"name":"VARCHAR(100)","_col1":"MAP(INTEGER,BIGINT)"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-map")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataMap() throws Exception {
        return queryMetadata("select name, histogram(id) from madatest group by name");
    }

    /**
     *
     * @return {"_col0":"COLOR"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-color")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataColor() throws Exception {
        return queryMetadata("select color('RED')");
    }

    /**
     *
     * @return {"_col0":"ARRAY(INTEGER)"}
     * @throws Exception
     */
    @GET
    @Path("/metadata-array")
    @Produces(MediaType.APPLICATION_JSON)
    public Object metadataArray() throws Exception {
        return queryMetadata("select array_distinct(array[1,2,3,2,14])");
    }
}
