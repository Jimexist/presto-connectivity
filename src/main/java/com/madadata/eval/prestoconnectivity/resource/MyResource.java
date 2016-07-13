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

    /*
    does not work well, error info:
    com.facebook.presto.jdbc.NotImplementedException: Method Connection.prepareStatement is not yet implemented
     */
    private List<Map<String, Object>> creatQuery(String sql, String... fields) throws SQLException {
        Set<String> fieldSet = ImmutableSet.copyOf(fields);
        try (Handle handle = jdbi.open()) {
            Query<Map<String, Object>> querySet = handle.createQuery(sql);
            return querySet.list();
        }
    }

    /*
    does not work well, error info:
    com.facebook.presto.jdbc.NotImplementedException: Method Connection.prepareStatement is not yet implemented
     */
    private int jdbiCreate(String sql) throws SQLException {
        try (Handle handle = jdbi.open()) {
            handle.execute(sql);
            return 0;
        }
    }


    private int executeCreate(String sql)throws SQLException {
        try (Handle handle = jdbi.open()) {
            Statement statement = handle.getConnection().createStatement();
            return statement.executeUpdate(sql);
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

    @GET
    @Path("/create")
    @Produces(MediaType.APPLICATION_JSON)
    public int create(@QueryParam("name") @NotEmpty String name) throws Exception {
        return jdbiCreate(String.format("create table %s (id int primary key, name varchar(100))", name));
    }

    @GET
    @Path("/create-table")
    @Produces(MediaType.APPLICATION_JSON)
    public int createTable(@QueryParam("name") @NotEmpty String name) throws Exception {
        return executeCreate(String.format("create table %s (id int primary key, name varchar(100))", name));
    }


    @GET
    @Path("/createquery")
    @Produces(MediaType.APPLICATION_JSON)
    public Object createQuery() throws Exception {
        return creatQuery("select * from hive.information_schema.tables",
                "table_catalog", "table_schema", "table_name", "table_type");
    }
}
