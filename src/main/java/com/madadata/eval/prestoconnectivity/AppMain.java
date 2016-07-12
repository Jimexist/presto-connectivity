package com.madadata.eval.prestoconnectivity;

import com.madadata.eval.prestoconnectivity.resource.MyResource;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.skife.jdbi.v2.DBI;

/**
 * Created by jiayu on 7/12/16.
 */
public class AppMain extends Application<AppConfig> {

    @Override
    public void initialize(Bootstrap<AppConfig> bootstrap) {
        // Enable variable substitution with environment variables
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)
                )
        );

    }

    public static void main(String[] args) throws Exception {
        new AppMain().run(args);
    }

    @Override
    public void run(AppConfig appConfig, Environment environment) throws Exception {
        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(environment, appConfig.getDataSourceFactory(), "presto");
        environment.jersey().register(new MyResource(jdbi));
    }
}
