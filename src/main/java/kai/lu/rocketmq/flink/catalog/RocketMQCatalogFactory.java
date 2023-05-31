package kai.lu.rocketmq.flink.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static kai.lu.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.DEFAULT_DATABASE;
import static kai.lu.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.IDENTIFIER;
import static kai.lu.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.NAME_SERVER_ADDR;
import static kai.lu.rocketmq.flink.catalog.RocketMQCatalogFactoryOptions.SCHEMA_REGISTRY_BASE_URL;

/** The {@CatalogFactory} implementation of RocketMQ. */
public class RocketMQCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        return new RocketMQCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(NAME_SERVER_ADDR),
                helper.getOptions().get(SCHEMA_REGISTRY_BASE_URL));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(NAME_SERVER_ADDR);
        options.add(SCHEMA_REGISTRY_BASE_URL);
        return options;
    }
}
