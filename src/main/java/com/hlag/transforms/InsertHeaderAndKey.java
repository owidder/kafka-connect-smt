/* Licensed under Apache-2.0 */
package com.hlag.transforms;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
public class InsertHeaderAndKey<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Add a header to each record. Add a UUID or a literal as key if there is no key.";

    public static final String HEADER_FIELD = "header";
    public static final String VALUE_LITERAL_FIELD = "value.literal";
    public static final String KEY_LITERAL_FIELD = "key.literal";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The name of the header.")
            .define(VALUE_LITERAL_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The literal value that is to be set as the header value on all records.")
            .define(KEY_LITERAL_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.LOW,
                    "The literal value that is to be set as the key value on all records. If not set, use a UUID");

    private String header;

    private SchemaAndValue literalValue;

    private String keyLiteral;

    @Override
    public R apply(R record) {
        Object newKey = record.key() != null ? record.key() : (keyLiteral != null ? keyLiteral : UUID.randomUUID().toString());
        Headers updatedHeaders = record.headers().duplicate();
        updatedHeaders.add(header, literalValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), newKey,
                record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        header = config.getString(HEADER_FIELD);
        keyLiteral = config.getString(KEY_LITERAL_FIELD);
        literalValue = Values.parseString(config.getString(VALUE_LITERAL_FIELD));
    }
}
