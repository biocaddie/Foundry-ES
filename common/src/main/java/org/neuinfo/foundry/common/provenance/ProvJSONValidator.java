package org.neuinfo.foundry.common.provenance;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.openprovenance.prov.model.ProvFactory;

import java.io.IOException;

/**
 * Created by bozyurt on 5/21/14.
 */
public class ProvJSONValidator {
    private static ProvFactory provFactory = new org.openprovenance.prov.xml.ProvFactory();
    final JsonSchema schema;

    public ProvJSONValidator() throws IOException, ProcessingException {
        JsonNode schemaJSON = JsonLoader.fromResource("/schema/prov-json-schema-v4.js");
        JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
        this.schema = factory.getJsonSchema(schemaJSON);
    }

    public boolean validate(String provSonFile) throws IOException, ProcessingException {
        final JsonNode json = JsonLoader.fromPath(provSonFile);
        ProcessingReport report = schema.validate(json);
        if (!report.isSuccess()) {

            System.err.println(report);
            return false;
        }
        return true;
    }
}
