package com.linkedin.metadata.aspect.validators;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.validation.StructuredPropertiesValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertiesValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  @Test
  public void testValidateAspectNumberUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    StructuredPropertyDefinition numberPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));

    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, numberPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(0.0)));
    numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, numberPropertyDef))
            .count(),
        1,
        "Should have raised exception for disallowed value 0.0");

    // Assign string value to number property
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, numberPropertyDef))
            .count(),
        2,
        "Should have raised exception for mis-matched types `string` vs `number` && `hello` is not a valid value of [90.0, 30.0, 60.0]");
  }

  @Test
  public void testValidateAspectDateUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    // Assign string value
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(
                Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime"))
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    // Assign invalid date
    StructuredPropertyDefinition datePropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.date"));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, datePropertyDef))
            .count(),
        1,
        "Should have raised exception for mis-matched types");

    // Assign valid date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(datePayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, datePropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);
  }

  @Test
  public void testValidateAspectStringUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    // Assign string value
    StructuredPropertyValueAssignment stringAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("hello")));
    StructuredProperties stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(stringAssignment));

    // Assign date
    StructuredPropertyValueAssignment dateAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(
                new PrimitivePropertyValueArray(PrimitivePropertyValue.create("2023-10-24")));
    StructuredProperties datePayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(dateAssignment));

    // Assign number
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    StructuredPropertyDefinition stringPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create("hello")),
                        new PropertyValue()
                            .setValue(PrimitivePropertyValue.create("2023-10-24")))));

    // Valid strings (both the date value and "hello" are valid)

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, stringPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);
    isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(datePayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, stringPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    // Invalid: assign a number to the string property
    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, stringPropertyDef))
            .count(),
        2,
        "Should have raised exception for mis-matched types. The double 30.0 is not a `string` && not one of the allowed types `2023-10-24` or `hello`");

    // Invalid allowedValue

    assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("not hello")));
    stringPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(stringPayload, TEST_REGISTRY),
                new MockAspectRetriever(propertyUrn, stringPropertyDef))
            .count(),
        1,
        "Should have raised exception for disallowed value `not hello`");
  }

  @Test
  public void testValidateSoftDeletedUpsert() throws URISyntaxException {
    Urn propertyUrn =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

    StructuredPropertyDefinition numberPropertyDef =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.number"))
            .setAllowedValues(
                new PropertyValueArray(
                    List.of(
                        new PropertyValue().setValue(PrimitivePropertyValue.create(30.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(60.0)),
                        new PropertyValue().setValue(PrimitivePropertyValue.create(90.0)))));

    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(propertyUrn)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(30.0)));
    StructuredProperties numberPayload =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    boolean isValid =
        StructuredPropertiesValidator.validateProposedUpserts(
                    TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                    new MockAspectRetriever(propertyUrn, numberPropertyDef))
                .count()
            == 0;
    Assert.assertTrue(isValid);

    assertEquals(
        StructuredPropertiesValidator.validateProposedUpserts(
                TestMCP.ofOneUpsertItemDatasetUrn(numberPayload, TEST_REGISTRY),
                new MockAspectRetriever(
                    propertyUrn, numberPropertyDef, new Status().setRemoved(true)))
            .count(),
        1,
        "Should have raised exception for soft deleted definition");
  }
}
