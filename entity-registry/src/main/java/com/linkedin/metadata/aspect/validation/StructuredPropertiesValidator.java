package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.aspect.validation.PropertyDefinitionValidator.softDeleteCheck;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** A Validator for StructuredProperties Aspect that is attached to entities like Datasets, etc. */
@Slf4j
public class StructuredPropertiesValidator extends AspectPayloadValidator {

  private static final Set<LogicalValueType> VALID_VALUE_STORED_AS_STRING =
      new HashSet<>(
          Arrays.asList(
              LogicalValueType.STRING,
              LogicalValueType.RICH_TEXT,
              LogicalValueType.DATE,
              LogicalValueType.URN));

  public StructuredPropertiesValidator(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  public static LogicalValueType getLogicalValueType(Urn valueType) {
    String valueTypeId = getValueTypeId(valueType);
    if (valueTypeId.equals("string")) {
      return LogicalValueType.STRING;
    } else if (valueTypeId.equals("date")) {
      return LogicalValueType.DATE;
    } else if (valueTypeId.equals("number")) {
      return LogicalValueType.NUMBER;
    } else if (valueTypeId.equals("urn")) {
      return LogicalValueType.URN;
    } else if (valueTypeId.equals("rich_text")) {
      return LogicalValueType.RICH_TEXT;
    }

    return LogicalValueType.UNKNOWN;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems, @Nonnull AspectRetriever aspectRetriever) {
    return validateProposedUpserts(
        mcpItems.stream()
            .filter(i -> ChangeType.UPSERT.equals(i.getChangeType()))
            .collect(Collectors.toList()),
        aspectRetriever);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, AspectRetriever aspectRetriever) {
    return Stream.empty();
  }

  public static Stream<AspectValidationException> validateProposedUpserts(
      @Nonnull Collection<BatchItem> mcpItems, @Nonnull AspectRetriever aspectRetriever) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // Validate propertyUrns
    Set<Urn> validPropertyUrns = validateStructuredPropertyUrns(mcpItems, exceptions);

    // Fetch property aspects for further validation
    Map<Urn, Map<String, Aspect>> allStructuredPropertiesAspects =
        fetchPropertyAspects(validPropertyUrns, aspectRetriever);

    // Validate assignments
    for (BatchItem i : exceptions.successful(mcpItems)) {
      for (StructuredPropertyValueAssignment structuredPropertyValueAssignment :
          i.getAspect(StructuredProperties.class).getProperties()) {

        Urn propertyUrn = structuredPropertyValueAssignment.getPropertyUrn();
        Map<String, Aspect> propertyAspects =
            allStructuredPropertiesAspects.getOrDefault(propertyUrn, Collections.emptyMap());

        // check definition soft delete
        softDeleteCheck(i, propertyAspects, "Cannot apply a soft deleted Structured Property value")
            .ifPresent(exceptions::addException);

        Aspect structuredPropertyDefinitionAspect =
            propertyAspects.get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
        if (structuredPropertyDefinitionAspect == null) {
          exceptions.addException(i, "Unexpected null value found.");
        }

        StructuredPropertyDefinition structuredPropertyDefinition =
            new StructuredPropertyDefinition(structuredPropertyDefinitionAspect.data());
        log.warn(
            "Retrieved property definition for {}. {}", propertyUrn, structuredPropertyDefinition);
        if (structuredPropertyDefinition != null) {
          PrimitivePropertyValueArray values = structuredPropertyValueAssignment.getValues();
          // Check cardinality
          if (structuredPropertyDefinition.getCardinality() == PropertyCardinality.SINGLE) {
            if (values.size() > 1) {
              exceptions.addException(
                  i,
                  "Property: "
                      + propertyUrn
                      + " has cardinality 1, but multiple values were assigned: "
                      + values);
            }
          }

          // Check values
          for (PrimitivePropertyValue value : values) {
            validateType(i, propertyUrn, structuredPropertyDefinition, value)
                .ifPresent(exceptions::addException);
            validateAllowedValues(i, propertyUrn, structuredPropertyDefinition, value)
                .ifPresent(exceptions::addException);
          }
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  private static Set<Urn> validateStructuredPropertyUrns(
      Collection<BatchItem> mcpItems, ValidationExceptionCollection exceptions) {
    Set<Urn> validPropertyUrns = new HashSet<>();

    for (BatchItem i : exceptions.successful(mcpItems)) {
      StructuredProperties structuredProperties = i.getAspect(StructuredProperties.class);

      log.warn("Validator called with {}", structuredProperties);
      Map<Urn, List<StructuredPropertyValueAssignment>> structuredPropertiesMap =
          structuredProperties.getProperties().stream()
              .collect(
                  Collectors.groupingBy(
                      x -> x.getPropertyUrn(),
                      HashMap::new,
                      Collectors.toCollection(ArrayList::new)));
      for (Map.Entry<Urn, List<StructuredPropertyValueAssignment>> entry :
          structuredPropertiesMap.entrySet()) {

        // There should only be one entry per structured property
        List<StructuredPropertyValueAssignment> values = entry.getValue();
        if (values.size() > 1) {
          exceptions.addException(
              i, "Property: " + entry.getKey() + " has multiple entries: " + values);
        } else {
          for (StructuredPropertyValueAssignment structuredPropertyValueAssignment :
              structuredProperties.getProperties()) {
            Urn propertyUrn = structuredPropertyValueAssignment.getPropertyUrn();

            if (!propertyUrn.getEntityType().equals("structuredProperty")) {
              exceptions.addException(
                  i,
                  "Unexpected entity type. Expected: structuredProperty Found: "
                      + propertyUrn.getEntityType());
            } else {
              validPropertyUrns.add(propertyUrn);
            }
          }
        }
      }
    }

    return validPropertyUrns;
  }

  private static Optional<AspectValidationException> validateAllowedValues(
      BatchItem item,
      Urn propertyUrn,
      StructuredPropertyDefinition definition,
      PrimitivePropertyValue value) {
    if (definition.getAllowedValues() != null) {
      Set<PrimitivePropertyValue> definedValues =
          definition.getAllowedValues().stream()
              .map(PropertyValue::getValue)
              .collect(Collectors.toSet());
      if (definedValues.stream().noneMatch(definedPrimitive -> definedPrimitive.equals(value))) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Property: %s, value: %s should be one of %s",
                    propertyUrn, value, definedValues)));
      }
    }
    return Optional.empty();
  }

  private static Optional<AspectValidationException> validateType(
      BatchItem item,
      Urn propertyUrn,
      StructuredPropertyDefinition definition,
      PrimitivePropertyValue value) {
    Urn valueType = definition.getValueType();
    LogicalValueType typeDefinition = getLogicalValueType(valueType);

    // Primitive Type Validation
    if (VALID_VALUE_STORED_AS_STRING.contains(typeDefinition)) {
      log.debug(
          "Property definition demands a string value. {}, {}", value.isString(), value.isDouble());
      if (value.getString() == null) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                "Property: "
                    + propertyUrn.toString()
                    + ", value: "
                    + value
                    + " should be a string"));
      } else if (typeDefinition.equals(LogicalValueType.DATE)) {
        if (!StructuredPropertyUtils.isValidDate(value)) {
          return Optional.of(
              AspectValidationException.forItem(
                  item,
                  "Property: "
                      + propertyUrn.toString()
                      + ", value: "
                      + value
                      + " should be a date with format YYYY-MM-DD"));
        }
      } else if (typeDefinition.equals(LogicalValueType.URN)) {
        StringArrayMap valueTypeQualifier = definition.getTypeQualifier();
        Urn typeValue;
        try {
          typeValue = Urn.createFromString(value.getString());
        } catch (URISyntaxException e) {
          return Optional.of(
              AspectValidationException.forItem(
                  item,
                  "Property: " + propertyUrn.toString() + ", value: " + value + " should be an urn",
                  e));
        }
        if (valueTypeQualifier != null) {
          if (valueTypeQualifier.containsKey("allowedTypes")) {
            // Let's get the allowed types and validate that the value is one of those types
            StringArray allowedTypes = valueTypeQualifier.get("allowedTypes");
            boolean matchedAny = false;
            for (String type : allowedTypes) {
              Urn typeUrn = null;
              try {
                typeUrn = Urn.createFromString(type);
              } catch (URISyntaxException e) {

                // we don't expect to have types that we allowed to be written that aren't
                // urns
                throw new RuntimeException(e);
              }
              String allowedEntityName = getValueTypeId(typeUrn);
              if (typeValue.getEntityType().equals(allowedEntityName)) {
                matchedAny = true;
              }
            }
            if (!matchedAny) {
              return Optional.of(
                  AspectValidationException.forItem(
                      item,
                      "Property: "
                          + propertyUrn.toString()
                          + ", value: "
                          + value
                          + " is not of any supported urn types:"
                          + allowedTypes));
            }
          }
        }
      }
    } else if (typeDefinition.equals(LogicalValueType.NUMBER)) {
      log.debug("Property definition demands a numeric value. {}, {}", value.isString(), value);
      try {
        Double doubleValue =
            value.getDouble() != null ? value.getDouble() : Double.parseDouble(value.getString());
      } catch (NumberFormatException | NullPointerException e) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                "Property: "
                    + propertyUrn.toString()
                    + ", value: "
                    + value
                    + " should be a number"));
      }
    } else {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              "Validation support for type "
                  + definition.getValueType()
                  + " is not yet implemented."));
    }

    return Optional.empty();
  }

  private static String getValueTypeId(@Nonnull final Urn valueType) {
    String valueTypeId = valueType.getId();
    if (valueTypeId.startsWith("datahub.")) {
      valueTypeId = valueTypeId.split("\\.")[1];
    }
    return valueTypeId;
  }

  private static Map<Urn, Map<String, Aspect>> fetchPropertyAspects(
      Set<Urn> structuredPropertyUrns, AspectRetriever aspectRetriever) {
    if (structuredPropertyUrns.isEmpty()) {
      return Collections.emptyMap();
    } else {
      try {
        return aspectRetriever.getLatestAspectObjects(
            structuredPropertyUrns,
            ImmutableSet.of(
                Constants.STATUS_ASPECT_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
