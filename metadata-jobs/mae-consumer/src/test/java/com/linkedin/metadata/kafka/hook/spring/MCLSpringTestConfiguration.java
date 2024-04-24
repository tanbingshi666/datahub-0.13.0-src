package com.linkedin.metadata.kafka.hook.spring;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Authentication;
import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.kafka",
      "com.linkedin.gms.factory.entity.update.indices",
      "com.linkedin.gms.factory.timeline.eventgenerator"
    })
public class MCLSpringTestConfiguration {

  @MockBean public EntityRegistry entityRegistry;

  @MockBean public ElasticSearchGraphService graphService;

  @MockBean public TimeseriesAspectService timeseriesAspectService;

  @MockBean public SystemMetadataService systemMetadataService;

  @MockBean public SearchDocumentTransformer searchDocumentTransformer;

  @MockBean public IngestionScheduler ingestionScheduler;

  @Bean(name = "systemEntityClient")
  public SystemEntityClient systemEntityClient(
      @Qualifier("systemAuthentication") Authentication systemAuthentication) {
    SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);
    when(systemEntityClient.getSystemAuthentication()).thenReturn(systemAuthentication);
    return systemEntityClient;
  }

  @MockBean public ElasticSearchService searchService;

  @MockBean public EntityService<?> entityService;

  @MockBean public FormService formService;

  @MockBean(name = "cachingAspectRetriever")
  CachingAspectRetriever cachingAspectRetriever;

  @MockBean(name = "systemAuthentication")
  public Authentication systemAuthentication;

  @MockBean(name = "dataHubUpgradeKafkaListener")
  public DataHubUpgradeKafkaListener dataHubUpgradeKafkaListener;

  @MockBean(name = "duheSchemaRegistryConfig")
  public SchemaRegistryConfig schemaRegistryConfig;

  @MockBean(name = "duheKafkaConsumerFactory")
  public DefaultKafkaConsumerFactory<String, GenericRecord> defaultKafkaConsumerFactory;

  @MockBean public SchemaRegistryService schemaRegistryService;

  @MockBean public EntityIndexBuilders entityIndexBuilders;

  @Bean(name = "systemOperationContext")
  public OperationContext operationContext(
      final EntityRegistry entityRegistry,
      @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      final IndexConvention indexConvention) {
    when(systemAuthentication.getActor())
        .thenReturn(TestOperationContexts.TEST_SYSTEM_AUTH.getActor());
    return OperationContext.asSystem(
        OperationContextConfig.builder().build(),
        entityRegistry,
        systemAuthentication,
        indexConvention);
  }
}
