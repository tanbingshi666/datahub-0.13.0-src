package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.form.FormAssignmentHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.Topics;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
@Import({
  UpdateIndicesHook.class,
  IngestionSchedulerHook.class,
  EntityChangeEventGeneratorHook.class,
  KafkaEventConsumerFactory.class,
  SiblingAssociationHook.class,
  FormAssignmentHook.class,
  IncidentsSummaryHook.class,
})
@EnableKafka
public class MetadataChangeLogProcessor {

  @Getter private final List<MetadataChangeLogHook> hooks;
  private final Histogram kafkaLagStats =
      MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Autowired
  public MetadataChangeLogProcessor(List<MetadataChangeLogHook> metadataChangeLogHooks) {
    this.hooks =
        metadataChangeLogHooks.stream()
            .filter(MetadataChangeLogHook::isEnabled)
            .sorted(Comparator.comparing(MetadataChangeLogHook::executionOrder))
            .collect(Collectors.toList());
    log.info(
        "Enabled hooks: {}",
        this.hooks.stream()
            .map(hook -> hook.getClass().getSimpleName())
            .collect(Collectors.toList()));
    this.hooks.forEach(MetadataChangeLogHook::init);
  }

  @KafkaListener(
      id = "${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}",
      topics = {
        "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
        "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}"
      },
      containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try (Timer.Context i = MetricUtils.timer(this.getClass(), "consume").time()) {
      kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
      final GenericRecord record = consumerRecord.value();
      log.debug(
          "Got Generic MCL on topic: {}, partition: {}, offset: {}",
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset());
      MetricUtils.counter(this.getClass(), "received_mcl_count").inc();

      MetadataChangeLog event;
      try {
        event = EventUtils.avroToPegasusMCL(record);
        log.debug(
            "Successfully converted Avro MCL to Pegasus MCL. urn: {}, key: {}",
            event.getEntityUrn(),
            event.getEntityKeyAspect());
      } catch (Exception e) {
        MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
        log.error("Error deserializing message due to: ", e);
        log.error("Message: {}", record.toString());
        return;
      }

      log.debug(
          "Invoking MCL hooks for urn: {}, key: {}",
          event.getEntityUrn(),
          event.getEntityKeyAspect());

      // Here - plug in additional "custom processor hooks"
      for (MetadataChangeLogHook hook : this.hooks) {
        if (!hook.isEnabled()) {
          log.debug(String.format("Skipping disabled hook %s", hook.getClass()));
          continue;
        }
        try (Timer.Context ignored =
            MetricUtils.timer(this.getClass(), hook.getClass().getSimpleName() + "_latency")
                .time()) {
          hook.invoke(event);
        } catch (Exception e) {
          // Just skip this hook and continue. - Note that this represents "at most once"//
          // processing.
          MetricUtils.counter(this.getClass(), hook.getClass().getSimpleName() + "_failure").inc();
          log.error(
              "Failed to execute MCL hook with name {}", hook.getClass().getCanonicalName(), e);
        }
      }
      // TODO: Manually commit kafka offsets after full processing.
      MetricUtils.counter(this.getClass(), "consumed_mcl_count").inc();
      log.debug(
          "Successfully completed MCL hooks for urn: {}, key: {}",
          event.getEntityUrn(),
          event.getEntityKeyAspect());
    }
  }
}
