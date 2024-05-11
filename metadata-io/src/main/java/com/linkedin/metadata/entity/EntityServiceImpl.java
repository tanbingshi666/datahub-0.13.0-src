package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToAspectMap;
import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.PegasusUtils.getDataTemplateClassFromSchema;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.codahale.metrics.Timer;
import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.common.urn.VersionedUrnUtils;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.ebean.PagedList;
import io.ebean.Transaction;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityNotFoundException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class specifying create, update, and read operations against metadata entities and aspects by
 * primary key (urn).
 *
 * <p>This interface is meant to abstract away the storage concerns of these pieces of metadata,
 * permitting any underlying storage system to be used in materializing GMS domain objects, which
 * are implemented using Pegasus {@link RecordTemplate}s.
 *
 * <p>Internal versioning semantics =============================
 *
 * <p>The latest version of any aspect is set to 0 for efficient retrieval; in most cases the latest
 * state of an aspect will be the only fetched.
 *
 * <p>As such, 0 is treated as a special number. Once an aspect is no longer the latest, versions
 * will increment monotonically, starting from 1. Thus, the second-to-last version of an aspect will
 * be equal to total # versions of the aspect - 1.
 *
 * <p>For example, if there are 5 instances of a single aspect, the latest will have version 0, and
 * the second-to-last will have version 4. The "true" latest version of an aspect is always equal to
 * the highest stored version of a given aspect + 1.
 *
 * <p>Note that currently, implementations of this interface are responsible for producing Metadata
 * Change Log on ingestion using {@link #conditionallyProduceMCLAsync(RecordTemplate,
 * SystemMetadata, RecordTemplate, SystemMetadata, MetadataChangeProposal, Urn, AuditStamp,
 * AspectSpec)}.
 *
 * <p>TODO: Consider whether we can abstract away virtual versioning semantics to subclasses of this
 * class.
 */
@Slf4j
public class EntityServiceImpl implements EntityService<ChangeItemImpl> {

  /**
   * As described above, the latest version of an aspect should <b>always</b> take the value 0, with
   * monotonically increasing version incrementing as usual once the latest version is replaced.
   */
  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  protected final AspectDao aspectDao;

  @VisibleForTesting
  @Getter
  private final EventProducer producer;
  private final EntityRegistry entityRegistry;
  private final Map<String, Set<String>> entityToValidAspects;
  private RetentionService<ChangeItemImpl> retentionService;
  private final Boolean alwaysEmitChangeLog;
  @Nullable
  @Getter
  private SearchIndicesService updateIndicesService;
  private final PreProcessHooks preProcessHooks;
  protected static final int MAX_KEYS_PER_QUERY = 500;

  private final Integer ebeanMaxTransactionRetry;
  private final boolean enableBrowseV2;

  public EntityServiceImpl(@Nonnull final AspectDao aspectDao, @Nonnull final EventProducer producer,
      @Nonnull final EntityRegistry entityRegistry, final boolean alwaysEmitChangeLog,
      final PreProcessHooks preProcessHooks, final boolean enableBrowsePathV2) {
    this(aspectDao, producer, entityRegistry, alwaysEmitChangeLog, preProcessHooks, DEFAULT_MAX_TRANSACTION_RETRY,
        enableBrowsePathV2);
  }

  public EntityServiceImpl(@Nonnull final AspectDao aspectDao, @Nonnull final EventProducer producer,
      @Nonnull final EntityRegistry entityRegistry, final boolean alwaysEmitChangeLog,
      final PreProcessHooks preProcessHooks, @Nullable final Integer retry, final boolean enableBrowseV2) {

    this.aspectDao = aspectDao;
    this.producer = producer;
    this.entityRegistry = entityRegistry;
    entityToValidAspects = EntityUtils.buildEntityToValidAspects(entityRegistry);
    this.alwaysEmitChangeLog = alwaysEmitChangeLog;
    this.preProcessHooks = preProcessHooks;
    ebeanMaxTransactionRetry = retry != null ? retry : DEFAULT_MAX_TRANSACTION_RETRY;
    this.enableBrowseV2 = enableBrowseV2;
  }

  public void setUpdateIndicesService(@Nullable SearchIndicesService updateIndicesService) {
    this.updateIndicesService = updateIndicesService;
    if (this.updateIndicesService != null) {
      this.updateIndicesService.initializeAspectRetriever(this);
    }
  }

  @Override
  public RecordTemplate getLatestAspect(@Nonnull Urn urn, @Nonnull String aspectName) {
    log.debug("Invoked getLatestAspect with urn {}, aspect {}", urn, aspectName);
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  /**
   * Retrieves the latest aspects corresponding to a batch of {@link Urn}s based on a provided set
   * of aspect names.
   *
   * @param urns set of urns to fetch aspects for
   * @param aspectNames aspects to fetch for each urn in urns set
   * @return a map of provided {@link Urn} to a List containing the requested aspects.
   */
  @Override
  public Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = getLatestAspect(urns, aspectNames);

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();

    // Each urn should have some result, regardless of whether aspects are found in the DB.
    for (Urn urn : urns) {
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
    }

    // Add "key" aspects for each urn. TODO: Replace this with a materialized key aspect.
    urnToAspects.keySet().forEach(key -> {
      final RecordTemplate keyAspect = EntityUtils.buildKeyAspect(entityRegistry, key);
      urnToAspects.get(key).add(keyAspect);
    });

    List<SystemAspect> systemAspects = EntityUtils.toSystemAspects(batchGetResults.values(), this);

    systemAspects.stream()
        // for now, don't add the key aspect here we have already added it above
        .filter(systemAspect -> !getKeyAspectName(systemAspect.getUrn()).equals(systemAspect.getAspectName()))
        .forEach(systemAspect -> urnToAspects.computeIfAbsent(systemAspect.getUrn(), u -> new ArrayList<>())
            .add(systemAspect.getRecordTemplate()));

    return urnToAspects;
  }

  @Nonnull
  @Override
  public Map<String, RecordTemplate> getLatestAspectsForUrn(@Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames) {
    Map<EntityAspectIdentifier, EntityAspect> batchGetResults =
        getLatestAspect(new HashSet<>(Arrays.asList(urn)), aspectNames);

    return EntityUtils.toSystemAspects(batchGetResults.values(), this)
        .stream()
        .map(systemAspect -> Pair.of(systemAspect.getAspectName(), systemAspect.getRecordTemplate()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Retrieves an aspect having a specific {@link Urn}, name, & version.
   *
   * <p>Note that once we drop support for legacy aspect-specific resources, we should make this a
   * protected method. Only visible for backwards compatibility.
   *
   * @param urn an urn associated with the requested aspect
   * @param aspectName name of the aspect requested
   * @param version specific version of the aspect being requests
   * @return the {@link RecordTemplate} representation of the requested aspect object, or null if
   *     one cannot be found
   */
  @Nullable
  @Override
  public RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, long version) {
    return getAspectVersionPair(urn, aspectName, version).getFirst();
  }

  public Pair<RecordTemplate, Long> getAspectVersionPair(@Nonnull final Urn urn, @Nonnull final String aspectName,
      long version) {

    log.debug("Invoked getAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    version = calculateVersionNumber(urn, aspectName, version);
    final EntityAspectIdentifier primaryKey = new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect = Optional.ofNullable(aspectDao.getAspect(primaryKey));

    return Pair.of(
        EntityUtils.toSystemAspect(maybeAspect.orElse(null), this).map(SystemAspect::getRecordTemplate).orElse(null),
        version);
  }

  /**
   * Retrieves the latest aspects for the given urn as dynamic aspect objects (Without having to
   * define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urn urn of entity to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Nullable
  @Override
  public EntityResponse getEntityV2(@Nonnull final String entityName, @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getEntitiesV2(entityName, Collections.singleton(urn), aspectNames).get(urn);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @WithSpan
  @Override
  public Map<Urn, EntityResponse> getEntitiesV2(@Nonnull final String entityName, @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getLatestEnvelopedAspects(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> EntityUtils.toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the aspects for the given set of urns and versions as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Override
  public Map<Urn, EntityResponse> getEntitiesVersionedV2(@Nonnull final Set<VersionedUrn> versionedUrns,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getVersionedEnvelopedAspects(versionedUrns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> EntityUtils.toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EntityAspect.EntitySystemAspect} object
   */
  @Override
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(@Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {

    final Set<EntityAspectIdentifier> dbKeys = urns.stream()
        .map(urn -> aspectNames.stream()
            .map(aspectName -> new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    return getCorrespondingAspects(dbKeys, urns);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  @Override
  public Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(@Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {

    Map<String, Map<String, Long>> urnAspectVersionMap = versionedUrns.stream()
        .collect(Collectors.toMap(versionedUrn -> versionedUrn.getUrn().toString(),
            versionedUrn -> VersionedUrnUtils.convertVersionStamp(versionedUrn.getVersionStamp())));

    // Cover full/partial versionStamp
    final Set<EntityAspectIdentifier> dbKeys = urnAspectVersionMap.entrySet()
        .stream()
        .filter(entry -> !entry.getValue().isEmpty())
        .map(entry -> aspectNames.stream()
            .filter(aspectName -> entry.getValue().containsKey(aspectName))
            .map(aspectName -> new EntityAspectIdentifier(entry.getKey(), aspectName, entry.getValue().get(aspectName)))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    // Cover empty versionStamp
    dbKeys.addAll(urnAspectVersionMap.entrySet()
        .stream()
        .filter(entry -> entry.getValue().isEmpty())
        .map(entry -> aspectNames.stream()
            .map(aspectName -> new EntityAspectIdentifier(entry.getKey(), aspectName, 0L))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet()));

    return getCorrespondingAspects(dbKeys, versionedUrns.stream()
        .map(versionedUrn -> versionedUrn.getUrn().toString())
        .map(UrnUtils::getUrn)
        .collect(Collectors.toSet()));
  }

  private Map<Urn, List<EnvelopedAspect>> getCorrespondingAspects(Set<EntityAspectIdentifier> dbKeys, Set<Urn> urns) {

    final Map<EntityAspectIdentifier, EnvelopedAspect> envelopedAspectMap = getEnvelopedAspects(dbKeys);

    // Group result by Urn
    final Map<String, List<EnvelopedAspect>> urnToAspects = envelopedAspectMap.entrySet()
        .stream()
        .collect(Collectors.groupingBy(entry -> entry.getKey().getUrn(),
            Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

    final Map<Urn, List<EnvelopedAspect>> result = new HashMap<>();
    for (Urn urn : urns) {
      List<EnvelopedAspect> aspects = urnToAspects.getOrDefault(urn.toString(), Collections.emptyList());
      EnvelopedAspect keyAspect = EntityUtils.getKeyEnvelopedAspect(urn, entityRegistry);
      // Add key aspect if it does not exist in the returned aspects
      if (aspects.isEmpty() || aspects.stream().noneMatch(aspect -> keyAspect.getName().equals(aspect.getName()))) {
        result.put(urn, ImmutableList.<EnvelopedAspect>builder().addAll(aspects).add(keyAspect).build());
      } else {
        result.put(urn, aspects);
      }
    }
    return result;
  }

  /**
   * Retrieves the latest aspect for the given urn as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  @Override
  public EnvelopedAspect getLatestEnvelopedAspect(@Nonnull final String entityName, @Nonnull final Urn urn,
      @Nonnull final String aspectName) throws Exception {
    return getLatestEnvelopedAspects(ImmutableSet.of(urn), ImmutableSet.of(aspectName)).getOrDefault(urn,
            Collections.emptyList())
        .stream()
        .filter(envelopedAspect -> envelopedAspect.getName().equals(aspectName))
        .findFirst()
        .orElse(null);
  }

  /** Retrieves an {@link VersionedAspect}, or null if one cannot be found. */
  @Nullable
  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug("Invoked getVersionedAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    VersionedAspect result = new VersionedAspect();

    Pair<RecordTemplate, Long> aspectRecord = getAspectVersionPair(urn, aspectName, version);
    if (aspectRecord.getFirst() == null) {
      return null;
    }

    Aspect resultAspect = new Aspect();

    RecordUtils.setSelectedRecordTemplateInUnion(resultAspect, aspectRecord.getFirst());
    result.setAspect(resultAspect);
    result.setVersion(aspectRecord.getSecond());

    return result;
  }

  /**
   * Retrieves a list of all aspects belonging to an entity of a particular type, sorted by urn.
   *
   * <p>Note that once we drop support for legacy 'getAllDataPlatforms' endpoint, we can drop
   * support for this unless otherwise required. Only visible for backwards compatibility.
   *
   * @param entityName name of the entity type the aspect belongs to, e.g. 'dataset'
   * @param aspectName name of the aspect requested, e.g. 'ownership'
   * @param start the starting index of the returned aspects, used in pagination
   * @param count the count of the aspects to be returned, used in pagination
   * @return a {@link ListResult} of {@link RecordTemplate}s representing the requested aspect.
   */
  @Nonnull
  @Override
  public ListResult<RecordTemplate> listLatestAspects(@Nonnull final String entityName,
      @Nonnull final String aspectName, final int start, final int count) {

    log.debug("Invoked listLatestAspects with entityName: {}, aspectName: {}, start: {}, count: {}", entityName,
        aspectName, start, count);

    final ListResult<String> aspectMetadataList =
        aspectDao.listLatestAspectMetadata(entityName, aspectName, start, count);

    List<EntityAspect> entityAspects = new ArrayList<>();
    for (int i = 0; i < aspectMetadataList.getValues().size(); i++) {
      EntityAspect entityAspect = new EntityAspect();
      entityAspect.setUrn(aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn().toString());
      entityAspect.setAspect(aspectName);
      entityAspect.setMetadata(aspectMetadataList.getValues().get(i));
      entityAspects.add(entityAspect);
    }

    return new ListResult<>(EntityUtils.toSystemAspects(entityAspects, this)
        .stream()
        .map(SystemAspect::getRecordTemplate)
        .collect(Collectors.toList()), aspectMetadataList.getMetadata(), aspectMetadataList.getNextStart(),
        aspectMetadataList.isHasNext(), aspectMetadataList.getTotalCount(), aspectMetadataList.getTotalPageCount(),
        aspectMetadataList.getPageSize());
  }

  /**
   * Common batch-like pattern used primarily in tests.
   *
   * @param entityUrn the entity urn
   * @param pairList list of aspects in pairs of aspect name and record template
   * @param auditStamp audit stamp
   * @param systemMetadata system metadata
   * @return update result
   */
  @Override
  public List<UpdateAspectResult> ingestAspects(@Nonnull Urn entityUrn, List<Pair<String, RecordTemplate>> pairList,
      @Nonnull final AuditStamp auditStamp, SystemMetadata systemMetadata) {
    List<? extends MCPItem> items = pairList.stream()
        .map(pair -> ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(pair.getKey())
            .recordTemplate(pair.getValue())
            .systemMetadata(systemMetadata)
            .auditStamp(auditStamp)
            .build(this))
        .collect(Collectors.toList());
    return ingestAspects(AspectsBatchImpl.builder().aspectRetriever(this).items(items).build(), true, true);
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataChangeLog}.
   *
   * @param aspectsBatch aspects to write
   * @param emitMCL whether a {@link com.linkedin.mxe.MetadataChangeLog} should be emitted in
   *     correspondence upon successful update
   * @return the {@link RecordTemplate} representation of the written aspect object
   */
  @Override
  public List<UpdateAspectResult> ingestAspects(@Nonnull final AspectsBatch aspectsBatch, boolean emitMCL,
      boolean overwrite) {
    Set<BatchItem> items = new HashSet<>(aspectsBatch.getItems());

    // Generate additional items as needed
    items.addAll(DefaultAspectsUtil.getAdditionalChanges(aspectsBatch, this, enableBrowseV2));
    AspectsBatch withDefaults = AspectsBatchImpl.builder().aspectRetriever(this).items(items).build();

    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestAspectsToLocalDB").time();

    /**
     * 插入元数据信息
     */
    List<UpdateAspectResult> ingestResults = ingestAspectsToLocalDB(withDefaults, overwrite);
    List<UpdateAspectResult> mclResults = emitMCL(ingestResults, emitMCL);
    ingestToLocalDBTimer.stop();

    return mclResults;
  }

  /**
   * Checks whether there is an actual update to the aspect by applying the updateLambda If there is
   * an update, push the new version into the local DB. Otherwise, do not push the new version, but
   * just update the system metadata.
   *
   * @param aspectsBatch Collection of the following: an urn associated with the new aspect, name of
   *     the aspect being inserted, and a function to apply to the latest version of the aspect to
   *     get the updated version
   * @return Details about the new and old version of the aspect
   */
  @Nonnull
  private List<UpdateAspectResult> ingestAspectsToLocalDB(@Nonnull final AspectsBatch aspectsBatch, boolean overwrite) {

    if (aspectsBatch.containsDuplicateAspects()) {
      log.warn(String.format("Batch contains duplicates: %s", aspectsBatch));
    }

    return aspectDao.runInTransactionWithRetry((tx) -> {
      // Read before write is unfortunate, however batch it
      final Map<String, Set<String>> urnAspects = aspectsBatch.getUrnAspectsMap();
      // read #1
      final Map<String, Map<String, SystemAspect>> latestAspects =
          EntityUtils.toSystemAspects(aspectDao.getLatestAspects(urnAspects), this);
      // read #2
      final Map<String, Map<String, Long>> nextVersions = aspectDao.getNextVersions(urnAspects);

      // 1. Convert patches to full upserts
      // 2. Run any entity/aspect level hooks
      Pair<Map<String, Set<String>>, List<ChangeMCP>> updatedItems = aspectsBatch.toUpsertBatchItems(latestAspects);

      // Fetch additional information if needed
      final Map<String, Map<String, SystemAspect>> updatedLatestAspects;
      final Map<String, Map<String, Long>> updatedNextVersions;
      if (!updatedItems.getFirst().isEmpty()) {
        Map<String, Map<String, SystemAspect>> newLatestAspects =
            EntityUtils.toSystemAspects(aspectDao.getLatestAspects(updatedItems.getFirst()), this);
        Map<String, Map<String, Long>> newNextVersions = aspectDao.getNextVersions(updatedItems.getFirst());
        // merge
        updatedLatestAspects = AspectsBatch.merge(latestAspects, newLatestAspects);
        updatedNextVersions = AspectsBatch.merge(nextVersions, newNextVersions);
      } else {
        updatedLatestAspects = latestAspects;
        updatedNextVersions = nextVersions;
      }

      // Add previous version to each upsert
      List<ChangeMCP> changeMCPs = updatedItems.getSecond().stream().peek(changeMCP -> {
        String urnStr = changeMCP.getUrn().toString();
        long nextVersion =
            updatedNextVersions.getOrDefault(urnStr, Map.of()).getOrDefault(changeMCP.getAspectName(), 0L);

        changeMCP.setPreviousSystemAspect(
            updatedLatestAspects.getOrDefault(urnStr, Map.of()).getOrDefault(changeMCP.getAspectName(), null));

        changeMCP.setNextAspectVersion(nextVersion);

        // support inner-batch upserts
        updatedLatestAspects.computeIfAbsent(urnStr, key -> new HashMap<>())
            .put(changeMCP.getAspectName(), changeMCP.getSystemAspect(nextVersion));
        updatedNextVersions.computeIfAbsent(urnStr, key -> new HashMap<>())
            .put(changeMCP.getAspectName(), nextVersion + 1);
      }).collect(Collectors.toList());

      // do final pre-commit checks with previous aspect value
      ValidationExceptionCollection exceptions = AspectsBatch.validatePreCommit(changeMCPs, this);
      if (!exceptions.isEmpty()) {
        throw new ValidationException(exceptions.toString());
      }

      // Database Upsert results
      List<UpdateAspectResult> upsertResults = changeMCPs.stream().map(item -> {
        final EntityAspect.EntitySystemAspect latest = (EntityAspect.EntitySystemAspect) item.getPreviousSystemAspect();

        final UpdateAspectResult result;
        if (overwrite || latest == null) {
          /**
           * insert ingestion metadata
           */
          result = ingestAspectToLocalDB(tx, item.getUrn(), item.getAspectName(), item.getRecordTemplate(),
              item.getAuditStamp(), item.getSystemMetadata(), latest == null ? null : latest,
              item.getNextAspectVersion()).toBuilder().request(item).build();
        } else {
          RecordTemplate oldValue = latest.getRecordTemplate();
          SystemMetadata oldMetadata = latest.getSystemMetadata();
          result = UpdateAspectResult.<ChangeItemImpl>builder()
              .urn(item.getUrn())
              .request(item)
              .oldValue(oldValue)
              .newValue(oldValue)
              .oldSystemMetadata(oldMetadata)
              .newSystemMetadata(oldMetadata)
              .operation(MetadataAuditOperation.UPDATE)
              .auditStamp(item.getAuditStamp())
              .maxVersion(latest.getVersion())
              .build();
        }

        return result;
      }).collect(Collectors.toList());

      // commit upserts prior to retention or kafka send, if supported by impl
      if (tx != null) {
        tx.commitAndContinue();
      }

      // Retention optimization and tx
      if (retentionService != null) {
        List<RetentionService.RetentionContext> retentionBatch = upsertResults.stream()
            // Only consider retention when there was a previous version
            .filter(result -> latestAspects.containsKey(result.getUrn().toString()) && latestAspects.get(
                result.getUrn().toString()).containsKey(result.getRequest().getAspectName()))
            .filter(result -> {
              RecordTemplate oldAspect = result.getOldValue();
              RecordTemplate newAspect = result.getNewValue();
              // Apply retention policies if there was an update to existing aspect
              // value
              return oldAspect != newAspect && oldAspect != null && retentionService != null;
            })
            .map(result -> RetentionService.RetentionContext.builder()
                .urn(result.getUrn())
                .aspectName(result.getRequest().getAspectName())
                .maxVersion(Optional.of(result.getMaxVersion()))
                .build())
            .collect(Collectors.toList());
        retentionService.applyRetentionWithPolicyDefaults(retentionBatch);
      } else {
        log.warn("Retention service is missing!");
      }

      return upsertResults;
    }, aspectsBatch, DEFAULT_MAX_TRANSACTION_RETRY).stream().flatMap(List::stream).collect(Collectors.toList());
  }

  @Nonnull
  private List<UpdateAspectResult> emitMCL(List<UpdateAspectResult> sqlResults, boolean emitMCL) {
    List<UpdateAspectResult> withEmitMCL = sqlResults.stream()
        .map(result -> emitMCL ? conditionallyProduceMCLAsync(result) : result)
        .collect(Collectors.toList());

    // join futures messages, capture error state
    List<Pair<Boolean, UpdateAspectResult>> statusPairs =
        withEmitMCL.stream().filter(result -> result.getMclFuture() != null).map(result -> {
          try {
            result.getMclFuture().get();
            return Pair.of(true, result);
          } catch (InterruptedException | ExecutionException e) {
            return Pair.of(false, result);
          }
        }).collect(Collectors.toList());

    if (statusPairs.stream().anyMatch(p -> !p.getFirst())) {
      log.error("Failed to produce MCLs: {}", statusPairs.stream()
          .filter(p -> !p.getFirst())
          .map(Pair::getValue)
          .map(v -> v.getRequest().toString())
          .collect(Collectors.toList()));
      // TODO restoreIndices?
      throw new RuntimeException("Failed to produce MCLs");
    }

    return withEmitMCL;
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataAuditEvent}.
   *
   * <p>This method runs a read -> write atomically in a single transaction, this is to prevent
   * multiple IDs from being created.
   *
   * <p>Note that in general, this should not be used externally. It is currently serving upgrade
   * scripts and is as such public.
   *
   * @param urn an urn associated with the new aspect
   * @param aspectName name of the aspect being inserted
   * @param newValue value of the aspect being inserted
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param systemMetadata
   * @return the {@link RecordTemplate} representation of the written aspect object
   */
  @Nullable
  @Override
  public RecordTemplate ingestAspectIfNotPresent(@Nonnull Urn urn, @Nonnull String aspectName,
      @Nonnull RecordTemplate newValue, @Nonnull AuditStamp auditStamp, @Nonnull SystemMetadata systemMetadata) {
    log.debug("Invoked ingestAspectIfNotPresent with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);

    AspectsBatchImpl aspectsBatch = AspectsBatchImpl.builder()
        .one(ChangeItemImpl.builder()
            .urn(urn)
            .aspectName(aspectName)
            .recordTemplate(newValue)
            .systemMetadata(systemMetadata)
            .auditStamp(auditStamp)
            .build(this), this)
        .build();
    List<UpdateAspectResult> ingested = ingestAspects(aspectsBatch, true, false);

    return ingested.stream().findFirst().get().getNewValue();
  }

  /**
   * Wrapper around batch method for single item
   *
   * @param proposal the proposal
   * @param auditStamp an audit stamp representing the time and actor proposing the change
   * @param async a flag to control whether we commit to primary store or just write to proposal log
   *     before returning
   * @return an {@link IngestResult} containing the results
   */
  @Override
  public IngestResult ingestProposal(MetadataChangeProposal proposal, AuditStamp auditStamp, final boolean async) {
    return ingestProposal(AspectsBatchImpl.builder().mcps(List.of(proposal), auditStamp, this).build(), async).stream()
        .findFirst()
        .get();
  }

  /**
   * Ingest a new {@link MetadataChangeProposal}. Note that this method does NOT include any
   * additional aspects or do any enrichment, instead it changes only those which are provided
   * inside the metadata change proposal.
   *
   * <p>Do not use this method directly for creating new entities, as it DOES NOT create an Entity
   * Key aspect in the DB. Instead, use an Entity Client.
   *
   * @param aspectsBatch the proposals to ingest
   * @param async a flag to control whether we commit to primary store or just write to proposal log
   *     before returning
   * @return an {@link IngestResult} containing the results
   */
  @Override
  public Set<IngestResult> ingestProposal(AspectsBatch aspectsBatch, final boolean async) {
    Stream<IngestResult> timeseriesIngestResults = ingestTimeseriesProposal(aspectsBatch, async);
    Stream<IngestResult> nonTimeseriesIngestResults =
        async ? ingestProposalAsync(aspectsBatch) : ingestProposalSync(aspectsBatch);

    return Stream.concat(timeseriesIngestResults, nonTimeseriesIngestResults).collect(Collectors.toSet());
  }

  /**
   * Timeseries is pass through to MCL, no MCP
   *
   * @param aspectsBatch timeseries upserts batch
   * @return returns ingest proposal result, however was never in the MCP topic
   */
  private Stream<IngestResult> ingestTimeseriesProposal(AspectsBatch aspectsBatch, final boolean async) {
    List<? extends BatchItem> unsupported = aspectsBatch.getItems()
        .stream()
        .filter(item -> item.getAspectSpec().isTimeseries() && item.getChangeType() != ChangeType.UPSERT)
        .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException("ChangeType not supported: " + unsupported.stream()
          .map(BatchItem::getChangeType)
          .collect(Collectors.toSet()));
    }

    if (!async) {
      // Create default non-timeseries aspects for timeseries aspects
      List<BatchItem> timeseriesItems = aspectsBatch.getItems()
          .stream()
          .filter(item -> item.getAspectSpec().isTimeseries())
          .collect(Collectors.toList());

      List<MCPItem> defaultAspects = DefaultAspectsUtil.getAdditionalChanges(
          AspectsBatchImpl.builder().aspectRetriever(this).items(timeseriesItems).build(), this, enableBrowseV2);
      ingestProposalSync(AspectsBatchImpl.builder().aspectRetriever(this).items(defaultAspects).build());
    }

    // Emit timeseries MCLs
    List<Pair<ChangeItemImpl, Optional<Pair<Future<?>, Boolean>>>> timeseriesResults = aspectsBatch.getItems()
        .stream()
        .filter(item -> item.getAspectSpec().isTimeseries())
        .map(item -> (ChangeItemImpl) item)
        .map(item -> Pair.of(item,
            conditionallyProduceMCLAsync(null, null, item.getRecordTemplate(), item.getSystemMetadata(),
                item.getMetadataChangeProposal(), item.getUrn(), item.getAuditStamp(), item.getAspectSpec())))
        .collect(Collectors.toList());

    return timeseriesResults.stream().map(result -> {
      Optional<Pair<Future<?>, Boolean>> emissionStatus = result.getSecond();

      emissionStatus.ifPresent(status -> {
        try {
          status.getFirst().get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });

      ChangeItemImpl request = result.getFirst();
      return IngestResult.builder()
          .urn(request.getUrn())
          .request(request)
          .publishedMCL(emissionStatus.map(status -> status.getFirst() != null).orElse(false))
          .processedMCL(emissionStatus.map(Pair::getSecond).orElse(false))
          .build();
    });
  }

  /**
   * For async ingestion of non-timeseries, any change type
   *
   * @param aspectsBatch non-timeseries ingest aspects
   * @return produced items to the MCP topic
   */
  private Stream<IngestResult> ingestProposalAsync(AspectsBatch aspectsBatch) {
    List<? extends MCPItem> nonTimeseries = aspectsBatch.getMCPItems()
        .stream()
        .filter(item -> !item.getAspectSpec().isTimeseries())
        .collect(Collectors.toList());

    List<Future<?>> futures = nonTimeseries.stream()
        .map(item ->
            // When async is turned on, we write to proposal log and return without waiting
            producer.produceMetadataChangeProposal(item.getUrn(), item.getMetadataChangeProposal()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    try {
      return nonTimeseries.stream()
          .map(item -> IngestResult.<MCPItem>builder().urn(item.getUrn()).request(item).publishedMCP(true).build());
    } finally {
      futures.forEach(f -> {
        try {
          f.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private Stream<IngestResult> ingestProposalSync(AspectsBatch aspectsBatch) {
    AspectsBatchImpl nonTimeseries = AspectsBatchImpl.builder()
        .aspectRetriever(this)
        .items(aspectsBatch.getItems()
            .stream()
            .filter(item -> !item.getAspectSpec().isTimeseries())
            .collect(Collectors.toList()))
        .build();

    List<? extends MCPItem> unsupported = nonTimeseries.getMCPItems()
        .stream()
        .filter(item -> item.getMetadataChangeProposal().getChangeType() != ChangeType.PATCH
            && item.getMetadataChangeProposal().getChangeType() != ChangeType.UPSERT)
        .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException("ChangeType not supported: " + unsupported.stream()
          .map(item -> item.getMetadataChangeProposal().getChangeType())
          .collect(Collectors.toSet()));
    }

    List<UpdateAspectResult> upsertResults = ingestAspects(nonTimeseries, true, true);

    return upsertResults.stream().map(result -> {
      ChangeMCP item = result.getRequest();

      return IngestResult.builder()
          .urn(item.getUrn())
          .request(item)
          .publishedMCL(result.getMclFuture() != null)
          .sqlCommitted(true)
          .isUpdate(result.getOldValue() != null)
          .build();
    });
  }

  @Override
  public String batchApplyRetention(Integer start, Integer count, Integer attemptWithVersion, String aspectName,
      String urn) {
    BulkApplyRetentionArgs args = new BulkApplyRetentionArgs();
    if (start == null) {
      start = 0;
    }
    args.start = start;
    if (count == null) {
      count = 100;
    }
    args.count = count;
    if (attemptWithVersion == null) {
      attemptWithVersion = 21;
    }
    args.attemptWithVersion = attemptWithVersion;
    args.aspectName = aspectName;
    args.urn = urn;
    BulkApplyRetentionResult result = retentionService.batchApplyRetentionEntities(args);
    return result.toString();
  }

  private boolean preprocessEvent(MetadataChangeLog metadataChangeLog) {
    if (preProcessHooks.isUiEnabled()) {
      if (metadataChangeLog.getSystemMetadata() != null) {
        if (metadataChangeLog.getSystemMetadata().getProperties() != null) {
          if (UI_SOURCE.equals(metadataChangeLog.getSystemMetadata().getProperties().get(APP_SOURCE))) {
            // Pre-process the update indices hook for UI updates to avoid perceived lag from Kafka
            if (updateIndicesService != null) {
              updateIndicesService.handleChangeEvent(metadataChangeLog);
            }
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public Integer getCountAspect(@Nonnull String aspectName, @Nullable String urnLike) {
    return aspectDao.countAspect(aspectName, urnLike);
  }

  @Nonnull
  @Override
  public RestoreIndicesResult restoreIndices(@Nonnull RestoreIndicesArgs args, @Nonnull Consumer<String> logger) {
    RestoreIndicesResult result = new RestoreIndicesResult();
    int ignored = 0;
    int rowsMigrated = 0;
    logger.accept(String.format("Args are %s", args));
    logger.accept(String.format("Reading rows %s through %s from the aspects table started.", args.start,
        args.start + args.batchSize));
    long startTime = System.currentTimeMillis();
    PagedList<EbeanAspectV2> rows = aspectDao.getPagedAspects(args);
    result.timeSqlQueryMs = System.currentTimeMillis() - startTime;
    startTime = System.currentTimeMillis();
    logger.accept(String.format("Reading rows %s through %s from the aspects table completed.", args.start,
        args.start + args.batchSize));

    LinkedList<Future<?>> futures = new LinkedList<>();

    List<SystemAspect> systemAspects =
        EntityUtils.toSystemAspectFromEbeanAspects(rows != null ? rows.getList() : List.<EbeanAspectV2>of(), this);

    for (SystemAspect aspect : systemAspects) {
      // 1. Extract an Entity type from the entity Urn
      result.timeGetRowMs = System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      Urn urn;
      try {
        urn = aspect.getUrn();
        result.lastUrn = urn.toString();
      } catch (Exception e) {
        logger.accept(
            String.format("Failed to bind Urn with value %s into Urn object: %s. Ignoring row.", aspect.getUrn(), e));
        ignored = ignored + 1;
        continue;
      }
      result.timeUrnMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 2. Verify that the entity associated with the aspect is found in the registry.
      final String entityName = urn.getEntityType();
      final EntitySpec entitySpec;
      try {
        entitySpec = entityRegistry.getEntitySpec(entityName);
      } catch (Exception e) {
        logger.accept(
            String.format("Failed to find entity with name %s in Entity Registry: %s. Ignoring row.", entityName, e));
        ignored = ignored + 1;
        continue;
      }
      result.timeEntityRegistryCheckMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      final String aspectName = aspect.getAspectName();
      result.lastAspect = aspectName;

      // 3. Verify that the aspect is a valid aspect associated with the entity
      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        logger.accept(String.format("Failed to find aspect with name %s associated with entity named %s", aspectName,
            entityName));
        ignored = ignored + 1;
        continue;
      }
      result.aspectCheckMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 4. Create record from json aspect
      final RecordTemplate aspectRecord;
      try {
        aspectRecord = aspect.getRecordTemplate();
      } catch (Exception e) {
        logger.accept(
            String.format("Failed to deserialize for entity %s, aspect %s: %s. Ignoring row.", entityName, aspectName,
                e));
        ignored = ignored + 1;
        continue;
      }
      result.createRecordMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // Force indexing to skip diff mode and fix error states
      SystemMetadata latestSystemMetadata = aspect.getSystemMetadata();

      StringMap properties =
          latestSystemMetadata.getProperties() != null ? latestSystemMetadata.getProperties() : new StringMap();
      properties.put(FORCE_INDEXING_KEY, Boolean.TRUE.toString());
      latestSystemMetadata.setProperties(properties);

      // 5. Produce MAE events for the aspect record
      futures.add(
          alwaysProduceMCLAsync(urn, entityName, aspectName, aspectSpec, null, aspectRecord, null, latestSystemMetadata,
              new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()),
              ChangeType.RESTATE).getFirst());
      result.sendMessageMs += System.currentTimeMillis() - startTime;

      rowsMigrated++;
    }
    futures.stream().filter(Objects::nonNull).forEach(f -> {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    try {
      TimeUnit.MILLISECONDS.sleep(args.batchDelayMs);
    } catch (InterruptedException e) {
      throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
    }
    result.ignored = ignored;
    result.rowsMigrated = rowsMigrated;
    return result;
  }

  /**
   * Lists the entity URNs found in storage.
   *
   * @param entityName the name associated with the entity
   * @param start the start offset
   * @param count the count
   */
  @Override
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count) {
    log.debug("Invoked listUrns with entityName: {}, start: {}, count: {}", entityName, start, count);

    // If a keyAspect exists, the entity exists.
    final String keyAspectName = getEntityRegistry().getEntitySpec(entityName).getKeyAspectSpec().getName();
    final ListResult<String> keyAspectList = aspectDao.listUrns(entityName, keyAspectName, start, count);

    final ListUrnsResult result = new ListUrnsResult();
    result.setStart(start);
    result.setCount(keyAspectList.getValues().size());
    result.setTotal(keyAspectList.getTotalCount());

    // Extract urns
    final UrnArray entityUrns = new UrnArray();
    for (String urn : keyAspectList.getValues()) {
      try {
        entityUrns.add(Urn.createFromString(urn));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(String.format("Failed to convert urn %s found in db to Urn object.", urn),
            e);
      }
    }
    result.setEntities(entityUrns);
    return result;
  }

  /**
   * Default implementations. Subclasses should feel free to override if it's more efficient to do
   * so.
   */
  @Override
  public Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return getEntities(Collections.singleton(urn), aspectNames).values().stream().findFirst().orElse(null);
  }

  /**
   * Deprecated! Use getEntitiesV2 instead.
   *
   * <p>Retrieves multiple entities.
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Deprecated
  @Override
  public Map<Urn, Entity> getEntities(@Nonnull final Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    log.debug("Invoked getEntities with urns {}, aspects {}", urns, aspectNames);
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }
    return getSnapshotUnions(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> EntityUtils.toEntity(entry.getValue())));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(@Nonnull final Urn urn, @Nonnull final AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    Future<?> future = producer.produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
    return Pair.of(future, preprocessEvent(metadataChangeLog));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(@Nonnull final Urn urn, @Nonnull String entityName,
      @Nonnull String aspectName, @Nonnull final AspectSpec aspectSpec, @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue, @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata, @Nonnull AuditStamp auditStamp,
      @Nonnull final ChangeType changeType) {
    final MetadataChangeLog metadataChangeLog =
        constructMCL(null, entityName, urn, changeType, aspectName, auditStamp, newAspectValue, newSystemMetadata,
            oldAspectValue, oldSystemMetadata);
    return alwaysProduceMCLAsync(urn, aspectSpec, metadataChangeLog);
  }

  public Optional<Pair<Future<?>, Boolean>> conditionallyProduceMCLAsync(@Nullable RecordTemplate oldAspect,
      @Nullable SystemMetadata oldSystemMetadata, RecordTemplate newAspect, SystemMetadata newSystemMetadata,
      @Nullable MetadataChangeProposal mcp, Urn entityUrn, AuditStamp auditStamp, AspectSpec aspectSpec) {
    boolean isNoOp = oldAspect == newAspect;
    if (!isNoOp || alwaysEmitChangeLog || shouldAspectEmitChangeLog(aspectSpec)) {
      log.debug("Producing MetadataChangeLog for ingested aspect {}, urn {}", aspectSpec.getName(), entityUrn);

      final MetadataChangeLog metadataChangeLog =
          constructMCL(mcp, urnToEntityName(entityUrn), entityUrn, isNoOp ? ChangeType.RESTATE : ChangeType.UPSERT,
              aspectSpec.getName(), auditStamp, newAspect, newSystemMetadata, oldAspect, oldSystemMetadata);

      log.debug("Serialized MCL event: {}", metadataChangeLog);
      Pair<Future<?>, Boolean> emissionStatus = alwaysProduceMCLAsync(entityUrn, aspectSpec, metadataChangeLog);
      return emissionStatus.getFirst() != null ? Optional.of(emissionStatus) : Optional.empty();
    } else {
      log.debug("Skipped producing MetadataChangeLog for ingested aspect {}, urn {}. Aspect has not changed.",
          aspectSpec.getName(), entityUrn);
      return Optional.empty();
    }
  }

  private UpdateAspectResult conditionallyProduceMCLAsync(UpdateAspectResult result) {
    ChangeMCP request = result.getRequest();
    Optional<Pair<Future<?>, Boolean>> emissionStatus =
        conditionallyProduceMCLAsync(result.getOldValue(), result.getOldSystemMetadata(), result.getNewValue(),
            result.getNewSystemMetadata(), request.getMetadataChangeProposal(), result.getUrn(), result.getAuditStamp(),
            request.getAspectSpec());

    return emissionStatus.map(
            status -> result.toBuilder().mclFuture(status.getFirst()).processedMCL(status.getSecond()).build())
        .orElse(result);
  }

  @Override
  public void ingestEntities(@Nonnull final List<Entity> entities, @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata) {
    log.debug("Invoked ingestEntities with entities {}, audit stamp {}", entities, auditStamp);
    Streams.zip(entities.stream(), systemMetadata.stream(), (a, b) -> new Pair<Entity, SystemMetadata>(a, b))
        .forEach(pair -> ingestEntity(pair.getFirst(), auditStamp, pair.getSecond()));
  }

  @Override
  public SystemMetadata ingestEntity(Entity entity, AuditStamp auditStamp) {
    SystemMetadata generatedSystemMetadata = new SystemMetadata();
    generatedSystemMetadata.setRunId(DEFAULT_RUN_ID);
    generatedSystemMetadata.setLastObserved(System.currentTimeMillis());

    ingestEntity(entity, auditStamp, generatedSystemMetadata);
    return generatedSystemMetadata;
  }

  @Override
  public void ingestEntity(@Nonnull Entity entity, @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    log.debug("Invoked ingestEntity with entity {}, audit stamp {} systemMetadata {}", entity, auditStamp,
        systemMetadata.toString());
    /**
     * 摄取元数据
     */
    ingestSnapshotUnion(entity.getValue(), auditStamp, systemMetadata);
  }

  @Nonnull
  protected Map<Urn, Snapshot> getSnapshotUnions(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return getSnapshotRecords(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> EntityUtils.toSnapshotUnion(entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, RecordTemplate> getSnapshotRecords(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return getLatestAspectUnions(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotRecord(entry.getKey(), entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, List<UnionTemplate>> getLatestAspectUnions(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return this.getLatestAspects(urns, aspectNames)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
            .stream()
            .map(aspectRecord -> toAspectUnion(entry.getKey(), aspectRecord))
            .collect(Collectors.toList())));
  }

  private void ingestSnapshotUnion(@Nonnull final Snapshot snapshotUnion, @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = com.datahub.util.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<Pair<String, RecordTemplate>> aspectRecordsToIngest =
        NewModelUtils.getAspectsFromSnapshot(snapshotRecord);

    log.info("INGEST urn {} with system metadata {}", urn.toString(), systemMetadata.toString());
    aspectRecordsToIngest.addAll(DefaultAspectsUtil.generateDefaultAspects(this, urn,
        aspectRecordsToIngest.stream().map(Pair::getFirst).collect(Collectors.toSet()), enableBrowseV2));

    /**
     * 构建 AspectsBatchImpl 对象
     */
    AspectsBatchImpl aspectsBatch = AspectsBatchImpl.builder()
        .aspectRetriever(this)
        .items(aspectRecordsToIngest.stream()
            .map(pair -> ChangeItemImpl.builder()
                .urn(urn)
                .aspectName(pair.getKey())
                .recordTemplate(pair.getValue())
                .auditStamp(auditStamp)
                .systemMetadata(systemMetadata)
                .build(this))
            .collect(Collectors.toList()))
        .build();

    /**
     * ingestion AspectsBatchImpl
     */
    ingestAspects(aspectsBatch, true, true);
  }

  @Override
  public AspectSpec getKeyAspectSpec(@Nonnull final Urn urn) {
    return getKeyAspectSpec(urnToEntityName(urn));
  }

  @Override
  public AspectSpec getKeyAspectSpec(@Nonnull final String entityName) {
    final EntitySpec spec = entityRegistry.getEntitySpec(entityName);
    return spec.getKeyAspectSpec();
  }

  @Override
  public Optional<AspectSpec> getAspectSpec(@Nonnull final String entityName, @Nonnull final String aspectName) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    return Optional.ofNullable(entitySpec.getAspectSpec(aspectName));
  }

  @Override
  public String getKeyAspectName(@Nonnull final Urn urn) {
    final EntitySpec spec = entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getName();
  }

  protected RecordTemplate toSnapshotRecord(@Nonnull final Urn urn,
      @Nonnull final List<UnionTemplate> aspectUnionTemplates) {
    final String entityName = urnToEntityName(urn);
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    return com.datahub.util.ModelUtils.newSnapshot(
        getDataTemplateClassFromSchema(entitySpec.getSnapshotSchema(), RecordTemplate.class), urn,
        aspectUnionTemplates);
  }

  protected UnionTemplate toAspectUnion(@Nonnull final Urn urn, @Nonnull final RecordTemplate aspectRecord) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(urnToEntityName(urn));
    final TyperefDataSchema aspectSchema = entitySpec.getAspectTyperefSchema();
    if (aspectSchema == null) {
      throw new RuntimeException(
          String.format("Aspect schema for %s is null: v4 operation is not supported on this entity registry",
              entitySpec.getName()));
    }
    return com.datahub.util.ModelUtils.newAspectUnion(
        getDataTemplateClassFromSchema(entitySpec.getAspectTyperefSchema(), UnionTemplate.class), aspectRecord);
  }

  @Override
  @Nonnull
  public EntityRegistry getEntityRegistry() {
    return entityRegistry;
  }

  @Override
  public void setRetentionService(RetentionService<ChangeItemImpl> retentionService) {
    this.retentionService = retentionService;
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  @Override
  public Set<String> getEntityAspectNames(final String entityName) {
    return entityToValidAspects.get(entityName);
  }

  @Override
  public void setWritable(boolean canWrite) {
    log.debug("Setting writable to {}", canWrite);
    aspectDao.setWritable(canWrite);
  }

  @Override
  public RollbackRunResult rollbackRun(List<AspectRowSummary> aspectRows, String runId, boolean hardDelete) {
    return rollbackWithConditions(aspectRows, Collections.singletonMap("runId", runId), hardDelete);
  }

  @Override
  public RollbackRunResult rollbackWithConditions(List<AspectRowSummary> aspectRows, Map<String, String> conditions,
      boolean hardDelete) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    AtomicInteger rowsDeletedFromEntityDeletion = new AtomicInteger(0);

    List<Future<?>> futures = aspectRows.stream().map(aspectToRemove -> {
      RollbackResult result =
          deleteAspect(aspectToRemove.getUrn(), aspectToRemove.getAspectName(), conditions, hardDelete);
      if (result != null) {
        Optional<AspectSpec> aspectSpec = getAspectSpec(result.entityName, result.aspectName);
        if (!aspectSpec.isPresent()) {
          log.error("Issue while rolling back: unknown aspect {} for entity {}", result.entityName, result.aspectName);
          return null;
        }

        rowsDeletedFromEntityDeletion.addAndGet(result.additionalRowsAffected);
        removedAspects.add(aspectToRemove);
        return alwaysProduceMCLAsync(result.getUrn(), result.getEntityName(), result.getAspectName(), aspectSpec.get(),
            result.getOldValue(), result.getNewValue(), result.getOldSystemMetadata(), result.getNewSystemMetadata(),
            // TODO: use properly attributed audit stamp.
            createSystemAuditStamp(), result.getChangeType()).getFirst();
      }

      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());

    futures.forEach(f -> {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion.get());
  }

  @Override
  public RollbackRunResult deleteUrn(Urn urn) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    Integer rowsDeletedFromEntityDeletion = 0;

    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    String keyAspectName = getKeyAspectName(urn);

    EntityAspect latestKey = null;
    try {
      latestKey = aspectDao.getLatestAspect(urn.toString(), keyAspectName);
    } catch (EntityNotFoundException e) {
      log.warn("Entity to delete does not exist. {}", urn.toString());
    }
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
    }

    SystemMetadata latestKeySystemMetadata =
        EntityUtils.toSystemAspect(latestKey, this).map(SystemAspect::getSystemMetadata).get();
    RollbackResult result = deleteAspect(urn.toString(), keyAspectName,
        Collections.singletonMap("runId", latestKeySystemMetadata.getRunId()), true);

    if (result != null) {
      AspectRowSummary summary = new AspectRowSummary();
      summary.setUrn(urn.toString());
      summary.setKeyAspect(true);
      summary.setAspectName(keyAspectName);
      summary.setVersion(0);
      summary.setTimestamp(latestKey.getCreatedOn().getTime());

      rowsDeletedFromEntityDeletion = result.additionalRowsAffected;
      removedAspects.add(summary);
      Future<?> future = alwaysProduceMCLAsync(result.getUrn(), result.getEntityName(), result.getAspectName(), keySpec,
          result.getOldValue(), result.getNewValue(), result.getOldSystemMetadata(), result.getNewSystemMetadata(),
          // TODO: Use a proper inferred audit stamp
          createSystemAuditStamp(), result.getChangeType()).getFirst();

      if (future != null) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
  }

  @Override
  public Set<Urn> exists(@Nonnull final Collection<Urn> urns, @Nullable String aspectName, boolean includeSoftDeleted) {
    final Set<EntityAspectIdentifier> dbKeys = urns.stream()
        .map(urn -> new EntityAspectIdentifier(urn.toString(),
            aspectName == null ? entityRegistry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec().getName()
                : aspectName, ASPECT_LATEST_VERSION))
        .collect(Collectors.toSet());
    final Map<EntityAspectIdentifier, EntityAspect> aspects = aspectDao.batchGet(dbKeys);
    final Set<String> existingUrnStrings = aspects.values()
        .stream()
        .filter(aspect -> aspect != null)
        .map(aspect -> aspect.getUrn())
        .collect(Collectors.toSet());

    Set<Urn> existing =
        urns.stream().filter(urn -> existingUrnStrings.contains(urn.toString())).collect(Collectors.toSet());

    if (includeSoftDeleted) {
      return existing;
    } else {
      // Additionally exclude status.removed == true
      Map<Urn, List<RecordTemplate>> statusResult = getLatestAspects(existing, Set.of(STATUS_ASPECT_NAME));
      return existing.stream().filter(urn ->
          // key aspect is always returned, make sure to only consider the status aspect
          statusResult.getOrDefault(urn, List.of())
              .stream()
              .filter(aspect -> STATUS_ASPECT_NAME.equals(aspect.schema().getName()))
              .noneMatch(aspect -> ((Status) aspect).isRemoved())).collect(Collectors.toSet());
    }
  }

  @Nullable
  @Override
  public RollbackResult deleteAspect(String urn, String aspectName, @Nonnull Map<String, String> conditions,
      boolean hardDelete) {
    final AuditStamp auditStamp =
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    // Validate pre-conditions before running queries
    Urn entityUrn = UrnUtils.getUrn(urn);

    // Runs simple validations
    MCPItem deleteItem =
        DeleteItemImpl.builder().urn(entityUrn).aspectName(aspectName).auditStamp(auditStamp).build(this);

    // Delete validation hooks
    ValidationExceptionCollection exceptions = AspectsBatch.validateProposed(List.of(deleteItem), this);
    if (!exceptions.isEmpty()) {
      throw new ValidationException(exceptions.toString());
    }

    final RollbackResult result = aspectDao.runInTransactionWithRetry((tx) -> {
      Integer additionalRowsDeleted = 0;

      // 1. Fetch the latest existing version of the aspect.
      final EntityAspect.EntitySystemAspect latest =
          (EntityAspect.EntitySystemAspect) EntityUtils.toSystemAspect(aspectDao.getLatestAspect(urn, aspectName), this)
              .orElse(null);

      // 1.1 If no latest exists, skip this aspect
      if (latest == null) {
        return null;
      }

      // 2. Compare the match conditions, if they don't match, ignore.
      SystemMetadata latestSystemMetadata = latest.getSystemMetadata();
      if (!filterMatch(latestSystemMetadata, conditions)) {
        return null;
      }

      // 3. Check if this is a key aspect
      Boolean isKeyAspect = getKeyAspectName(entityUrn).equals(aspectName);

      // 4. Fetch all preceding aspects, that match
      List<EntityAspect> aspectsToDelete = new ArrayList<>();
      long maxVersion = aspectDao.getMaxVersion(urn, aspectName);
      EntityAspect.EntitySystemAspect survivingAspect = null;
      String previousMetadata = null;
      boolean filterMatch = true;
      while (maxVersion > 0 && filterMatch) {
        EntityAspect.EntitySystemAspect candidateAspect = (EntityAspect.EntitySystemAspect) EntityUtils.toSystemAspect(
            aspectDao.getAspect(urn, aspectName, maxVersion), this).orElse(null);
        SystemMetadata previousSysMetadata = candidateAspect != null ? candidateAspect.getSystemMetadata() : null;
        filterMatch = previousSysMetadata != null && filterMatch(previousSysMetadata, conditions);
        if (filterMatch) {
          aspectsToDelete.add(candidateAspect.getEntityAspect());
          maxVersion = maxVersion - 1;
        } else {
          survivingAspect = candidateAspect;
          previousMetadata = survivingAspect.getMetadataRaw();
        }
      }

      // Delete validation hooks
      ValidationExceptionCollection preCommitExceptions = AspectsBatch.validatePreCommit(aspectsToDelete.stream()
          .map(toDelete -> DeleteItemImpl.builder()
              .urn(UrnUtils.getUrn(toDelete.getUrn()))
              .aspectName(toDelete.getAspect())
              .auditStamp(auditStamp)
              .build(this))
          .collect(Collectors.toList()), this);
      if (!preCommitExceptions.isEmpty()) {
        throw new ValidationException(preCommitExceptions.toString());
      }

      // 5. Apply deletes and fix up latest row
      aspectsToDelete.forEach(aspect -> aspectDao.deleteAspect(tx, aspect));

      if (survivingAspect != null) {
        // if there was a surviving aspect, copy its information into the latest row
        // eBean does not like us updating a pkey column (version) for the surviving aspect
        // as a result we copy information from survivingAspect to latest and delete
        // survivingAspect
        latest.getEntityAspect().setMetadata(survivingAspect.getEntityAspect().getMetadata());
        latest.getEntityAspect().setSystemMetadata(survivingAspect.getEntityAspect().getSystemMetadata());
        latest.getEntityAspect().setCreatedOn(survivingAspect.getCreatedOn());
        latest.getEntityAspect().setCreatedBy(survivingAspect.getCreatedBy());
        latest.getEntityAspect().setCreatedFor(survivingAspect.getEntityAspect().getCreatedFor());
        aspectDao.saveAspect(tx, latest.getEntityAspect(), false);
        // metrics
        aspectDao.incrementWriteMetrics(aspectName, 1, latest.getMetadataRaw().getBytes(StandardCharsets.UTF_8).length);
        aspectDao.deleteAspect(tx, survivingAspect.getEntityAspect());
      } else {
        if (isKeyAspect) {
          if (hardDelete) {
            // If this is the key aspect, delete the entity entirely.
            additionalRowsDeleted = aspectDao.deleteUrn(tx, urn);
          } else if (deleteItem.getEntitySpec().hasAspect(Constants.STATUS_ASPECT_NAME)) {
            // soft delete by setting status.removed=true (if applicable)
            final Status statusAspect = new Status();
            statusAspect.setRemoved(true);

            final MetadataChangeProposal gmce = new MetadataChangeProposal();
            gmce.setEntityUrn(entityUrn);
            gmce.setChangeType(ChangeType.UPSERT);
            gmce.setEntityType(entityUrn.getEntityType());
            gmce.setAspectName(Constants.STATUS_ASPECT_NAME);
            gmce.setAspect(GenericRecordUtils.serializeAspect(statusAspect));

            this.ingestProposal(gmce, auditStamp, false);
          }
        } else {
          // Else, only delete the specific aspect.
          aspectDao.deleteAspect(tx, latest.getEntityAspect());
        }
      }

      // 6. Emit the Update
      try {
        final RecordTemplate latestValue = latest == null ? null : latest.getRecordTemplate();
        final RecordTemplate previousValue = survivingAspect == null ? null : latest.getRecordTemplate();

        final Urn urnObj = Urn.createFromString(urn);
        // We are not deleting key aspect if hardDelete has not been set so do not return a
        // rollback result
        if (isKeyAspect && !hardDelete) {
          return null;
        }
        return new RollbackResult(urnObj, urnObj.getEntityType(), latest.getAspectName(), latestValue, previousValue,
            latestSystemMetadata, previousValue == null ? null : survivingAspect.getSystemMetadata(),
            survivingAspect == null ? ChangeType.DELETE : ChangeType.UPSERT, isKeyAspect, additionalRowsDeleted);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to emit the update for urn %s", urn));
      } catch (IllegalStateException e) {
        log.warn("Unable to find aspect, rollback result will not be sent. Error: {}", e.getMessage());
        return null;
      }
    }, DEFAULT_MAX_TRANSACTION_RETRY);

    return result;
  }

  protected boolean filterMatch(@Nonnull SystemMetadata systemMetadata, Map<String, String> conditions) {
    String runIdCondition = conditions.getOrDefault("runId", null);
    if (runIdCondition != null) {
      if (!runIdCondition.equals(systemMetadata.getRunId())) {
        return false;
      }
    }
    String registryNameCondition = conditions.getOrDefault("registryName", null);
    if (registryNameCondition != null) {
      if (!registryNameCondition.equals(systemMetadata.getRegistryName())) {
        return false;
      }
    }
    String registryVersionCondition = conditions.getOrDefault("registryVersion", null);
    if (registryVersionCondition != null) {
      if (!registryVersionCondition.equals(systemMetadata.getRegistryVersion())) {
        return false;
      }
    }
    return true;
  }

  protected AuditStamp createSystemAuditStamp() {
    return new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
  }

  @Nonnull
  private Map<EntityAspectIdentifier, EntityAspect> getLatestAspect(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {

    log.debug("Invoked getLatestAspects with urns: {}, aspectNames: {}", urns, aspectNames);

    // Create DB keys
    final Set<EntityAspectIdentifier> dbKeys = urns.stream().map(urn -> {
      final Set<String> aspectsToFetch = aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
      return aspectsToFetch.stream()
          .map(aspectName -> new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
          .collect(Collectors.toList());
    }).flatMap(List::stream).collect(Collectors.toSet());

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = new HashMap<>();
    Iterators.partition(dbKeys.iterator(), MAX_KEYS_PER_QUERY)
        .forEachRemaining(batch -> batchGetResults.putAll(aspectDao.batchGet(ImmutableSet.copyOf(batch))));
    return batchGetResults;
  }

  /*
   * When a user tries to fetch a negative version, we want to index most recent to least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {
    if (version < 0) {
      return aspectDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  private Map<EntityAspectIdentifier, EnvelopedAspect> getEnvelopedAspects(final Set<EntityAspectIdentifier> dbKeys) {
    final Map<EntityAspectIdentifier, EntityAspect> dbEntries = aspectDao.batchGet(dbKeys);

    List<SystemAspect> envelopedAspects = EntityUtils.toSystemAspects(dbEntries.values(), this);

    return envelopedAspects.stream()
        .collect(
            Collectors.toMap(systemAspect -> ((EntityAspect.EntitySystemAspect) systemAspect).getAspectIdentifier(),
                systemAspect -> ((EntityAspect.EntitySystemAspect) systemAspect).toEnvelopedAspects()));
  }

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDB(@Nullable Transaction tx, @Nonnull final Urn urn,
      @Nonnull final String aspectName, @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata, @Nullable final EntityAspect.EntitySystemAspect latest,
      @Nonnull final Long nextVersion) {

    // Set the "last run id" to be the run id provided with the new system metadata. This will be
    // stored in index
    // for all aspects that have a run id, regardless of whether they change.
    providedSystemMetadata.setLastRunId(providedSystemMetadata.getRunId(GetMode.NULL), SetMode.IGNORE_NULL);

    // 2. Compare the latest existing and new.
    final RecordTemplate oldValue = latest == null ? null : latest.getRecordTemplate();

    // 3. If there is no difference between existing and new, we just update
    // the lastObserved in system metadata. RunId should stay as the original runId
    if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
      SystemMetadata latestSystemMetadata = latest.getSystemMetadata();
      latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());
      latestSystemMetadata.setLastRunId(providedSystemMetadata.getLastRunId(GetMode.NULL), SetMode.IGNORE_NULL);

      latest.getEntityAspect().setSystemMetadata(RecordUtils.toJsonString(latestSystemMetadata));

      log.info("Ingesting aspect with name {}, urn {}", aspectName, urn);
      /**
       * save ingestion metadata
       * 本质插入 mysql metadata_aspect_v2 的审计日志 也即 python -m datahub ingestion --config xxx.yml 的日志信息
       */
      aspectDao.saveAspect(tx, latest.getEntityAspect(), false);

      // metrics
      aspectDao.incrementWriteMetrics(aspectName, 1, latest.getMetadataRaw().getBytes(StandardCharsets.UTF_8).length);

      return UpdateAspectResult.builder()
          .urn(urn)
          .oldValue(oldValue)
          .newValue(oldValue)
          .oldSystemMetadata(latest.getSystemMetadata())
          .newSystemMetadata(latestSystemMetadata)
          .operation(MetadataAuditOperation.UPDATE)
          .auditStamp(auditStamp)
          .maxVersion(0)
          .build();
    }

    // 4. Save the newValue as the latest version
    log.debug("Ingesting aspect with name {}, urn {}", aspectName, urn);
    String newValueStr = EntityUtils.toJsonAspect(newValue);
    long versionOfOld = aspectDao.saveLatestAspect(tx, urn.toString(), aspectName,
        latest == null ? null : EntityUtils.toJsonAspect(oldValue), latest == null ? null : latest.getCreatedBy(),
        latest == null ? null : latest.getEntityAspect().getCreatedFor(), latest == null ? null : latest.getCreatedOn(),
        latest == null ? null : latest.getSystemMetadataRaw(), newValueStr, auditStamp.getActor().toString(),
        auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
        new Timestamp(auditStamp.getTime()), EntityUtils.toJsonAspect(providedSystemMetadata), nextVersion);

    // metrics
    aspectDao.incrementWriteMetrics(aspectName, 1, newValueStr.getBytes(StandardCharsets.UTF_8).length);

    return UpdateAspectResult.builder()
        .urn(urn)
        .oldValue(oldValue)
        .newValue(newValue)
        .oldSystemMetadata(latest == null ? null : latest.getSystemMetadata())
        .newSystemMetadata(providedSystemMetadata)
        .operation(MetadataAuditOperation.UPDATE)
        .auditStamp(auditStamp)
        .maxVersion(versionOfOld)
        .build();
  }

  private static boolean shouldAspectEmitChangeLog(@Nonnull final AspectSpec aspectSpec) {
    final List<RelationshipFieldSpec> relationshipFieldSpecs = aspectSpec.getRelationshipFieldSpecs();
    return relationshipFieldSpecs.stream().anyMatch(RelationshipFieldSpec::isLineageRelationship);
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, com.linkedin.entity.Aspect>> getLatestAspectObjects(Set<Urn> urns,
      Set<String> aspectNames) throws RemoteInvocationException, URISyntaxException {
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    } else {
      String entityName = urns.stream().findFirst().map(Urn::getEntityType).get();
      return entityResponseToAspectMap(getEntitiesV2(entityName, urns, aspectNames));
    }
  }
}
