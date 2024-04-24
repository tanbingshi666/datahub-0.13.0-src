package com.linkedin.entity.client;

import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.common.client.ClientCache;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Builder
public class EntityClientCache {
  @NonNull private EntityClientCacheConfig config;
  @NonNull private final ClientCache<Key, EnvelopedAspect, EntityClientCacheConfig> cache;
  @NonNull private Function<CollectionKey, Map<Urn, EntityResponse>> loadFunction;

  public EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames) {
    return batchGetV2(opContext, Set.of(urn), aspectNames).get(urn);
  }

  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    final Map<Urn, EntityResponse> response;

    if (config.isEnabled()) {
      Set<Key> keys =
          urns.stream()
              .flatMap(
                  urn ->
                      aspectNames.stream()
                          .map(
                              a ->
                                  Key.builder()
                                      .contextId(opContext.getEntityContextId())
                                      .urn(urn)
                                      .aspectName(a)
                                      .build()))
              .collect(Collectors.toSet());
      Map<Key, EnvelopedAspect> envelopedAspects = cache.getAll(keys);

      Set<EntityResponse> responses =
          envelopedAspects.entrySet().stream()
              .map(entry -> Pair.of(entry.getKey().getUrn(), entry.getValue()))
              .collect(
                  Collectors.groupingBy(
                      Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())))
              .entrySet()
              .stream()
              .map(e -> toEntityResponse(e.getKey(), e.getValue()))
              .collect(Collectors.toSet());

      response =
          responses.stream().collect(Collectors.toMap(EntityResponse::getUrn, Function.identity()));
    } else {
      response =
          loadFunction.apply(
              CollectionKey.builder()
                  .contextId(opContext.getEntityContextId())
                  .urns(urns)
                  .aspectNames(aspectNames)
                  .build());
    }

    return response;
  }

  private static EntityResponse toEntityResponse(
      Urn urn, Collection<EnvelopedAspect> envelopedAspects) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urnToEntityName(urn));
    response.setAspects(
        new EnvelopedAspectMap(
            envelopedAspects.stream()
                .collect(Collectors.toMap(EnvelopedAspect::getName, aspect -> aspect))));
    return response;
  }

  public static class EntityClientCacheBuilder {

    private EntityClientCacheBuilder cache(LoadingCache<Key, EnvelopedAspect> cache) {
      return this;
    }

    public EntityClientCache build(Class<?> metricClazz) {
      // estimate size
      Weigher<Key, EnvelopedAspect> weighByEstimatedSize =
          (key, value) -> value.getValue().data().toString().getBytes().length;

      // batch loads data from entity client (restli or java)
      Function<Iterable<? extends Key>, Map<Key, EnvelopedAspect>> loader =
          (Iterable<? extends Key> keys) -> {
            Map<String, Map<String, Set<Key>>> keysByContextEntity = groupByContextEntity(keys);

            // load responses by context and combine
            return keysByContextEntity.entrySet().stream()
                .flatMap(
                    entry ->
                        loadByEntity(entry.getKey(), entry.getValue(), loadFunction)
                            .entrySet()
                            .stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          };

      // ideally the cache time comes from caching headers from service, but configuration driven
      // for now
      BiFunction<EntityClientCacheConfig, Key, Integer> ttlSeconds =
          (config, key) ->
              Optional.ofNullable(config.getEntityAspectTTLSeconds())
                  .orElse(Map.of())
                  .getOrDefault(key.getEntityName(), Map.of())
                  .getOrDefault(key.getAspectName(), config.getDefaultTTLSeconds());

      cache =
          ClientCache.<Key, EnvelopedAspect, EntityClientCacheConfig>builder()
              .weigher(weighByEstimatedSize)
              .config(config)
              .loadFunction(loader)
              .ttlSecondsFunction(ttlSeconds)
              .build(metricClazz);

      return new EntityClientCache(config, cache, loadFunction);
    }
  }

  private static Map<String, Map<String, Set<Key>>> groupByContextEntity(
      Iterable<? extends Key> keys) {
    // group by context
    Map<String, Set<Key>> byContext =
        StreamSupport.stream(keys.spliterator(), false)
            .collect(Collectors.groupingBy(Key::getContextId, Collectors.toSet()));

    // then by entity
    return byContext.entrySet().stream()
        .map(
            contextSet ->
                Pair.of(
                    contextSet.getKey(),
                    contextSet.getValue().stream()
                        .collect(Collectors.groupingBy(Key::getEntityName, Collectors.toSet()))))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private static Map<Key, EnvelopedAspect> loadByEntity(
      String contextId,
      Map<String, Set<Key>> keysByEntity,
      Function<CollectionKey, Map<Urn, EntityResponse>> loadFunction) {
    return keysByEntity.entrySet().stream()
        .flatMap(
            entry -> {
              Set<Urn> urns =
                  entry.getValue().stream().map(Key::getUrn).collect(Collectors.toSet());
              Set<String> aspects =
                  entry.getValue().stream().map(Key::getAspectName).collect(Collectors.toSet());
              return loadFunction
                  .apply(
                      CollectionKey.builder()
                          .contextId(contextId)
                          .urns(urns)
                          .aspectNames(aspects)
                          .build())
                  .entrySet()
                  .stream();
            })
        .flatMap(
            resp ->
                resp.getValue().getAspects().values().stream()
                    .map(
                        envAspect -> {
                          Key key =
                              Key.builder()
                                  .urn(resp.getKey())
                                  .aspectName(envAspect.getName())
                                  .build();
                          return Map.entry(key, envAspect);
                        }))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Data
  @Builder
  protected static class Key {
    private final String contextId;
    private final Urn urn;
    private final String aspectName;

    public String getEntityName() {
      return urn.getEntityType();
    }
  }

  @Data
  @Builder
  public static class CollectionKey {
    private final String contextId;
    private final Set<Urn> urns;
    private final Set<String> aspectNames;
  }
}
