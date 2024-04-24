package com.linkedin.gms.factory.auth;

import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class InviteTokenServiceFactory {

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Bean(name = "inviteTokenService")
  @Scope("singleton")
  @Nonnull
  protected InviteTokenService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new InviteTokenService(entityClient, _secretService);
  }
}
