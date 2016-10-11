/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller;

import java.util.Arrays;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.client.RestTemplate;

/**
 * Main class for the pz-servicecontroller. Launches the application
 * 
 * @author mlynum
 * @since 1.0
 */

@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
@EnableMongoRepositories("org.venice.piazza.serviceregistry.data.mongodb.repository")
/* Enable Boot application and MongoRepositories */
public class Application extends SpringBootServletInitializer {
	@Value("${http.max.total}")
	private int httpMaxTotal;
	@Value("${http.max.route}")
	private int httpMaxRoute;
	@Value("${http.request.timeout}")
	private int httpRequestTimeout;
	

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {

		return builder.sources(Application.class);
	}

	@Bean
	public RestTemplate restTemplate() {
		RestTemplate restTemplate = new RestTemplate();
		HttpClient httpClient = HttpClientBuilder.create().setMaxConnTotal(httpMaxTotal)
				.setMaxConnPerRoute(httpMaxRoute).build();
		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(httpRequestTimeout * 1000);
		factory.setConnectTimeout(httpRequestTimeout * 1000);
		restTemplate.setRequestFactory(factory);

		return restTemplate;
	}

	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(Application.class, args); //NOSONAR

		// now check to see if the first parameter is true, if so then test the health of the
		// Spring environment
		if (args.length == 1) {
			// Get the value of the first argument
			// If it is true then do a health check and print it out
			Boolean inspectBool = Boolean.valueOf(args[0]);
			if (inspectBool.booleanValue() == true) {
				inspectSprintEnv(ctx);
			}
		}

	}

	/**
	 * Determines if the appropriate bean definitions are available
	 */
	public static void inspectSprintEnv(ApplicationContext ctx) {

		System.out.println("Spring Boot Beans");
		System.out.println("-----------------");

		String[] beanNames = ctx.getBeanDefinitionNames();
		Arrays.sort(beanNames);
		for (String beanName : beanNames) {
			System.out.println(beanName);
		}

	}

	@Bean
	public LocalValidatorFactoryBean getLocalValidatorFactoryBean() {
		return new LocalValidatorFactoryBean();
	}
}
