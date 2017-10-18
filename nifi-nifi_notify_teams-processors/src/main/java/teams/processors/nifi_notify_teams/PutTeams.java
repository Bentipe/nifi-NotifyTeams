/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package teams.processors.nifi_notify_teams;

import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"teams","microsoft","put","message","v0.2"})
@CapabilityDescription("Sends a webhook message to teams")
@ReadsAttributes({@ReadsAttribute(attribute="title", description="the title that the message has to be sent"),@ReadsAttribute(attribute="body", description="gets the body, the content of the message")})
public class PutTeams extends AbstractProcessor {

    public static final PropertyDescriptor TITLE = new PropertyDescriptor
            .Builder().name("TITLE")
            .displayName("Title")
            .description("the title of the message")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor BODY = new PropertyDescriptor
            .Builder().name("BODY")
            .displayName("Body")
            .description("The body of the message")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor WEBHOOK = new PropertyDescriptor
            .Builder().name("WEBHOOK")
            .displayName("Webhook")
            .description("The link were the request has to be sent")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCC = new Relationship.Builder()
            .name("Success")
            .description("Success relation")
            .build();

    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("Failure")
            .description("Failure relation")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TITLE);
        descriptors.add(BODY);
        descriptors.add(WEBHOOK);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCC);
        relationships.add(REL_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        org.apache.http.client.HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(context.getProperty(WEBHOOK).getValue());
        try {
            StringEntity params = new StringEntity(new Gson().toJson(new Message(context.getProperty(TITLE).evaluateAttributeExpressions(flowFile).getValue(),context.getProperty(BODY).evaluateAttributeExpressions(flowFile).getValue())));
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAIL);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAIL);
        }
        session.transfer(flowFile, REL_SUCC);
    }
}
