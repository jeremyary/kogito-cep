/*
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.syseng.businessautomation.cep.services;

import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.drools.core.time.SessionPseudoClock;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.api.runtime.rule.EntryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;

@ApplicationScoped
public class SampleService {

    KieSession session;
    EntryPoint entry;
    SessionPseudoClock sessionClock;

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleService.class);

    @PostConstruct
    public void init() {

        try {

            LOGGER.info("setting up rule session...");
            KieServices ks = KieServices.Factory.get();
            KieContainer kContainer = ks.getKieClasspathContainer();
            session = kContainer.newKieSession("simpleCEPKS");

            // in most uses, you'd want to create/use custom listeners to focus in on the triggers that you care about
            // but these defaults work in a pinch - the output is just not super-friendly to read through
            LOGGER.info("attaching listeners...");
            session.addEventListener(new DebugAgendaEventListener());
            session.addEventListener(new DebugRuleRuntimeEventListener());

            // it's possible this is no longer needed, but it used to be required for session clock manipulation,
            // event durations, and the ability to reason over the absence of events
            LOGGER.info("swap rule session to streaming for CEP...");
            KieBaseConfiguration config = ks.newKieBaseConfiguration();
            config.setOption(EventProcessingOption.STREAM);

            // set a pseudo-clock on the session so that we can manipulate event entry timing rather than spoofing "timestamps"
            session.getSessionConfiguration().setOption(ClockTypeOption.get("pseudo"));
            sessionClock = session.getSessionClock();


            // note that if you programmatically use an entry point that is not referenced in a rule somewhere, this will
            // throw a NPE - see 'from "sample-service"' I added to rule for example, but you could do away with it and
            // just use session.insert when the use case just needs a single entry
            entry = session.getEntryPoint("sample-service");

        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    @PreDestroy
    public void teardown() {
        session.dispose();
    }

    public void run(CloudEvent<? extends Attributes, JsonObject> event) {

        // note that you can do 'update' and 'retract' as well since the rule session is long-lived as opposed to a
        // stateless use case where you'd just fire once, get the results, and discard the whole thing
        LOGGER.info("inserting event & triggering fire...");
        entry.insert(event);

        // If you want to see the "meets" rule fail, you can uncomment this line to advance the clock beyond the 5 second timeframe
        //sessionClock.advanceTime(10, TimeUnit.SECONDS);

        session.fireAllRules();
    }

    public String getValue() {
        return "";
    }
}
