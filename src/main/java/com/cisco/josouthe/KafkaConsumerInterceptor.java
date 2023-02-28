package com.cisco.josouthe;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.EntryTypes;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.agent.api.impl.NoOpTransaction;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.logging.ISDKLogger;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerInterceptor extends AGenericInterceptor {
    private ISDKLogger logger;
    private Map<String,Boolean> methodMap;

    public KafkaConsumerInterceptor() {
        this.logger = getLogger();
        this.methodMap = new HashMap<>();
        logger.info(String.format("Initialized KafkaConsumerInterceptor version: %s build: %s author: %s", MetaData.VERSION, MetaData.BUILDTIMESTAMP, MetaData.GECOS));
    }

    @Override
    public Object onMethodBegin(Object objectIntercepted, String className, String methodName, Object[] params) {
        if( !hasAnnotation("org.springframework.kafka.annotation.KafkaListener", objectIntercepted, className, methodName) )
            return null;

        Transaction transaction = AppdynamicsAgent.getTransaction();
        if( transaction instanceof NoOpTransaction ) {
            transaction = AppdynamicsAgent.startTransaction("KafkaListener-notYetCorrelated", getCorrelationHeader(objectIntercepted), EntryTypes.POJO, false);
            logger.info(String.format("Started BT: %s for %s.%s(%d params)", transaction.getUniqueIdentifier(), className, methodName, (params==null?0:params.length)));
        } else {
            logger.info(String.format("Already Found BT: %s for %s.%s(%d params)", transaction.getUniqueIdentifier(), className, methodName,
                    (params == null ? 0 : params.length)));
        }
        return transaction;
    }

    private String getCorrelationHeader(Object objectIntercepted) {
        return null;
    }

    private boolean hasAnnotation(String annotationName, Object objectIntercepted, String className, String methodName) {
        Boolean boolValue = methodMap.get(String.format("%s.%s()",className,methodName));
        if( boolValue == null ) {
            outer: for (Method method : objectIntercepted.getClass().getMethods()) {
                if( method.getName().equalsIgnoreCase(methodName) ) {
                    for(Annotation annotation : method.getAnnotations()) {
                        if( annotation.toString().startsWith(annotationName,1) ) {
                            boolValue=true;
                            break outer;
                        }
                    }
                }
            }
            if( boolValue == null ) boolValue=false;
            methodMap.put(String.format("%s.%s()",className,methodName), boolValue);
        }
        return boolValue;
    }

    @Override
    public void onMethodEnd(Object state, Object objectIntercepted, String className, String methodName, Object[] params, Throwable exception, Object returnedObject) {
        if( state == null ) return;
        Transaction transaction = (Transaction) state;
        
        if( exception != null ) {
            transaction.markAsError(String.format("Kafka Consumer Error: %s", exception.getMessage()));
        }
        transaction.end();
    }

    @Override
    public List<Rule> initializeRules() {
        List<Rule> rules = new ArrayList<>();
        rules.add(new Rule.Builder("org.springframework.stereotype.Component")
                .classMatchType(SDKClassMatchType.HAS_CLASS_ANNOTATION)
                .methodMatchString(".*")
                .methodStringMatchType(SDKStringMatchType.REGEX)
                .build()
        );

        return rules;
    }
}
