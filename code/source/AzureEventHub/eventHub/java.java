package AzureEventHub.eventHub;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import com.azure.messaging.eventhubs.*;
import java.util.*;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
// --- <<IS-END-IMPORTS>> ---

public final class java

{
	// ---( internal utility methods )---

	final static java _instance = new java();

	static java _newInstance() { return new java(); }

	static java _cast(Object o) { return (java)o; }

	// ---( server methods )---




	public static final void sendMessageToAzure (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(sendMessageToAzure)>> ---
		// @sigtype java 3.5
		// [i] record:0:required config
		// [i] - field:0:required tenantId
		// [i] - field:0:required clientId
		// [i] - field:0:required clientSecret
		// [i] - field:0:required namespace
		// [i] - field:0:required eventHubName
		// [i] record:0:required inputDoc
		// [i] - field:0:required eventData
		// [i] - field:0:required partitionKey
		IDataCursor pipelineCursor = pipeline.getCursor();
		
		String tenantId = null;
		String clientId = null;
		String clientSecret = null;
		String namespace = null;
		String eventHubName = null;
		String eventData = null;
		String partitionKey = null;
		
		EventHubProducerClient producer = null; 
		
		try {
		    //  Read config
		    IData config = IDataUtil.getIData(pipelineCursor, "config");
		    if (config == null) {
		        throw new ServiceException("config document is missing");
		    }
		    
		
		    IDataCursor configCursor = config.getCursor();
		    tenantId = IDataUtil.getString(configCursor, "tenantId");
		    clientId = IDataUtil.getString(configCursor, "clientId");
		    clientSecret = IDataUtil.getString(configCursor, "clientSecret");
		    namespace = IDataUtil.getString(configCursor, "namespace");
		    eventHubName = IDataUtil.getString(configCursor, "eventHubName");
		    configCursor.destroy();
		
		    //  Read inputDoc
		    IData inputDoc = IDataUtil.getIData(pipelineCursor, "inputDoc");
		    if (inputDoc == null) {
		        throw new ServiceException("inputDoc is missing");
		    }
		
		    IDataCursor inputCursor = inputDoc.getCursor();
		    eventData = IDataUtil.getString(inputCursor, "eventData");
		    partitionKey = IDataUtil.getString(inputCursor, "partitionKey");
		    inputCursor.destroy();
		
		    //  Validate inputs
		    if (tenantId == null || clientId == null || clientSecret == null ||
		        namespace == null || eventHubName == null || eventData == null) {
		        throw new ServiceException("Missing required input parameters");
		    }
		
		    //  Normalize namespace
		    String fqNamespace =
		            namespace.endsWith(".servicebus.windows.net")
		                    ? namespace
		                    : namespace + ".servicebus.windows.net";
		
		    //  Create HTTP client (FIX FOR YOUR ERROR)
		   
		
		    //  Create credential (ATTACH HTTP CLIENT HERE )
		    TokenCredential credential =
		            new ClientSecretCredentialBuilder()
		                    .tenantId(tenantId)
		                    .clientId(clientId)
		                    .clientSecret(clientSecret)
		                    .build();
		
		    //  Create Event Hub producer
		    producer = new EventHubClientBuilder()
		            .credential(fqNamespace, eventHubName, credential)
		            .buildProducerClient();
		
		    //  Create batch (with optional partition key)
		    EventDataBatch batch;
		
		    if (partitionKey != null && partitionKey.trim().length() > 0) {
		        CreateBatchOptions options = new CreateBatchOptions()
		                .setPartitionKey(partitionKey);
		        batch = producer.createBatch(options);
		    } else {
		        batch = producer.createBatch();
		    }
		
		    //  Add event
		    EventData event = new EventData(eventData);
		
		    if (!batch.tryAdd(event)) {
		        throw new ServiceException("Event exceeds batch size limit");
		    }
		
		    //  Send message
		    producer.send(batch);
		
		    //  Output success
		    IDataUtil.put(pipelineCursor, "success", "true");
		    IDataUtil.put(pipelineCursor, "message", "Message sent successfully");
		
		} catch (Exception e) {
		
		    //  Logging
		    com.wm.util.JournalLogger.log(
		        com.wm.util.JournalLogger.ERROR,
		        com.wm.util.JournalLogger.FAC_FLOW_SVC,
		        com.wm.util.JournalLogger.ERROR,
		        "Azure EventHub Error: " + e.toString()
		    );
		
		    IDataUtil.put(pipelineCursor, "success", "false");
		    IDataUtil.put(pipelineCursor, "message", e.getMessage());
		
		    throw new ServiceException(e);
		
		} finally {
		
		    if (producer != null) {
		        producer.close();
		    }
		
		    pipelineCursor.destroy();
		}
		
		
		
		
			
		// --- <<IS-END>> ---

                
	}
}

