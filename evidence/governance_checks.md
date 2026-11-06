# Unity Catalog governance — enforced, not just declared

Captured 2026-11-06T21:56Z as `sabir.ayappalli@databricks.com`.

## 1 · Policies in force

```sql
DESCRIBE FUNCTION EXTENDED sabir_freshmart.gov.region_filter
```

```json
[
  {
    "function_desc": "Function:      sabir_freshmart.gov.region_filter"
  },
  {
    "function_desc": "Type:          SCALAR"
  },
  {
    "function_desc": "Input:         region STRING COLLATE UTF8_BINARY"
  },
  {
    "function_desc": "Returns:       BOOLEAN"
  },
  {
    "function_desc": "Comment:       Row filter: HQ sees all regions; regional managers only their own region (group freshmart_\u003cregion\u003e_managers or mapping table)."
  },
  {
    "function_desc": "Collation:     UTF8_BINARY"
  },
  {
    "function_desc": "Deterministic: true"
  },
  {
    "function_desc": "Data Access:   READS SQL DATA"
  },
  {
    "function_desc": "Configs:       runtime.databricks.sql.functions.counter_diff.enabled=true"
  },
  {
    "function_desc": "               runtime.databricks.sql.functions.time_bucket.enabled=true"
  },
  {
    "function_desc": "               spark.connect.cloudfetch.transparent.enabled=true"
  },
  {
    "function_desc": "               spark.connect.cloudfetch.transparent.prefetchDepth=6"
  },
  {
    "function_desc": "               spark.connect.inline.result.byteLimit.enabled=true"
  },
  {
    "function_desc": "               spark.connect.qrc.transparent.enabled=false"
  },
  {
    "function_desc": "               spark.connect.resultCachingShadowMode.billingMeter.enabled=true"
  },
  {
    "function_desc": "               spark.connect.session.connectML.enabled=true"
  },
  {
    "function_desc": "               spark.connect.session.connectML.mlCache.memoryControl.maxModelSize=268435456"
  },
  {
    "function_desc": "               spark.connect.session.planCache.analyzedPlan.enabled=false"
  },
  {
    "function_desc": "               spark.connect.session.planCompression.threshold=10485760"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.initialSize=4"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.maxRetries=100"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.partitionJitterEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.retriableInvokeStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.retryMaxDelayMs=120000"
  },
  {
    "function_desc": "               spark.databricks.ai.grpc.sync.retriableStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED"
  },
  {
    "function_desc": "               spark.databricks.docingest.apdfl.sandbox.enabled=false"
  },
  {
    "function_desc": "               spark.databricks.docingest.batchEval.size=8"
  },
  {
    "function_desc": "               spark.databricks.docingest.contentElementTypes.excluded="
  },
  {
    "function_desc": "               spark.databricks.docingest.maxDocumentPages=1000"
  },
  {
    "function_desc": "               spark.databricks.docingest.pagination.concurrency=1"
  },
  {
    "function_desc": "               spark.databricks.docingest.pdf.drawFlags=USE_ANNOT_FACES"
  },
  {
    "function_desc": "               spark.databricks.docingest.stage2.contentModel=databricks-qwen-3-vl-4b-parse-s2"
  },
  {
    "function_desc": "               spark.databricks.docingest.stage2.descriptionModel=databricks-gemma-3-12b"
  },
  {
    "function_desc": "               spark.databricks.docingest.version.default=2.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.hermes.embeddingBatch.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.checkIntervalMs=3000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.gcPressure.aggressiveReductionFactor=0.5"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.gcPressure.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.jvmRuntime.heapThresholdPercent=95.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.throttleCooldownMs=6000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.aimdLimiter.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.aimdLimiter.max=200"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.globalMaxInFlightElements=10240"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxBufferedResults=512"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxInFlightElements=200"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxWaitTimeSeconds=172800"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.initialRetryInterval=100"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.maxRetryInterval=10000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.retryIntervalJitterFactor=1.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.richArguments.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.hermes.extendedTypeConversion.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.expression.aiFunctions.repartition=0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiForecast.enabled=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.aclValidateAllFunctions=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.clusterSizeBasedGlobalParallelism.scaleFactor=512.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.debugLogEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.dynamicPoolSizeEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize.aiParseDocument=64"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdCurrentQpsIncreaseRatio=0.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdSuccessRatio=0.95"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdTotalQpsIncreaseRatio=0.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.taskWaitTimeInSeconds=1000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.threadKeepAliveTimeInSeconds=600"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.useDynamicTaskQueueExecutor=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batch.aiQuery.embedding.request.size=40"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batch.execution.size=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchInferenceApi.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchSession.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchSession.terminate.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.decimal.dataType.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.disabled=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.embeddingsEndpointName=databricks-gte-large-en"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.extract.tileMetadata.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.grpc.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.directBuilder.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.failOnError.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.files.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.errorTemplate.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.partitionProcessorConf.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermesEnabledFunctions=ai_classify,ai_extract,ai_parse_document,ai_prep_search,ai_query,ai_search,ai_mask,ai_summarize,ai_translate,ai_transcribe,ai_fix_grammar,ai_analyze_sentiment,ai_decide"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.includeWorkspaceUrl=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.model.parameters.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.modelEndpointTypeParsing.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.multiModality.model.list=databricks-llama-4-maverick,databricks-claude-sonnet-4,databricks-claude-3-7-sonnet,databricks-gemma-3-12b,databricks-gpt-5,databricks-gpt-5-mini,databricks-gpt-5-nano,databricks-gpt-5-1,databricks-gpt-5-2,databricks-gemini-2-5-flash,databricks-gemini-2-5-pro,databricks-gemini-3-flash,databricks-gemini-3-pro,databricks-claude-haiku-4-5"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.multiModality.useCapabilitiesApi=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.nullInputJsonOutput={\"response\":null,\"error_message\":null}"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.batch.execution.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.translate.instruction=### Instruction\n\nTranslate the provided text to %s, and output only the translated text\nin the target language in this format: \u003cDBSQLAI\u003etranslated text\u003c/DBSQLAI\u003e.\n\nIf the text is already in the target language, output the provided text.\n\n### Text\n\n%s"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.registry.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteFunction.grpc.batch.maxConcurrentRequests=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteHttpClient.maxConnections=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteHttpClient.timeoutInSeconds=360"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.safe.inference.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.scalingReasonLogging.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.useDedicatedHttpClient=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.useGrpcForSessionManagement=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.initialRetryIntervalMillis=1000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.maxRetryInterval=120000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.retryIntervalMultiplierFactor=2.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiGen.endpointName=databricks-meta-llama-3-3-70b-instruct"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.hybrid.reasoning.foundation.model.list=databricks-claude-3-7-sonnet,databricks-gemini-2-5-pro,databricks-gemini-2-5-flash,databricks-gemini-3-flash"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.openAI.oSeries.model.list=o1-mini,o1,o3-mini,o3,o4-mini,gpt-5,gpt-5-mini,gpt-5-nano,GPT-5,GPT-5 Mini,GPT-5 Nano"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.temperature.whitelistMode.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.ucModelService.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.vectorSearch.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.vectorSearch.use.indexIdentifierOnly=true"
  },
  {
    "function_desc": "               spark.databricks.sql.rowColumnAccess.useEffectivePolicies.enabled=true"
  },
  {
    "function_desc": "               spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput=false"
  },
  {
    "function_desc": "               spark.sql.ansi.enabled=true"
  },
  {
    "function_desc": "               spark.sql.artifact.isolation.enabled=true"
  },
  {
    "function_desc": "               spark.sql.connect.ldp.enableLdpPipelinesHandler=true"
  },
  {
    "function_desc": "               spark.sql.connect.shuffleDependency.fileCleanup.enabled=true"
  },
  {
    "function_desc": "               spark.sql.copyInto.timeout.threadDump.enabled=false"
  },
  {
    "function_desc": "               spark.sql.cte.recursion.enabled=true"
  },
  {
    "function_desc": "               spark.sql.decfloatType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.excel.streaming.enabled=true"
  },
  {
    "function_desc": "               spark.sql.fileType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.redirectLogging.enabled=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.redirectStackTraceLogging.enabled=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.retryOn400TimeoutError=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.retryOnSocketTimeoutException=true"
  },
  {
    "function_desc": "               spark.sql.hive.convertCTAS=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.disallowMisalignedColumns.enabled=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.enabled=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.oldDPOCodePath.enabled=true"
  },
  {
    "function_desc": "               spark.sql.legacy.blockCreateTempTableUsingProvider=true"
  },
  {
    "function_desc": "               spark.sql.legacy.codingErrorAction=true"
  },
  {
    "function_desc": "               spark.sql.legacy.createHiveTableByDefault=false"
  },
  {
    "function_desc": "               spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false"
  },
  {
    "function_desc": "               spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled=false"
  },
  {
    "function_desc": "               spark.sql.legacy.observeMetricsAggregateAllAttempts=true"
  },
  {
    "function_desc": "               spark.sql.legacy.useLegacyXMLParser.migrationValidation.enabled=false"
  },
  {
    "function_desc": "               spark.sql.orc.compression.codec=snappy"
  },
  {
    "function_desc": "               spark.sql.parquet.compression.codec=snappy"
  },
  {
    "function_desc": "               spark.sql.path.enabled=true"
  },
  {
    "function_desc": "               spark.sql.photon.skipIndexForTextSearch.indexingBitmapAgg.forceSerializedOutput=false"
  },
  {
    "function_desc": "               spark.sql.photon.skipIndexForTextSearch.indexingBitmapAndAgg.enabled=true"
  },
  {
    "function_desc": "               spark.sql.pivot.emptyBucketReturnsAggregateDefault=false"
  },
  {
    "function_desc": "               spark.sql.readSideCharPadding=true"
  },
  {
    "function_desc": "               spark.sql.scripting.enabled=true"
  },
  {
    "function_desc": "               spark.sql.scripting.maxNumberOfCharacters=1000000"
  },
  {
    "function_desc": "               spark.sql.scripting.maxNumberOfLines=30000"
  },
  {
    "function_desc": "               spark.sql.searchIndex.alterColumnSyntax.enabled=true"
  },
  {
    "function_desc": "               spark.sql.searchIndex.clusterBy.enabled=true"
  },
  {
    "function_desc": "               spark.sql.searchIndex.clusterBy.generalization.enabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.asyncCreation.enabled=false"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.manualRefresh.disabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.rowIdWatermark.enabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.showInDescribeTable=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.ucIntegration.enabled=false"
  },
  {
    "function_desc": "               spark.sql.session.timeZone=Etc/UTC"
  },
  {
    "function_desc": "               spark.sql.shuffleDependency.skipMigration.enabled=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.enabled=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.incremental.maxBytesThreshold=10000000000000"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.ngramIndex.parameterCalculation=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.patternBucketCount=524288"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.rowsBatchSize=32"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.v2.enabled=true"
  },
  {
    "function_desc": "               spark.sql.sources.commitProtocolClass=com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol"
  },
  {
    "function_desc": "               spark.sql.sources.default=delta"
  },
  {
    "function_desc": "               spark.sql.stableDerivedColumnAlias.enabled=true"
  },
  {
    "function_desc": "               spark.sql.standardIndex.costBasedSelection.enabled=true"
  },
  {
    "function_desc": "               spark.sql.standardIndex.enabled=true"
  },
  {
    "function_desc": "               spark.sql.streaming.checkpoint.fileChecksum.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.optimizer.subqueryEarlyUnnesting.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
  },
  {
    "function_desc": "               spark.sql.streaming.statefulOperator.stateRebalancing.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.stopTimeout=15s"
  },
  {
    "function_desc": "               spark.sql.timeType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.timestampNanosTypes.enabled=false"
  },
  {
    "function_desc": "               spark.sql.validateColumnNamesSync.throwException=true"
  },
  {
    "function_desc": "               spark.sql.variable.substitute=false"
  },
  {
    "function_desc": "               spark.sql.variant.pushVariantIntoScan=true"
  },
  {
    "function_desc": "               spark.sql.variant.pushVariantIntoScan.deferCastError=true"
  },
  {
    "function_desc": "               spark.sql.variant.writeShredding.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.numCentroidsScalingFactor=1"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.optimization.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.optimization.enabledForCommands=true"
  },
  {
    "function_desc": "               spark.sql.windowExec.buffer.in.memory.size.threshold=134217728"
  },
  {
    "function_desc": "Owner:         sabir.ayappalli@databricks.com"
  },
  {
    "function_desc": "Create Time:   Fri Nov 06 01:05:51 UTC 2026"
  },
  {
    "function_desc": "Body:          is_account_group_member('freshmart_hq')\n      OR is_account_group_member(concat('freshmart_', lower(replace(region, ' ', '')), '_managers'))\n      OR EXISTS (SELECT 1 FROM sabir_freshmart.gov.user_region_access a WHERE a.principal = current_user() AND a.region IN ('ALL', region))"
  },
  {
    "function_desc": "SQL Path:      system.builtin, system.ai, main.default"
  }
]
```

```sql
DESCRIBE FUNCTION EXTENDED sabir_freshmart.gov.mask_unit_cost
```

```json
[
  {
    "function_desc": "Function:      sabir_freshmart.gov.mask_unit_cost"
  },
  {
    "function_desc": "Type:          SCALAR"
  },
  {
    "function_desc": "Input:         unit_cost DOUBLE"
  },
  {
    "function_desc": "Returns:       DOUBLE"
  },
  {
    "function_desc": "Comment:       Column mask: supplier unit cost (margin-sensitive) visible to finance / HQ only."
  },
  {
    "function_desc": "Collation:     UTF8_BINARY"
  },
  {
    "function_desc": "Deterministic: true"
  },
  {
    "function_desc": "Data Access:   READS SQL DATA"
  },
  {
    "function_desc": "Configs:       runtime.databricks.sql.functions.counter_diff.enabled=true"
  },
  {
    "function_desc": "               runtime.databricks.sql.functions.time_bucket.enabled=true"
  },
  {
    "function_desc": "               spark.connect.cloudfetch.transparent.enabled=true"
  },
  {
    "function_desc": "               spark.connect.cloudfetch.transparent.prefetchDepth=6"
  },
  {
    "function_desc": "               spark.connect.inline.result.byteLimit.enabled=true"
  },
  {
    "function_desc": "               spark.connect.qrc.transparent.enabled=false"
  },
  {
    "function_desc": "               spark.connect.resultCachingShadowMode.billingMeter.enabled=true"
  },
  {
    "function_desc": "               spark.connect.session.connectML.enabled=true"
  },
  {
    "function_desc": "               spark.connect.session.connectML.mlCache.memoryControl.maxModelSize=268435456"
  },
  {
    "function_desc": "               spark.connect.session.planCache.analyzedPlan.enabled=false"
  },
  {
    "function_desc": "               spark.connect.session.planCompression.threshold=10485760"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.initialSize=4"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.maxRetries=100"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.partitionJitterEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.retriableInvokeStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED"
  },
  {
    "function_desc": "               spark.databricks.ai.adaptiveBatch.retryMaxDelayMs=120000"
  },
  {
    "function_desc": "               spark.databricks.ai.grpc.sync.retriableStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED"
  },
  {
    "function_desc": "               spark.databricks.docingest.apdfl.sandbox.enabled=false"
  },
  {
    "function_desc": "               spark.databricks.docingest.batchEval.size=8"
  },
  {
    "function_desc": "               spark.databricks.docingest.contentElementTypes.excluded="
  },
  {
    "function_desc": "               spark.databricks.docingest.maxDocumentPages=1000"
  },
  {
    "function_desc": "               spark.databricks.docingest.pagination.concurrency=1"
  },
  {
    "function_desc": "               spark.databricks.docingest.pdf.drawFlags=USE_ANNOT_FACES"
  },
  {
    "function_desc": "               spark.databricks.docingest.stage2.contentModel=databricks-qwen-3-vl-4b-parse-s2"
  },
  {
    "function_desc": "               spark.databricks.docingest.stage2.descriptionModel=databricks-gemma-3-12b"
  },
  {
    "function_desc": "               spark.databricks.docingest.version.default=2.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.hermes.embeddingBatch.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.checkIntervalMs=3000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.gcPressure.aggressiveReductionFactor=0.5"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.gcPressure.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.jvmRuntime.heapThresholdPercent=95.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.memoryGuardrails.throttleCooldownMs=6000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.aimdLimiter.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.aimdLimiter.max=200"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.globalMaxInFlightElements=10240"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxBufferedResults=512"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxInFlightElements=200"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.partitionProcessor.maxWaitTimeSeconds=172800"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.initialRetryInterval=100"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.maxRetryInterval=10000"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.retryIntervalJitterFactor=1.0"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.grpc.richArguments.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.ai.remoteFunction.hermes.extendedTypeConversion.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.expression.aiFunctions.repartition=0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiForecast.enabled=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.aclValidateAllFunctions=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.clusterSizeBasedGlobalParallelism.scaleFactor=512.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.debugLogEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.dynamicPoolSizeEnabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize.aiParseDocument=64"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdCurrentQpsIncreaseRatio=0.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdSuccessRatio=0.95"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdTotalQpsIncreaseRatio=0.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.taskWaitTimeInSeconds=1000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.threadKeepAliveTimeInSeconds=600"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.useDynamicTaskQueueExecutor=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batch.aiQuery.embedding.request.size=40"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batch.execution.size=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchInferenceApi.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchSession.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.batchSession.terminate.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.decimal.dataType.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.disabled=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.embeddingsEndpointName=databricks-gte-large-en"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.extract.tileMetadata.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.grpc.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.directBuilder.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.failOnError.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.files.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.errorTemplate.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermes.partitionProcessorConf.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.hermesEnabledFunctions=ai_classify,ai_extract,ai_parse_document,ai_prep_search,ai_query,ai_search,ai_mask,ai_summarize,ai_translate,ai_transcribe,ai_fix_grammar,ai_analyze_sentiment,ai_decide"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.includeWorkspaceUrl=false"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.model.parameters.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.modelEndpointTypeParsing.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.multiModality.model.list=databricks-llama-4-maverick,databricks-claude-sonnet-4,databricks-claude-3-7-sonnet,databricks-gemma-3-12b,databricks-gpt-5,databricks-gpt-5-mini,databricks-gpt-5-nano,databricks-gpt-5-1,databricks-gpt-5-2,databricks-gemini-2-5-flash,databricks-gemini-2-5-pro,databricks-gemini-3-flash,databricks-gemini-3-pro,databricks-claude-haiku-4-5"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.multiModality.useCapabilitiesApi=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.nullInputJsonOutput={\"response\":null,\"error_message\":null}"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.batch.execution.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.translate.instruction=### Instruction\n\nTranslate the provided text to %s, and output only the translated text\nin the target language in this format: \u003cDBSQLAI\u003etranslated text\u003c/DBSQLAI\u003e.\n\nIf the text is already in the target language, output the provided text.\n\n### Text\n\n%s"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.registry.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteFunction.grpc.batch.maxConcurrentRequests=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteHttpClient.maxConnections=2048"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.remoteHttpClient.timeoutInSeconds=360"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.safe.inference.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.scalingReasonLogging.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.useDedicatedHttpClient=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.useGrpcForSessionManagement=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.initialRetryIntervalMillis=1000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.maxRetryInterval=120000"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiFunctions.vectorSearch.retryIntervalMultiplierFactor=2.0"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiGen.endpointName=databricks-meta-llama-3-3-70b-instruct"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.hybrid.reasoning.foundation.model.list=databricks-claude-3-7-sonnet,databricks-gemini-2-5-pro,databricks-gemini-2-5-flash,databricks-gemini-3-flash"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.openAI.oSeries.model.list=o1-mini,o1,o3-mini,o3,o4-mini,gpt-5,gpt-5-mini,gpt-5-nano,GPT-5,GPT-5 Mini,GPT-5 Nano"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.temperature.whitelistMode.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.aiQuery.ucModelService.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.vectorSearch.enabled=true"
  },
  {
    "function_desc": "               spark.databricks.sql.functions.vectorSearch.use.indexIdentifierOnly=true"
  },
  {
    "function_desc": "               spark.databricks.sql.rowColumnAccess.useEffectivePolicies.enabled=true"
  },
  {
    "function_desc": "               spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput=false"
  },
  {
    "function_desc": "               spark.sql.ansi.enabled=true"
  },
  {
    "function_desc": "               spark.sql.artifact.isolation.enabled=true"
  },
  {
    "function_desc": "               spark.sql.connect.ldp.enableLdpPipelinesHandler=true"
  },
  {
    "function_desc": "               spark.sql.connect.shuffleDependency.fileCleanup.enabled=true"
  },
  {
    "function_desc": "               spark.sql.copyInto.timeout.threadDump.enabled=false"
  },
  {
    "function_desc": "               spark.sql.cte.recursion.enabled=true"
  },
  {
    "function_desc": "               spark.sql.decfloatType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.excel.streaming.enabled=true"
  },
  {
    "function_desc": "               spark.sql.fileType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.redirectLogging.enabled=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.redirectStackTraceLogging.enabled=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.retryOn400TimeoutError=true"
  },
  {
    "function_desc": "               spark.sql.functions.remoteHttpClient.retryOnSocketTimeoutException=true"
  },
  {
    "function_desc": "               spark.sql.hive.convertCTAS=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.disallowMisalignedColumns.enabled=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.enabled=true"
  },
  {
    "function_desc": "               spark.sql.insertIntoReplaceUsing.oldDPOCodePath.enabled=true"
  },
  {
    "function_desc": "               spark.sql.legacy.blockCreateTempTableUsingProvider=true"
  },
  {
    "function_desc": "               spark.sql.legacy.codingErrorAction=true"
  },
  {
    "function_desc": "               spark.sql.legacy.createHiveTableByDefault=false"
  },
  {
    "function_desc": "               spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false"
  },
  {
    "function_desc": "               spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled=false"
  },
  {
    "function_desc": "               spark.sql.legacy.observeMetricsAggregateAllAttempts=true"
  },
  {
    "function_desc": "               spark.sql.legacy.useLegacyXMLParser.migrationValidation.enabled=false"
  },
  {
    "function_desc": "               spark.sql.orc.compression.codec=snappy"
  },
  {
    "function_desc": "               spark.sql.parquet.compression.codec=snappy"
  },
  {
    "function_desc": "               spark.sql.path.enabled=true"
  },
  {
    "function_desc": "               spark.sql.photon.skipIndexForTextSearch.indexingBitmapAgg.forceSerializedOutput=false"
  },
  {
    "function_desc": "               spark.sql.photon.skipIndexForTextSearch.indexingBitmapAndAgg.enabled=true"
  },
  {
    "function_desc": "               spark.sql.pivot.emptyBucketReturnsAggregateDefault=false"
  },
  {
    "function_desc": "               spark.sql.readSideCharPadding=true"
  },
  {
    "function_desc": "               spark.sql.scripting.enabled=true"
  },
  {
    "function_desc": "               spark.sql.scripting.maxNumberOfCharacters=1000000"
  },
  {
    "function_desc": "               spark.sql.scripting.maxNumberOfLines=30000"
  },
  {
    "function_desc": "               spark.sql.searchIndex.alterColumnSyntax.enabled=true"
  },
  {
    "function_desc": "               spark.sql.searchIndex.clusterBy.enabled=true"
  },
  {
    "function_desc": "               spark.sql.searchIndex.clusterBy.generalization.enabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.asyncCreation.enabled=false"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.manualRefresh.disabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.rowIdWatermark.enabled=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.showInDescribeTable=true"
  },
  {
    "function_desc": "               spark.sql.secondaryIndex.ucIntegration.enabled=false"
  },
  {
    "function_desc": "               spark.sql.session.timeZone=Etc/UTC"
  },
  {
    "function_desc": "               spark.sql.shuffleDependency.skipMigration.enabled=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.enabled=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.incremental.maxBytesThreshold=10000000000000"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.ngramIndex.parameterCalculation=true"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.patternBucketCount=524288"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.rowsBatchSize=32"
  },
  {
    "function_desc": "               spark.sql.skipIndexForTextSearch.v2.enabled=true"
  },
  {
    "function_desc": "               spark.sql.sources.commitProtocolClass=com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol"
  },
  {
    "function_desc": "               spark.sql.sources.default=delta"
  },
  {
    "function_desc": "               spark.sql.stableDerivedColumnAlias.enabled=true"
  },
  {
    "function_desc": "               spark.sql.standardIndex.costBasedSelection.enabled=true"
  },
  {
    "function_desc": "               spark.sql.standardIndex.enabled=true"
  },
  {
    "function_desc": "               spark.sql.streaming.checkpoint.fileChecksum.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.optimizer.subqueryEarlyUnnesting.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
  },
  {
    "function_desc": "               spark.sql.streaming.statefulOperator.stateRebalancing.enabled=false"
  },
  {
    "function_desc": "               spark.sql.streaming.stopTimeout=15s"
  },
  {
    "function_desc": "               spark.sql.timeType.enabled=false"
  },
  {
    "function_desc": "               spark.sql.timestampNanosTypes.enabled=false"
  },
  {
    "function_desc": "               spark.sql.validateColumnNamesSync.throwException=true"
  },
  {
    "function_desc": "               spark.sql.variable.substitute=false"
  },
  {
    "function_desc": "               spark.sql.variant.pushVariantIntoScan=true"
  },
  {
    "function_desc": "               spark.sql.variant.pushVariantIntoScan.deferCastError=true"
  },
  {
    "function_desc": "               spark.sql.variant.writeShredding.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.numCentroidsScalingFactor=1"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.optimization.enabled=true"
  },
  {
    "function_desc": "               spark.sql.vectorIndex.optimization.enabledForCommands=true"
  },
  {
    "function_desc": "               spark.sql.windowExec.buffer.in.memory.size.threshold=134217728"
  },
  {
    "function_desc": "Owner:         sabir.ayappalli@databricks.com"
  },
  {
    "function_desc": "Create Time:   Fri Nov 06 01:05:52 UTC 2026"
  },
  {
    "function_desc": "Body:          CASE WHEN is_account_group_member('freshmart_finance')\n              OR EXISTS (SELECT 1 FROM sabir_freshmart.gov.user_region_access a WHERE a.principal = current_user() AND a.region = 'ALL')\n         THEN unit_cost ELSE NULL END"
  },
  {
    "function_desc": "SQL Path:      system.builtin, system.ai, main.default"
  }
]
```

```sql
SELECT * FROM sabir_freshmart.gov.user_region_access ORDER BY role, principal
```

```json
[
  {
    "principal": "5b2c13f0-0134-4430-8465-0317907aff13",
    "region": "ALL",
    "role": "app_service_principal"
  },
  {
    "principal": "sabir.ayappalli@databricks.com",
    "region": "ALL",
    "role": "hq_supply_chain_admin"
  },
  {
    "principal": "northeast.manager@freshmart.example",
    "region": "Northeast",
    "role": "regional_manager"
  },
  {
    "principal": "west.manager@freshmart.example",
    "region": "West",
    "role": "regional_manager"
  }
]
```

## 2 · As HQ (mapped to region ALL): all regions visible, supplier cost visible

```sql
SELECT region, count(*) store_skus FROM sabir_freshmart.ml.stockout_risk_scores GROUP BY region ORDER BY region
```

```json
[
  {
    "region": "Midwest",
    "store_skus": "4000"
  },
  {
    "region": "Northeast",
    "store_skus": "4000"
  },
  {
    "region": "Southeast",
    "store_skus": "4000"
  },
  {
    "region": "West",
    "store_skus": "4000"
  }
]
```

```sql
SELECT sku_id, sku_name, unit_price, unit_cost FROM sabir_freshmart.silver.products ORDER BY sku_id LIMIT 3
```

```json
[
  {
    "sku_id": "SKU00001",
    "sku_name": "Golden Acre Milk Classic",
    "unit_cost": "3.95",
    "unit_price": "5.49"
  },
  {
    "sku_id": "SKU00002",
    "sku_name": "Valley Farms Yogurt Organic",
    "unit_cost": "2.05",
    "unit_price": "3.02"
  },
  {
    "sku_id": "SKU00003",
    "sku_name": "Peak Cheese Family Size",
    "unit_cost": "3.11",
    "unit_price": "4.27"
  }
]
```

## 3 · Same queries after removing our HQ mapping (we are now a user with no region and not in finance)

```sql
SELECT count(*) AS visible_store_skus FROM sabir_freshmart.ml.stockout_risk_scores
```

```json
[
  {
    "visible_store_skus": "0"
  }
]
```

```sql
SELECT sku_id, sku_name, unit_price, unit_cost FROM sabir_freshmart.silver.products ORDER BY sku_id LIMIT 3
```

```json
[
  {
    "sku_id": "SKU00001",
    "sku_name": "Golden Acre Milk Classic",
    "unit_cost": "",
    "unit_price": "5.49"
  },
  {
    "sku_id": "SKU00002",
    "sku_name": "Valley Farms Yogurt Organic",
    "unit_cost": "",
    "unit_price": "3.02"
  },
  {
    "sku_id": "SKU00003",
    "sku_name": "Peak Cheese Family Size",
    "unit_cost": "",
    "unit_price": "4.27"
  }
]
```

_HQ mapping restored:_

```sql
SELECT count(*) AS visible_store_skus FROM sabir_freshmart.ml.stockout_risk_scores
```

```json
[
  {
    "visible_store_skus": "16000"
  }
]
```

## 4 · Tags

```sql
SELECT table_schema, table_name, tag_name, tag_value FROM sabir_freshmart.information_schema.table_tags ORDER BY 1, 2, 3
```

```json
```

```sql
SELECT schema_name, tag_name, tag_value FROM sabir_freshmart.information_schema.schema_tags ORDER BY 1
```

```json
[
  {
    "schema_name": "bronze",
    "tag_name": "tier",
    "tag_value": "bronze"
  },
  {
    "schema_name": "gold",
    "tag_name": "tier",
    "tag_value": "gold"
  },
  {
    "schema_name": "ml",
    "tag_name": "tier",
    "tag_value": "ml"
  },
  {
    "schema_name": "serving",
    "tag_name": "tier",
    "tag_value": "serving"
  },
  {
    "schema_name": "silver",
    "tag_name": "tier",
    "tag_value": "silver"
  }
]
```

```sql
SELECT table_schema, table_name, column_name, tag_name, tag_value FROM sabir_freshmart.information_schema.column_tags
```

```json
```

## 5 · Lineage (system.access.table_lineage): raw files → bronze → silver → gold → ml → serving

```sql
SELECT DISTINCT coalesce(source_table_full_name, source_path) AS source, target_table_full_name AS target FROM system.access.table_lineage WHERE target_table_catalog = 'sabir_freshmart' AND event_date >= '2026-11-02' AND coalesce(source_table_full_name, source_path) IS NOT NULL AND target_table_full_name IS NOT NULL ORDER BY 2, 1
```

```json
[
  {
    "source": "sabir_freshmart.gold.store_sku_daily",
    "target": "sabir_freshmart.gold.stockout_metrics"
  },
  {
    "source": "sabir_freshmart.gold.store_sku_daily",
    "target": "sabir_freshmart.gold.store_daily_kpis"
  },
  {
    "source": "sabir_freshmart.silver.deliveries",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.silver.inventory_snapshots",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.silver.pos_sales",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.silver.products",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.silver.promo_calendar",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.silver.stores",
    "target": "sabir_freshmart.gold.store_sku_daily"
  },
  {
    "source": "sabir_freshmart.ml.stockout_risk_scores",
    "target": "sabir_freshmart.ml.alert_ai_reasons"
  },
  {
    "source": "sabir_freshmart.gold.store_sku_daily",
    "target": "sabir_freshmart.ml.store_sku_features"
  },
  {
    "source": "sabir_freshmart.ml.alert_ai_reasons",
    "target": "sabir_freshmart.serving.stockout_alerts"
  },
  {
    "source": "sabir_freshmart.ml.stockout_risk_scores",
    "target": "sabir_freshmart.serving.stockout_alerts"
  },
  {
    "source": "sabir_freshmart.serving.stockout_alerts",
    "target": "sabir_freshmart.serving.store_briefings"
  },
  {
    "source": "sabir_freshmart.bronze.deliveries_raw",
    "target": "sabir_freshmart.silver.deliveries"
  },
  {
    "source": "sabir_freshmart.bronze.inventory_snapshots_raw",
    "target": "sabir_freshmart.silver.inventory_snapshots"
  },
  {
    "source": "sabir_freshmart.bronze.pos_sales_raw",
    "target": "sabir_freshmart.silver.pos_sales"
  },
  {
    "source": "sabir_freshmart.bronze.products_raw",
    "target": "sabir_freshmart.silver.products"
  },
  {
    "source": "sabir_freshmart.bronze.promotions_raw",
    "target": "sabir_freshmart.silver.promo_calendar"
  },
  {
    "source": "sabir_freshmart.bronze.stores_raw",
    "target": "sabir_freshmart.silver.promo_calendar"
  },
  {
    "source": "sabir_freshmart.bronze.stores_raw",
    "target": "sabir_freshmart.silver.stores"
  }
]
```

## 6 · Metric view: one governed KPI definition (used by Genie and the app)

```sql
SELECT `Region`, MEASURE(`Net Sales`) net_sales, MEASURE(`Estimated Lost Sales`) est_lost_sales, MEASURE(`On Shelf Availability`) osa, MEASURE(`Promo Out Of Stock Rate`) promo_oos FROM sabir_freshmart.gold.stockout_metrics GROUP BY ALL ORDER BY 1
```

```json
[
  {
    "Region": "Midwest",
    "est_lost_sales": "1110044.655699997",
    "net_sales": "7.857348556003082E7",
    "osa": "0.9506489071038251",
    "promo_oos": "0.14685676392572944"
  },
  {
    "Region": "Northeast",
    "est_lost_sales": "1128664.0983999919",
    "net_sales": "7.842599238002756E7",
    "osa": "0.9500437158469945",
    "promo_oos": "0.1489161661008096"
  },
  {
    "Region": "Southeast",
    "est_lost_sales": "1330507.9264000005",
    "net_sales": "9.776949734001502E7",
    "osa": "0.9524959016393443",
    "promo_oos": "0.15152319731693684"
  },
  {
    "Region": "West",
    "est_lost_sales": "1069872.4538000037",
    "net_sales": "7.467224826001664E7",
    "osa": "0.9502513661202185",
    "promo_oos": "0.14401165871754107"
  }
]
```

## 7 · Grants to the app service principal (least privilege)

```sql
SHOW GRANTS `5b2c13f0-0134-4430-8465-0317907aff13` ON CATALOG sabir_freshmart
```

```json
[
  {
    "ActionType": "USE CATALOG",
    "ObjectKey": "sabir_freshmart",
    "ObjectType": "CATALOG",
    "Principal": "5b2c13f0-0134-4430-8465-0317907aff13"
  }
]
```

```sql
SHOW GRANTS ON SCHEMA sabir_freshmart.serving
```

```json
[
  {
    "ActionType": "SELECT",
    "ObjectKey": "sabir_freshmart.serving",
    "ObjectType": "SCHEMA",
    "Principal": "5b2c13f0-0134-4430-8465-0317907aff13"
  },
  {
    "ActionType": "USE SCHEMA",
    "ObjectKey": "sabir_freshmart.serving",
    "ObjectType": "SCHEMA",
    "Principal": "5b2c13f0-0134-4430-8465-0317907aff13"
  },
  {
    "ActionType": "READ METADATA",
    "ObjectKey": "unity-catalog-demo",
    "ObjectType": "METASTORE",
    "Principal": "leon.pater@databricks.com"
  },
  {
    "ActionType": "READ METADATA",
    "ObjectKey": "unity-catalog-demo",
    "ObjectType": "METASTORE",
    "Principal": "prakjit.jena@databricks.com"
  },
  {
    "ActionType": "ALL PRIVILEGES",
    "ObjectKey": "sabir_freshmart",
    "ObjectType": "CATALOG",
    "Principal": "57e7dc57-63b9-4114-a2ad-0a6226794a22"
  },
  {
    "ActionType": "MANAGE",
    "ObjectKey": "sabir_freshmart",
    "ObjectType": "CATALOG",
    "Principal": "57e7dc57-63b9-4114-a2ad-0a6226794a22"
  }
]
```
