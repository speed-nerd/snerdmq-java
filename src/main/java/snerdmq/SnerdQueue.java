package snerdmq;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import io.javalin.Javalin;
import io.javalin.websocket.WsContext;


public class SnerdQueue {
    private String binaryPath;
    private String storagePath;
    private Integer shards;
    private Integer maxLocalShards;
    private Integer maxWorkers;
    private final Map<String, Consumer<String>> handlers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> maxRetryHandlers = new ConcurrentHashMap<>();
    private static final ThreadLocal<String> currentTaskId = new ThreadLocal<>();
    private final Set<WsContext> wsClients = ConcurrentHashMap.newKeySet();
    private volatile List<String> ownedShards = new ArrayList<>();


    private Process process;
    private BufferedWriter writer;
    private BufferedReader reader;

    private ExecutorService stdoutReaderPool;
    private ExecutorService jobExecutionPool;
    private volatile boolean isShuttingDown = false;
    private final Map<String, CompletableFuture<Void>> pendingEnqueues = new ConcurrentHashMap<>();

    public SnerdQueue() throws IOException, InterruptedException {
        this(null, null);
    }

    public SnerdQueue(String binaryPath) throws IOException, InterruptedException {
        this(binaryPath, null);
    }

    public SnerdQueue(String binaryPath, String storagePath) throws IOException, InterruptedException {
        this(binaryPath, storagePath, null, null, null);
    }

    public SnerdQueue(String binaryPath, String storagePath, Integer shards, Integer maxLocalShards, Integer maxWorkers) throws IOException, InterruptedException {
        this.binaryPath = binaryPath;
        this.storagePath = storagePath;
        this.shards = shards;
        this.maxLocalShards = maxLocalShards;
        this.maxWorkers = maxWorkers;

        if (this.binaryPath == null) {
            this.binaryPath = SnerdmqInstaller.ensureDownloaded();
        }

        if (this.binaryPath == null) {
            throw new RuntimeException("[Snerd] Binary path cannot be null. Installer failed or path not provided.");
        }
    }

    public void registerHandler(String taskType, Consumer<String> callback) {
        handlers.put(taskType, callback);
        if (process != null && process.isAlive() && !isShuttingDown) {
            sendMessage(String.format("{\"action\":\"register\",\"task_type\":\"%s\"}", taskType));
        }
    }

    public void registerMaxRetryHandler(String taskType, Consumer<String> callback) {
        maxRetryHandlers.put(taskType, callback);
    }

    public void startListening() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(this.binaryPath);
        if (this.storagePath != null) {
            cmd.add(this.storagePath);
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // Pass sharding options as environment variables.
        Map<String, String> env = pb.environment();
        if (shards != null)         env.put("SNERD_SHARDS",       String.valueOf(shards));
        if (maxLocalShards != null) env.put("SNERD_MAX_SHARDS",   String.valueOf(maxLocalShards));
        if (maxWorkers != null)     env.put("SNERD_MAX_WORKERS",  String.valueOf(maxWorkers));
        this.process = pb.start();

        this.writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        this.reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        // Background thread to constantly read standard output from Rust
        this.stdoutReaderPool = Executors.newSingleThreadExecutor();
        // Background thread pool to execute the user's jobs concurrently
        this.jobExecutionPool = Executors.newCachedThreadPool();

        this.stdoutReaderPool.submit(() -> {
            try {
                String line;
                // Keep reading even while shutting down — drain cooperation
                // requires us to keep processing execute responses until the
                // daemon closes its stdout pipe.
                while ((line = reader.readLine()) != null) {
                    handleLine(line.trim());
                }
            } catch (IOException e) {
                if (!isShuttingDown) e.printStackTrace();
            } finally {
                // Engine is gone — it can never ack. Fail all pending enqueues
                // so callers fail fast instead of blocking on get()/join() forever.
                for (Map.Entry<String, CompletableFuture<Void>> entry : pendingEnqueues.entrySet()) {
                    entry.getValue().completeExceptionally(new RuntimeException(
                        "[Snerd] Engine terminated before ack for task '" + entry.getKey() + "'"));
                }
                pendingEnqueues.clear();
            }
        });

        // Re-register existing handlers with the newly spawned daemon
        for (String taskType : handlers.keySet()) {
            sendMessage(String.format("{\"action\":\"register\",\"task_type\":\"%s\"}", taskType));
        }
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, null, null, null, null, null, null, null, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, null, null, null, null, null, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, null, null, null, null, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe, Double urgencyScore) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, urgencyScore, null, null, null, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe, Double urgencyScore, String executeAt, String cron, String webhookUrl) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, urgencyScore, executeAt, cron, webhookUrl, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe, Double urgencyScore, String executeAt, String cron, String webhookUrl, Integer maxExecutionSeconds) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, urgencyScore, executeAt, cron, webhookUrl, maxExecutionSeconds, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe, Double urgencyScore, String executeAt, String cron, String webhookUrl, Integer maxExecutionSeconds, String pool) {
        if (process == null || !process.isAlive() || isShuttingDown) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("[Snerd] Cannot enqueue task: Queue is not running."));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingEnqueues.put(taskId, future);

        // We escape the inner JSON string safely
        String escapedJson = jsonData.replace("\"", "\\\"");
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append(String.format(
            Locale.US,
            "{\"action\":\"enqueue\",\"task_id\":\"%s\",\"task_type\":\"%s\",\"task_data\":\"%s\",\"max_retries\":%d,\"retry_after_hours\":%.2f",
            taskId, taskType, escapedJson, maxRetries, retryAfterHours
        ));
        
        if (rateLimitGroup != null) {
            jsonBuilder.append(String.format(",\"rate_limit_group\":\"%s\"", rateLimitGroup));
        }
        if (maxPerMinute != null) {
            jsonBuilder.append(String.format(",\"max_per_minute\":%d", maxPerMinute));
        }
        
        if (autoDedupe != null) { jsonBuilder.append(String.format(",\"auto_dedupe\":%b", autoDedupe)); }
        if (urgencyScore != null) { jsonBuilder.append(String.format(java.util.Locale.US, ",\"urgency_score\":%.2f", urgencyScore)); }
        if (executeAt != null) { jsonBuilder.append(String.format(",\"execute_at\":\"%s\"", executeAt)); }
        if (cron != null) { jsonBuilder.append(String.format(",\"cron\":\"%s\"", cron)); }
        if (webhookUrl != null) { jsonBuilder.append(String.format(",\"webhook_url\":\"%s\"", webhookUrl)); }
        if (maxExecutionSeconds != null) { jsonBuilder.append(String.format(",\"max_execution_seconds\":%d", maxExecutionSeconds)); }
        if (pool != null) { jsonBuilder.append(String.format(",\"pool\":\"%s\"", pool)); }
        jsonBuilder.append("}");
        
        sendMessage(jsonBuilder.toString());
        return future;
    }

    public void shutdown() {
        this.isShuttingDown = true;

        if (process != null && process.isAlive()) {
            // SIGTERM triggers the daemon's graceful drain sequence.
            process.destroy();
        }

        // Wait up to 35s for the daemon to exit after drain completes.
        if (process != null) {
            try {
                boolean exited = process.waitFor(35, java.util.concurrent.TimeUnit.SECONDS);
                if (!exited) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }

        if (stdoutReaderPool != null) stdoutReaderPool.shutdown();
        if (jobExecutionPool != null) jobExecutionPool.shutdown();
    }

    /** Returns the shard keys owned by this instance (from the last membership event). */
    public List<String> getOwnedShards() {
        return Collections.unmodifiableList(ownedShards);
    }

    private synchronized void sendMessage(String json) {
        if (isShuttingDown || writer == null) return;
        if (process != null && !process.isAlive()) return;
        try {
            writer.write(json);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            // Daemon died, pipe broken — silently ignore
        }
    }

    private void handleLine(String line) {
        if (line.isEmpty()) return;

        // Note: For a zero-dependency Java library, we use simple RegEx to parse the JSON-RPC.
        // In a massive framework, users would provide Jackson, but we don't want to force Jackson in the SDK jar.
        
        String action = extractJsonField(line, "action");
        if (action == null) return;

        if (action.equals("execute")) {
            String taskId = extractJsonField(line, "task_id");
            String taskType = extractJsonField(line, "task_type");
            String taskData = extractJsonField(line, "task_data");
            String maxExecutionStr = extractJsonNumberField(line, "max_execution_seconds");
            Integer maxExecutionSeconds = maxExecutionStr != null ? Integer.parseInt(maxExecutionStr) : null;

            if (taskId == null || taskType == null) return;

            Consumer<String> handler = handlers.get(taskType);
            
            if (handler == null) {
                sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"No handler registered\"}", taskId));
                return;
            }

            // Execute on the cached thread pool so we don't block the stdout reader!
            CompletableFuture<Void> executionFuture = CompletableFuture.runAsync(() -> {
                currentTaskId.set(taskId);
                String unescapedData = taskData != null ? taskData.replace("\\\"", "\"").replace("\\\\", "\\") : "";
                handler.accept(unescapedData);
            }, jobExecutionPool);
            
            if (maxExecutionSeconds != null) {
                executionFuture = executionFuture.orTimeout(maxExecutionSeconds, java.util.concurrent.TimeUnit.SECONDS);
            }
            
            executionFuture.whenComplete((res, err) -> {
                if (err != null) {
                    if (err.getCause() instanceof java.util.concurrent.TimeoutException || err instanceof java.util.concurrent.TimeoutException) {
                        sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"Task execution timed out after %d seconds\"}", taskId, maxExecutionSeconds));
                    } else {
                        String errorMsg = err.getMessage() != null ? err.getMessage().replace("\"", "'") : "Unknown Exception";
                        sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"%s\"}", taskId, errorMsg));
                    }
                } else {
                    sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"success\"}", taskId));
                }
            });
            
        } else if (action.equals("ack")) {
            String taskId = extractJsonField(line, "task_id");
            if (taskId != null) {
                CompletableFuture<Void> future = pendingEnqueues.remove(taskId);
                if (future != null) future.complete(null);
            }
        } else if (action.equals("error")) {
            String taskId = extractJsonField(line, "task_id");
            String message = extractJsonField(line, "message");
            if (taskId != null) {
                CompletableFuture<Void> future = pendingEnqueues.remove(taskId);
                if (future != null) future.completeExceptionally(new RuntimeException(message));
            } else {
                System.err.println("[Snerd] Error from engine: " + message);
            }
        } else if (action.equals("membership")) {
            // Informational only — daemon owns all routing.
            // Parse the "owned" array from the raw JSON line.
            List<String> owned = new ArrayList<>();
            java.util.regex.Matcher m = Pattern.compile("\"([^\"]+)\"").matcher(
                line.replaceAll(".*\"owned\"\\s*:\\s*\\[", "").replaceAll("\\].*", ""));
            while (m.find()) owned.add(m.group(1));
            this.ownedShards = owned;
            System.err.println("[Snerd] Cluster: queue=" + extractJsonField(line, "queue")
                + " shards=" + extractJsonNumberField(line, "shards")
                + " owned=" + owned
                + " version=" + extractJsonNumberField(line, "version"));
        } else if (action.equals("progress")) {
            for (WsContext ctx : wsClients) {
                if (ctx.session.isOpen()) {
                    ctx.send(line); // Forward the raw JSON string
                }
            }
        } else if (action.equals("max_retries_reached")) {
            String taskId = extractJsonField(line, "task_id");
            String taskType = extractJsonField(line, "task_type");
            
            Consumer<String> handler = maxRetryHandlers.get(taskType);
            if (handler != null && taskId != null) {
                String taskData = extractJsonField(line, "task_data");
                jobExecutionPool.submit(() -> {
                    try {
                        currentTaskId.set(taskId);
                        String unescapedData = taskData != null ? taskData.replace("\\\"", "\"").replace("\\\\", "\\") : "";
                        handler.accept(unescapedData);
                    } catch (Exception e) {
                        System.err.println("[Snerd] Error in max retry handler for task " + taskId + ": " + e.getMessage());
                    }
                });
            } else {
                System.out.println("[Snerd] Dead Letter Queue: Task " + taskId + " (" + taskType + ") permanently failed.");
            }
        }
    }

    // A lightweight helper to extract flat string fields from a JSON payload
    private String extractJsonField(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + key + "\"\\s*:\\s*\"(.*?)(?<!\\\\)\"");
        Matcher matcher = pattern.matcher(json);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String extractJsonNumberField(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + key + "\"\\s*:\\s*([0-9.]+)");
        Matcher matcher = pattern.matcher(json);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public void yieldProgress(String data) {
        String taskId = currentTaskId.get();
        if (taskId == null) {
            throw new RuntimeException("[Snerd] yieldProgress must be called within a task handler context.");
        }
        
        String escapedData = data != null ? data.replace("\"", "\\\"") : "";
        String msg = String.format("{\"action\":\"progress\",\"task_id\":\"%s\",\"data\":\"%s\"}", taskId, escapedData);
        sendMessage(msg);
    }

    public void startDashboard(int port) {
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> {
                cors.add(it -> it.anyHost());
            });
        }).start(port);

        app.get("/", ctx -> {
            Path htmlPath = Paths.get(System.getProperty("user.dir"), "static", "index.html");
            if (Files.exists(htmlPath)) {
                ctx.contentType("text/html");
                ctx.result(Files.readString(htmlPath));
            } else {
                ctx.status(404).result("Dashboard UI not found in static folder.");
            }
        });

        app.get("/api/stats", ctx -> {
            int enqueued = 0, processed = 0, failed = 0;
            Path tasksPath = Paths.get(this.storagePath != null ? this.storagePath : "./.snerdata", "tasks", "tasks.log");
            Map<String, String> tasksMap = new java.util.LinkedHashMap<>();
            if (Files.exists(tasksPath)) {
                try {
                    for (String line : Files.readAllLines(tasksPath)) {
                        if (line.trim().isEmpty()) continue;
                        String tId = extractJsonField(line, "taskId");
                        if (tId != null) tasksMap.put(tId, line);
                    }
                } catch (Exception e) {}
            }
            for (String t : tasksMap.values()) {
                enqueued++;
                if (t.contains("\"deletedAt\":")) {
                    if (t.contains("\"LastJobError\":")) {
                        failed++;
                    } else {
                        processed++;
                    }
                }
            }
            String result = String.format("{\"enqueued\":%d,\"processed\":%d,\"failed\":%d}", enqueued, processed, failed);
            ctx.contentType("application/json").result(result);
        });

        app.get("/api/tasks", ctx -> {
            Map<String, String> tasksMap = new java.util.LinkedHashMap<>();
            Path tasksPath = Paths.get(this.storagePath != null ? this.storagePath : "./.snerdata", "tasks", "tasks.log");
            if (Files.exists(tasksPath)) {
                try {
                    for (String line : Files.readAllLines(tasksPath)) {
                        if (line.trim().isEmpty()) continue;
                        String tId = extractJsonField(line, "taskId");
                        if (tId != null) tasksMap.put(tId, line);
                    }
                } catch (Exception e) {}
            }

            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String t : tasksMap.values()) {
                String tId = extractJsonField(t, "taskId");
                String tType = extractJsonField(t, "taskType");
                String status;
                if (t.contains("\"deletedAt\":")) {
                    String rCountStr = extractJsonNumberField(t, "retryCount");
                    String mRetriesStr = extractJsonNumberField(t, "maxRetries");
                    int rCountVal = rCountStr != null ? (int) Double.parseDouble(rCountStr) : 0;
                    int mRetriesVal = mRetriesStr != null ? (int) Double.parseDouble(mRetriesStr) : 3;
                    if (t.contains("\"LastJobError\":") && rCountVal >= mRetriesVal) {
                        status = "dead_letter";
                    } else if (t.contains("\"LastJobError\":")) {
                        status = "failed";
                    } else {
                        status = "completed";
                    }
                } else if (t.contains("\"LastJobError\":")) {
                    status = "failed";
                } else {
                    String execAt = extractJsonField(t, "executeAt");
                    if (execAt != null) {
                        try {
                            java.time.Instant execTime = java.time.Instant.parse(execAt);
                            status = execTime.isBefore(java.time.Instant.now()) ? "active" : "queued";
                        } catch (Exception e) {
                            status = "queued";
                        }
                    } else {
                        status = "queued";
                    }
                }
                
                String rCount = extractJsonNumberField(t, "retryCount");
                String mRetries = extractJsonNumberField(t, "maxRetries");
                String rAfter = extractJsonField(t, "retryAfterTime");
                String cronExpr = extractJsonField(t, "cronExpression");
                String webhookUrl = extractJsonField(t, "webhookUrl");
                String maxExecSecs = extractJsonNumberField(t, "maxExecutionSeconds");
                
                if (!first) sb.append(",");
                sb.append(String.format("{\"id\":\"%s\",\"type\":\"%s\",\"status\":\"%s\",\"progress\":0", tId, tType, status));
                if (rCount != null) sb.append(",\"retryCount\":").append(rCount);
                if (mRetries != null) sb.append(",\"maxRetries\":").append(mRetries);
                if (rAfter != null) sb.append(",\"retryAfterTime\":\"").append(rAfter).append("\"");
                if (cronExpr != null) sb.append(",\"cronExpression\":\"").append(cronExpr).append("\"");
                if (webhookUrl != null) sb.append(",\"webhookUrl\":\"").append(webhookUrl).append("\"");
                if (maxExecSecs != null) sb.append(",\"maxExecutionSeconds\":").append(maxExecSecs);
                sb.append("}");
                first = false;
            }
            sb.append("]");
            ctx.contentType("application/json").result(sb.toString());
        });

        app.ws("/ws", ws -> {
            ws.onConnect(ctx -> wsClients.add(ctx));
            ws.onClose(ctx -> wsClients.remove(ctx));
            ws.onError(ctx -> wsClients.remove(ctx));
        });

        System.out.println("[Snerd] Dashboard running on http://localhost:" + port);
    }

}
