<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Java Logo" />
  <h1>☕ SnerdMQ Java & Kotlin SDK v1.0.5</h1>
  <p>A zero-config, C-speed background job queue for the JVM. Ditch Redis and heavy queue workers for a simple, embedded Rust daemon.</p>

  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

This is the official JVM SDK wrapper for **SnerdMQ**. It handles all JSON-RPC communication and `ProcessBuilder` orchestration so you can write lightning-fast background jobs in Java, Kotlin, or Scala without managing any external databases like Redis or ActiveMQ.

## ✨ v1.0.5 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.
- **Ditch Redis**: Gives your Spring Boot or Ktor apps persistent state, automatic retries, and dead-letter queues right out of the box with zero external infrastructure.
- **Zero Rust Required**: Our built-in `SnerdmqInstaller` class automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Thread-Safe**: Built on top of native Java `ExecutorService` and `ProcessBuilder`, it is heavily optimized for massively concurrent enterprise workloads.

### ⚙️ Advanced Task Configuration (v1.0.5)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`autoDedupe` (`Boolean`)**: If set to `true`, the daemon computes a cryptographic hash of the `taskType` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`Double`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`String`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`Integer`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`String` | `java.time.Instant`)**: A timestamp of when the job should be executed in the future.
* **`retryAfterHours` (`double`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`cron` (`String`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhookUrl` (`String`)**: By providing a webhook URL, SnerdMQ will completely bypass your local Java handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.
* **`maxExecutionSeconds` (`Integer`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed.

### Note on Hard Timeouts (`maxExecutionSeconds`)
When `maxExecutionSeconds` is provided, the Java SDK executes your handler using `CompletableFuture.orTimeout()`. If the task takes longer than the timeout, a `TimeoutException` is caught and the execution will be marked as failed. The background Rust daemon also enforces this timeout at the IPC level.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhookUrl`, the internal background processor will skip any registered handlers (`queue.registerHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhookUrl` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfterHours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 📦 Installation

This package is designed to work flawlessly in both modern Gradle projects and legacy Maven projects!

### Option A: Gradle (Modern)
Add the dependency to your `build.gradle`:
```groovy
dependencies {
    implementation 'io.github.speed-nerd:snerdmq:1.0.5'
}
```

### Option B: Maven (Enterprise)
Add the dependency to your `pom.xml`:
```xml
<dependency>
    <groupId>io.github.speed-nerd</groupId>
    <artifactId>snerdmq</artifactId>
    <version>1.0.5</version>
</dependency>
```

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your Consumer callbacks, and start listening!

```java
import snerdmq.SnerdQueue;
import snerdmq.SnerdmqInstaller;

public class App {
    public static void main(String[] args) throws Exception {
        // 1. (Optional) Download the Rust daemon to the user's ~/.snerdmq folder
        SnerdmqInstaller.ensureDownloaded();

        // 2. Initialize the daemon in the background
        SnerdQueue queue = new SnerdQueue();

        // 3. Register your background job logic
        queue.registerHandler("send_email", (jsonData) -> {
            System.out.println("Executing background job with payload: " + jsonData);
            // Throw a RuntimeException here to automatically trigger SnerdMQ's retry logic!
        });

        // 4. Start the async Listener threads
        queue.startListening();
        System.out.println("SnerdMQ Java SDK is listening for jobs...");

        // 5. Enqueue a job from anywhere in your codebase
        queue.enqueue(
            "email-123",
            "send_email",
            "{\"to\":\"james.gosling@java.com\",\"subject\":\"SnerdMQ Update\"}",
            3,              // max retries
            0.5,            // retry after hours (wait 30 minutes before retrying)
            "email_api",    // rateLimitGroup
            100,            // maxPerMinute
            null,           // autoDedupe
            null,           // urgencyScore
            null,           // executeAt
            null,           // cron
            null,           // webhookUrl
            null            // maxExecutionSeconds
        );

        // 6. Need scheduling, deduplication, or serverless execution? All
        // orchestration options are opt-in — combine only what you need:
        queue.enqueue(
            "email-digest-1",
            "send_email",
            "{\"to\":\"james.gosling@java.com\",\"subject\":\"Daily Digest\"}",
            3,
            0.0,
            null,           // no rate limit group
            null,           // no maxPerMinute cap
            true,           // autoDedupe: drop identical pending payloads
            0.99,           // urgencyScore: float to the front of the queue
            null,
            "0 8 * * *",    // cron: run every day at 08:00
            "https://api.example.com/webhook", // Execute via HTTP instead of local handlers
            300             // maxExecutionSeconds: hard timeout
        );
        
        // Let the application run
        Thread.sleep(Long.MAX_VALUE);
    }
}
```

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `maxRetries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

> **Delivery semantics:** SnerdMQ provides **at-least-once** delivery. In rare cases — e.g. if the daemon is killed while a task is executing — a task may be executed again after restart. Make your handlers idempotent.

```java
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
queue.registerMaxRetryHandler("send_email", data -> {
    System.out.println("Email task failed after all retries! Data: " + data);
});
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK — no extra services or ports to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `yieldProgress` calls in your handlers

```java
SnerdQueue queue = new SnerdQueue();

// Start the built-in dashboard on http://localhost:9090
queue.startDashboard(9090);

// ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. Updates are pushed to the page over WebSocket the moment jobs change state, and the dashboard also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top.

> **Note:** The dashboard serves its `static/` assets relative to your process's working directory, so run your application from the directory that contains the `static/` folder. `startDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```java
queue.registerHandler("generate_report", (jsonData) -> {
    for (int step = 1; step <= 10; step++) {
        doWork(step);
        queue.yieldProgress("Step " + step + "/10 complete");
    }
});
```

> `yieldProgress` must be called **inside a task handler** — the SDK tracks the currently executing task on each worker thread so each update lands on the right job in the dashboard.

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

Each `SnerdQueue` client spawns its own Rust daemon and **exclusively owns** its storage directory (`.snerdata` by default). The recommended pattern is **one client per application process**: register every job type on it and serve a single shared dashboard:

```java
import snerdmq.SnerdQueue;
import snerdmq.SnerdmqInstaller;

public class App {
    public static void main(String[] args) throws Exception {
        SnerdmqInstaller.ensureDownloaded();

        // ONE queue client for the whole app
        SnerdQueue queue = new SnerdQueue();

        // Job type #1: image processing
        queue.registerHandler("process_image", (jsonData) -> {
            System.out.println("Processing image: " + jsonData);
        });

        // Job type #2: OTP emails — same queue, same daemon
        queue.registerHandler("send_otp_email", (jsonData) -> {
            System.out.println("Sending OTP: " + jsonData);
        });

        queue.startListening();

        // Both job types flow through the exact same queue
        queue.enqueue("img-1", "process_image", "{\"image_id\":\"abc123\"}", 3, 0.5);
        queue.enqueue("otp-1", "send_otp_email", "{\"to\":\"john@wick.com\"}", 3, 0.5);

        // ONE dashboard shows every job type
        queue.startDashboard(8080);
    }
}
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:8080` showing all of them.

### 🚫 Same storage twice = fails fast

The daemon takes an **exclusive OS-level lock** on its storage directory at startup. A second client on the same storage fails instead of silently double-executing your jobs:

```java
SnerdQueue first = new SnerdQueue();  // ✅ owns .snerdata
SnerdQueue second = new SnerdQueue(); // ❌ daemon refuses to start:
// "Another daemon is already running on storage '.snerdata'"
```

This applies across processes too — multiple JVM services on the same machine each spawn their own daemon, so each needs its own `storagePath`.

### 🔀 Need multiple queues? Give each one its own storage

```java
SnerdQueue images = new SnerdQueue(null, ".snerdata-images");
SnerdQueue emails = new SnerdQueue(null, ".snerdata-emails");

images.startDashboard(8080); // separate dashboards, so separate ports
emails.startDashboard(8081);
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different teams, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

Because the daemon exclusively locks its storage directory, scaling horizontally means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the jobs it enqueued:

```java
// Each server runs its own daemon on its own storage dir (local disk works fine)
SnerdQueue queue = new SnerdQueue(null, "/var/data/snerd"); // per-server storage
```

A shared network drive (AWS EFS or NFS) is still a good home for that storage when a single instance needs durable state — e.g. a container that restarts but must keep its queue. Native OS file locking (`flock`) keeps writes safe — no Redis required.

*Built with ❤️ for John Wick tier engineering.*
