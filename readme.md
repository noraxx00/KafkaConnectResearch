# Kafka Connect Architecture

General reference for how Kafka Connect is structured and how it behaves.

Kafka Connect 是什麼？   
Kafka Connect 是 Apache Kafka 旗下的開源框架，主要`用於在 Kafka 與外部系統（如資料庫、Redis、應用程式等）之間`高效移動資料。

  - 配置驅動 (Configuration-driven)：透過`可重複使用的連接器 (Connectors)，只需進行設定`即可完成資料整合。
  - 無需自訂程式碼 (No custom code)：針對常見的系統整合，工程師不需要動手撰寫傳統的生產者 (Producer) 或消費者 (Consumer) 程式碼。
  - 只需要啟動一台帶有相同 `group.id` 的新伺服器（Worker），它會利用 Kafka 內部的 Group Coordinator 機制自動向叢集報到。由框架協調Leader 選舉、分派 Task 的任務。
  - 完全自動化的生命週期，不需撰寫優雅停機`(check details, check large thruput)`

#### Kafka Connect Graceful Shutdown Sequence

```text
  [OS Signals SIGTERM] ──► [Worker pauses Consumer Polls]
                                      │
                                      ▼
                   [Tasks finish processing current batch]
                                      │
                                      ▼
                  [Synchronous DB Write Completes Success]
                                      │
                                      ▼
                  [Final safe Offset Commit to Kafka Broker]
                                      │
                                      ▼
                   [JVM Process terminates cleanly (0 OOM)]
```


  - `Error Handling` : 在connector json設定中加入DLQ設定，異常/損壞資料會自動被分配到DLQ Topic


Kafka Connect 是在『基礎設施層層級』控制流量。它幫我們把資料庫連線鎖死、提供天然的backpressure 保護、實現快速與慢速資料庫的解耦，並透過設定檔直接提供生產級的故障容錯（DLQ），讓我們能把 Golang 開發精力 100% 集中在核心邏輯的撰寫上。   
  

## 核心組件
```text
┌───────────────┐       ┌─────────────────────────────────────────────────────────┐
│               │       │                      KAFKA CONNECT                      │
│               │       │                                                         │
│ Source system │ ────> │ ┌──────────────────┐   ┌─────────────────┐   ┌─────────┐ │       ┌───────────────┐
│               │       │ │ Source connector │ ─>│ Transformations │ ─>│Converter│ │ ────> │               │
└───────────────┘       │ └──────────────────┘   └─────────────────┘   └─────────┘ │       │               │
                        └─────────────────────────────────────────────────────────┘       │               │
                                                                                          │ Kafka cluster │
┌───────────────┐       ┌─────────────────────────────────────────────────────────┐       │               │
│               │       │                      KAFKA CONNECT                      │       │               │
│               │       │                                                         │       │               │
│  Sink system  │ <──── │ ┌──────────────────┐   ┌─────────────────┐   ┌─────────┐ │ <──── └───────────────┘
│               │       │ │  Sink connector  │ <─│ Transformations │ <─│Converter│ │
└───────────────┘       │ └──────────────────┘   └─────────────────┘   └─────────┘ │
                        └─────────────────────────────────────────────────────────┘
```
### Workers (JVM Process)

  - Worker是一個跑connectors的JVM Process，可以是Linux Server/ Container/ Pod
  - 一個Worker就是一個Kafka Connect，執行實際的任務分配、負載平衡
  - 分散式 (Kafka Connect Cluster):
  - 利用相同 group.id 及 internal config/offset/status topics 配置，Connect可以用Kafka內部Group Coodinator自動發現彼此並建立Cluster
  - 當前Worker不足以應付流量時，啟動相同配置的新Worker即可。Worker可加入/離開，Connect會重新平衡當前Worker Cluster 的Task

### Connectors (宣告)

  - 利用JSON config檔案定義Connecter配置，並通過REST API POST /connectors 註冊到Connect
  - Connector只是配置藍圖，利用API 註冊到connect之後由connect根據註冊的配置檔執行實際動作
  - `Source connectors` poll外部系統如 rabbitmq, db, AWS S3, Saas，將數據產生到Kafka
  - `Sink connectors` consume Kafka Topics -> 寫入外部系統 (e.g. 關聯式DB JDBC sink, Redis sink)

### Tasks (執行緒)

  - Worker process內部的CPU Thread，執行實際的資料搬運
  - 在Connector json config 中 tasks.max宣告配置
  - 在Sink Connector, 根據Topic的分區(Partition)數量，task各自被賦予處理一定數量的partition. 不需要將tasks.max設定多於分區數量，多餘的tasks沒事做造成浪費
  - 可以利用API設定tasks.max配置
  - tasks.max配置的改變與Worker加入/離開會引起 Rebalances ，造成Task的重新分配，並短暫暫停資料搬運，約2~10秒。(Apache Kafka 2.2 and older)
  - Apache Kafka 2.4 and 2.5+ 的較新版本同樣會進行 Rebalances，並產生局部 freeze(約500ms~2s)，但只影響被計算要重新分配task的分區(partition)`(確認監控指標)`

### Converters

  - 轉換Kafka raw bytes及Connect內部紀錄格式: JsonConverter, StringConverter, AvroConverter (requires a Schema Registry), etc.
  - Key/ Value可以分別使用不同converter(e.g. key使用StringConverter , value 使用JsonConverter)
  - schemas.enable (JSON converter only) toggles whether the JSON payload carries an explicit {"schema":..., "payload":...} envelope. Sink connectors that need typed columns (e.g. a JDBC sink building CREATE/INSERT statements) generally require schemas.enable: true, since a schemaless JSON map has no declared field types to build a table from.

### Transforms (SMTs)

  - 單一訊息轉換(Single Message Transforms) 根據connector json設置，一次轉換一筆資料，為無狀態，不記憶任筆資料，無法對多筆資料進行組合操作(字串組合/運算)。
  - 設置的操作包含欄位重命名/ 過濾/ 遮罩, 路由, 型別轉換，參考: <https://docs.confluent.io/kafka-connectors/transforms/current/overview.html#kconnect-long-single-message-transformations-reference-for-product>
  - 跨訊息加總：必須依靠 Kafka Streams/ ksqlDB(Kafka生態系)或 Dataflow(GCP生態系)才能實現。

## 機制細節

### Topic-to-target 映射

  - A sink connector's topics config accepts a comma-separated list — one connector can consume multiple topics and route each to an independent target (e.g. separate tables), rather than needing one connector per topic.
  - For the JDBC sink, table.name.format controls the target table name and defaults to ${topic} (table name = topic name), but is fully customizable per connector.

### Batching觸發

Whether data flows in near-real-time or in bursts comes down to whichever of two independent thresholds fires first:

  - Time-based — a linger/flush interval: send whatever has accumulated after N ms, even if the batch isn't full.
  - Size-based — a record-count or byte-size threshold: send once enough has accumulated, regardless of elapsed time.

These knobs exist at multiple layers — producer linger.ms / max batch bytes, the Connect worker's offset.flush.interval.ms, and sink-specific settings like the JDBC sink's batch.size (a row count, not a byte size). Tuning the wrong layer won't change the behavior you're trying to affect.

### Error Handling
  - `Kafka Connect Dead Letter Queue (DLQ) Error Isolation`
    - 處理JSON parse error

```text
                  ┌──────────────── [ Kafka Connect Task ] ────────────────┐
                  │                                                        │
                  │              ┌───► [ Successful Parse ] ──► Target DB  │
                  │              │                                         │
[ Kafka Topic ] ──┼─► [ JSON Converter ]                                   │
                  │              │                                         │
                  │              └───► [ Parsing Error ] ───► [ DLQ Topic ]│
                  └────────────────────────────────────────────────────────┘
```

## 擴展調校參數

  - `tasks.max` 單一連接器 (Connector) 的並行處理上限；若設定值超過Topic 的分割區 (Partition) 數量，則不會產生額外效果。
  - 分割區數量 (Partition count) 決定了 tasks.max 的最大有效值，並決定了Topic的總吞吐量如何分配到各個任務 (Task) 中（使用Hash根據Message Key/ Partition Key做分配，若Partition Key具有高基數且均勻分佈，則分配會大致平均，所以應該使用獨特值的key, 如 ID, UUID）。
  - `Worker數量` 應該擴展Worker而不只是調整tasks.max 。
    - 增加Worker 可以將現有的任務分散到更多個 JVM 或主機上，而不是全堆積在同一個節點上。  
    - AWS MSK Connect 可以根據 CPU 或消費者延遲 (Consumer-lag) 指標來自動調整 tasks.max；
    - 自維護的 Kubernetes 上，tasks.max 的變更通常會透過 GitOps (基礎設施即程式碼，IaC) 進行，以便在部署Pipeline中追蹤 JVM 記憶體佔用的變化。  
  - `拆分連接器 (Splitting connectors)` 在中小規模下，每個外部系統配置一個連接器就足夠了。但如果共用同一個系統的各個Topic中，有某個Topic的資料量或業務領域與其他Topic出現劇烈分歧（例如：高吞吐量Topic與低吞吐量Topic混合），就應該針對該系統拆分成多個Connectors，達成 **資源隔離** 與 **效能最佳化**。

##### 1. 拆分前：單一連接器架構（潛在瓶頸）

```text
[ 外部系統 / 資料庫 ]
  │
  ├── 核心業務 A (高吞吐量: 每秒萬筆) ──┐
  │                                   │
  └── 邊緣業務 B (低吞吐量: 每分百筆) ──┴──> [ 統一連接器: MySystemConnector ]
                                               │ (共享 Workers 資源與線程)
                                               ├── Task 1 ──> Kafka: topic.heavy-traffic-A
                                               └── Task 2 ──> Kafka: topic.light-traffic-B 
                                                   ^^^^^^
                                            (容易因 Task 1 阻塞而導致延遲)
```

#### 2. 拆分後：多連接器架構（資源隔離）

拆分後，高、低吞吐量的業務擁有各自獨立的連接器生命週期與 Task 資源池。即使高吞吐量連接器滿載，也不會影響到低吞吐量連接器的運行。

```text
[ 外部系統 / 資料庫 ]
  │
  ├── 核心業務 A (高吞吐量) ──────────> [ 連接器 A: MySystem-Heavy-Connector ]
  │                                         │ (配置高 Task 數量，專注高併發)
  │                                         └── Task 1 & 2 ──> Kafka: topic.heavy-traffic-A
  │
  └── 邊緣業務 B (低吞吐量) ──────────> [ 連接器 B: MySystem-Light-Connector ]
                                            │ (配置低 Task 數量，確保即時回應)
                                            └── Task 1 ──────> Kafka: topic.light-traffic-B
```

#### 3. 拆分核心效益對比

| 評估面向 | 拆分前（單一連接器） | 拆分後（多連接器） |
| :--- | :--- | :--- |
| **資源隔離** | 相互搶奪執行線程與記憶體 | 獨立配置 Tasks 數量，互不干擾 |
| **故障影響** | 任何一個 Topic 異常可能導致整個 Connector 壞死 | 單一業務異常僅影響自身 Connector，其他維持正常 |
| **維護彈性** | 修改任一 Topic 規格皆需重啟整個系統的連接 | 可針對特定業務進行獨立升級、暫停或微調 |
| **監控警報** | 難以針對單一業務領域設置精準的 Lag 告警 | 可依據業務特性（高/低吞吐）分開監控性能指標 |


## 自動化擴展/ 擴展機制

  - 提供標準化架構讓Kubernetes HPA or AWS MSK Connect 等雲端管理工具做擴展
  - 利用監控工具如Prometheus / 雲端metric觸發K8s/ AWS MSK Connect自動擴展工具-> 開啟相同 group.id 的 Worker server/ pod (擴容)
  - (縮容) 低峰值時， 當延遲降為0，自動擴展工具逆向擴展流程，降低tasks.max並終止多餘Worker instances
  - 利用參數設置讓雲端 infra 處理擴展。
  - Task Scaling via GitOps/API: The deployment pipeline automatically updates the connector configuration to increase tasks.max (e.g., from 6 to 12).



#### Kafka Connect Distributed Architecture

```text
                  ┌─────────────── [ Kafka Connect Cluster ] ───────────────┐
                  │                                                         │
                  │  ┌──────────────────────┐    ┌──────────────────────┐   │
                  │  │   Worker Server 1    │    │   Worker Server 2    │   │
                  │  │                      │    │                      │   │
[ Kafka Cluster ] │  │  ┌────────────────┐  │    │  ┌────────────────┐  │   │
 (3 Brokers)     ─┼──┼─►│ MariaDB Task 0 │  │    │  │ MariaDB Task 1 │  │   │
                  │  │  └────────────────┘  │    │  └────────────────┘  │   │
                  │  │  ┌────────────────┐  │    │                      │   │
                  │  │  │  Redis Task 0  │  │    │                      │   │
                  │  │  └────────────────┘  │    │                      │   │
                  │  └──────────────────────┘    └──────────────────────┘   │
                  └─────────────────────────────────────────────────────────┘
```



## Bottlenecks to watch (generic)

  - Kafka broker network/disk I/O — amplified by replication factor (each write replicates to every in-sync replica).
  - Target sink write capacity — DB connection limits/write throughput, cache command throughput.
  - Data/schema mismatch between producer and sink — a Struct type mismatch fails the sink task rather than silently coercing.
  - Hot partitions from a low-cardinality or skewed partition key — no amount of tasks.max or worker count fixes uneven load across partitions.  

### MariaDB

  - 無共享連線池，tasks.max = 26 將會開啟數量最大到26個獨立連線。每個task會開啟個別的mariadb連線
  - future tweaking
  ```
  "batch.size": "5000",                               // 每次加大到 5000 筆batch SQL
  "consumer.override.max.poll.records": "5000",       // 讓 Kafka 一次拉足 5000 筆
  "consumer.override.fetch.min.bytes": "10485760",    // 強迫 Broker 積滿 10MB 資料才發送
  "consumer.override.linger.ms": "100"                // 給予 100ms 的緩衝時間來累積大 Batch
  ```
  - `max_allowed_packet`, package 超過上限，PacketTooBigException並拒絕執行->中斷連線
    - Task received PacketTooBigException, 根據max.retries重試，若重是持續失敗，這個 Task 執行緒就會直接掛掉(FAILED) 並停工

## Monitoring

  - JMX metrics (broker + Connect worker)
  - AKHQ — view topics, live-tail data, monitor consumer group lag
  - Kafbat UI

## Questions
  #### `Graceful Shutdown`
  #### tasks.max: Hash根據`Message Key/ Partition Key`做分配
  #### Apache Kafka 2.4 and 2.5+ 的較新版本同樣會進行 Rebalances，並產生局部 freeze(約500ms~2s)，但只影響被計算要重新分配task的分區(partition)`(確認監控指標)`
  #### 針對Worker擴展的問題
  - 只調整tasks.max，只增加了 JVM 內部的並發線程，無法突破單機的 CPU、記憶體、與網卡硬體瓶脅，且極易引發單機 OOM
  #### polling/batching
  - Kafka Connect Task Polling & Synchronous Batching Pipeline

```text
[ Connect Task Thread ]               [ Network Wire ]               [ MariaDB Database Engine ]
         │                                   │                                   │
         ├─── (1. KafkaConsumer.poll() )     │                                   │
         │     Pulls max 2,000 records into JVM                          │
         │                                   │                                   │
         ├─── (2. Executes SMTs / Transforms)│                                   │
         │                                   │                                   │
         ├─── (3. Generates 1 Multi-Row SQL) │                                   │
         │                                   │                                   │
         ├─── (4. Sends In-Flight Batch) ───►│                                   │
         │                                   │                                   │
         X [THREAD BLOCKED / PAUSED]         ├─── (5. Executes Transaction) ────►│ (Locks rows,
         X                                   │                               ◄───┤  writes to disk)
         X                                   │◄── (6. Returns SQL Success) ──────┤
         │                                   │                                   │
         ├─── (7. Commits Kafka Offsets)     │                                   │
         │     Updates __consumer_offsets    │                                   │
         │     bookmark to Broker            │                                   │
         │                                   │                                   │
         └─── (8. Loops back to Step 1: Poll)│                                   │
```


## See also

  - 文件參考RedPanda <https://www.redpanda.com/guides/kafka-cloud-kafka-connectors>  
    Docker設置參考: <https://docs.confluent.io/platform/current/installation/docker/config-reference.html#required-kconnect-long-configurations>
