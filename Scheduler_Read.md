# Flink on Kubernetes 调度流程与 Job 启动流程深度分析

> 基于 Apache Flink 源码的深度解析，涵盖集群启动、Job 提交、资源调度、Pod 管理全链路。

---

## 目录

- [1. 架构总览](#1-架构总览)
- [2. 核心组件介绍](#2-核心组件介绍)
- [3. 集群启动流程](#3-集群启动流程)
- [4. Job 提交与启动流程](#4-job-提交与启动流程)
- [5. 资源调度与 Slot 管理](#5-资源调度与-slot-管理)
- [6. Kubernetes Pod 生命周期管理](#6-kubernetes-pod-生命周期管理)
- [7. Pod 装饰器模式详解](#7-pod-装饰器模式详解)
- [8. 容错与恢复机制](#8-容错与恢复机制)
- [9. 关键源码索引](#9-关键源码索引)

---

## 1. 架构总览

### 1.1 整体架构图

```mermaid
graph TB
    subgraph "Client 端"
        CLI["Flink CLI / REST Client"]
    end

    subgraph "Kubernetes Cluster"
        subgraph "JobManager Pod"
            CE["ClusterEntrypoint<br/>(Session/Application)"]
            DRMC["DispatcherResourceManager<br/>ComponentFactory"]

            subgraph "核心服务"
                D["Dispatcher"]
                RM["ResourceManager"]
                JM["JobMaster"]
                DS["DefaultScheduler"]
            end

            subgraph "基础设施"
                RPC["RPC Service"]
                HA["HA Service"]
                BS["BlobServer"]
                HB["HeartBeat Service"]
                MR["Metric Registry"]
            end
        end

        subgraph "TaskManager Pod 1"
            TE1["TaskExecutor"]
            SM1["SlotTable"]
            TM1["Task 1..N"]
        end

        subgraph "TaskManager Pod 2"
            TE2["TaskExecutor"]
            SM2["SlotTable"]
            TM2["Task 1..N"]
        end

        subgraph "TaskManager Pod N"
            TEN["TaskExecutor"]
            SMN["SlotTable"]
            TMN["Task 1..N"]
        end

        K8S_API["Kubernetes API Server"]
    end

    CLI -->|"提交 Job"| D
    CE --> DRMC
    DRMC --> D
    DRMC --> RM
    D -->|"创建 JobMaster"| JM
    JM -->|"调度"| DS
    DS -->|"请求 Slot"| RM
    RM -->|"创建 Pod"| K8S_API
    K8S_API -->|"调度 Pod"| TE1
    K8S_API -->|"调度 Pod"| TE2
    K8S_API -->|"调度 Pod"| TEN
    TE1 -->|"注册 Slot"| RM
    TE2 -->|"注册 Slot"| RM
    TEN -->|"注册 Slot"| RM
    JM -->|"部署 Task"| TE1
    JM -->|"部署 Task"| TE2

    style CE fill:#e1f5fe,stroke:#01579b
    style D fill:#f3e5f5,stroke:#4a148c
    style RM fill:#e8f5e9,stroke:#1b5e20
    style JM fill:#fff3e0,stroke:#e65100
    style DS fill:#fce4ec,stroke:#880e4f
    style K8S_API fill:#f5f5f5,stroke:#212121
```

### 1.2 两种部署模式

```mermaid
graph LR
    subgraph "Session 模式"
        S_CE["KubernetesSession<br/>ClusterEntrypoint"]
        S_D["Dispatcher<br/>(长驻)"]
        S_JM1["JobMaster 1"]
        S_JM2["JobMaster 2"]
        S_JMN["JobMaster N"]

        S_CE --> S_D
        S_D --> S_JM1
        S_D --> S_JM2
        S_D --> S_JMN
    end

    subgraph "Application 模式"
        A_CE["KubernetesApplication<br/>ClusterEntrypoint"]
        A_D["Dispatcher<br/>(单 Job)"]
        A_JM["JobMaster"]

        A_CE -->|"加载用户 JAR"| A_D
        A_D --> A_JM
    end

    style S_CE fill:#e3f2fd,stroke:#1565c0
    style A_CE fill:#e8f5e9,stroke:#2e7d32
    style S_D fill:#f3e5f5,stroke:#6a1b9a
    style A_D fill:#f3e5f5,stroke:#6a1b9a
```

| 特性 | Session 模式 | Application 模式 |
|------|-------------|-----------------|
| 入口类 | `KubernetesSessionClusterEntrypoint` | `KubernetesApplicationClusterEntrypoint` |
| 生命周期 | 长驻运行，接受多 Job | 单 Job，完成后关闭 |
| Job 提交 | Client 远程提交 | 内部自动提交 |
| 适用场景 | 多 Job 共享集群 | 独立 Job 运行 |

---

## 2. 核心组件介绍

### 2.1 组件交互关系图

```mermaid
graph TB
    subgraph "作业管理层"
        Dispatcher["Dispatcher<br/>━━━━━━━━━━━━━━<br/>• 接收 Job 提交<br/>• 创建 JobManagerRunner<br/>• 管理 Job 生命周期"]
        JobMaster["JobMaster<br/>━━━━━━━━━━━━━━<br/>• 管理 ExecutionGraph<br/>• 协调 Task 部署<br/>• 管理 SlotPool"]
    end

    subgraph "调度层"
        Scheduler["DefaultScheduler<br/>━━━━━━━━━━━━━━<br/>• 调度策略执行<br/>• Slot 分配<br/>• Vertex 部署"]
        Strategy["SchedulingStrategy<br/>━━━━━━━━━━━━━━<br/>• 拓扑排序<br/>• 调度决策"]
        SlotAllocator["ExecutionSlot<br/>Allocator<br/>━━━━━━━━━━━━━━<br/>• Slot 分配请求<br/>• Slot 共享"]
        Deployer["ExecutionDeployer<br/>━━━━━━━━━━━━━━<br/>• 等待 Slot 就绪<br/>• 部署 Execution"]
    end

    subgraph "资源管理层"
        ResourceManager["ResourceManager<br/>━━━━━━━━━━━━━━<br/>• TaskManager 注册<br/>• 资源声明处理<br/>• Slot 管理委托"]
        SlotManager["FineGrained<br/>SlotManager<br/>━━━━━━━━━━━━━━<br/>• Slot 状态跟踪<br/>• 资源需求匹配<br/>• 分配策略"]
        K8sDriver["Kubernetes<br/>ResourceManager<br/>Driver<br/>━━━━━━━━━━━━━━<br/>• Pod 创建/删除<br/>• Pod 事件监听<br/>• 工作节点恢复"]
    end

    subgraph "Kubernetes 交互层"
        KubeClient["FlinkKubeClient<br/>(Fabric8)<br/>━━━━━━━━━━━━━━<br/>• K8s API 调用<br/>• Pod CRUD<br/>• Service 管理"]
        PodWatcher["KubernetesPods<br/>Watcher<br/>━━━━━━━━━━━━━━<br/>• Pod 事件监听<br/>• 状态变更回调"]
        TMFactory["KubernetesTask<br/>ManagerFactory<br/>━━━━━━━━━━━━━━<br/>• Pod Spec 构建<br/>• 装饰器链应用"]
    end

    Dispatcher -->|"创建"| JobMaster
    JobMaster -->|"委托调度"| Scheduler
    Scheduler --> Strategy
    Scheduler --> SlotAllocator
    Scheduler --> Deployer
    SlotAllocator -->|"请求 Slot"| ResourceManager
    ResourceManager -->|"管理 Slot"| SlotManager
    SlotManager -->|"请求资源"| K8sDriver
    K8sDriver -->|"构建 Pod"| TMFactory
    K8sDriver -->|"操作 K8s"| KubeClient
    K8sDriver -->|"监听事件"| PodWatcher
    KubeClient --> PodWatcher

    style Dispatcher fill:#e1bee7,stroke:#6a1b9a
    style JobMaster fill:#ffcc80,stroke:#e65100
    style Scheduler fill:#ef9a9a,stroke:#b71c1c
    style ResourceManager fill:#a5d6a7,stroke:#1b5e20
    style K8sDriver fill:#81d4fa,stroke:#01579b
    style KubeClient fill:#b0bec5,stroke:#263238
```

### 2.2 核心组件职责说明

| 组件 | 源码路径 | 核心职责 |
|------|---------|---------|
| **ClusterEntrypoint** | `flink-runtime/.../entrypoint/ClusterEntrypoint.java` | JVM 进程入口，初始化所有基础服务 |
| **Dispatcher** | `flink-runtime/.../dispatcher/Dispatcher.java` | Job 路由器，接收提交并创建 JobMaster |
| **JobMaster** | `flink-runtime/.../jobmaster/JobMaster.java` | 单个 Job 的管家，管理执行图和 Slot 池 |
| **DefaultScheduler** | `flink-runtime/.../scheduler/DefaultScheduler.java` | 核心调度引擎，决定何时何地执行 Task |
| **ResourceManager** | `flink-runtime/.../resourcemanager/ResourceManager.java` | 资源协调者，管理 TaskManager 注册和心跳 |
| **FineGrainedSlotManager** | `flink-runtime/.../slotmanager/FineGrainedSlotManager.java` | 细粒度 Slot 管理，匹配需求与资源 |
| **KubernetesResourceManagerDriver** | `flink-kubernetes/.../KubernetesResourceManagerDriver.java` | K8s 驱动层，将资源请求转化为 Pod 操作 |
| **FlinkKubeClient** | `flink-kubernetes/.../kubeclient/Fabric8FlinkKubeClient.java` | K8s API 封装，基于 Fabric8 库 |

---

## 3. 集群启动流程

### 3.1 集群启动时序图

```mermaid
sequenceDiagram
    autonumber
    participant Main as main()
    participant CE as ClusterEntrypoint
    participant SVC as 基础服务
    participant Factory as ComponentFactory
    participant D as Dispatcher
    participant RM as ResourceManager
    participant K8s as Kubernetes API

    Note over Main,K8s: 阶段一：JVM 进程启动

    Main->>CE: runClusterEntrypoint()
    CE->>CE: startCluster()

    Note over CE,SVC: 阶段二：基础服务初始化

    CE->>SVC: initializeServices()
    activate SVC
    SVC->>SVC: 创建 RpcService (Akka/Pekko)
    SVC->>SVC: 创建 HAServices
    SVC->>SVC: 创建 BlobServer
    SVC->>SVC: 创建 HeartbeatServices
    SVC->>SVC: 创建 MetricRegistry
    SVC-->>CE: 服务就绪
    deactivate SVC

    Note over CE,K8s: 阶段三：核心组件创建

    CE->>Factory: createDispatcherResourceManagerComponent()
    Factory->>D: 创建 Dispatcher
    Factory->>RM: 创建 ResourceManager (K8s 驱动)

    Note over RM,K8s: 阶段四：Kubernetes 资源管理初始化

    RM->>K8s: watchTaskManagerPods()
    K8s-->>RM: Pod Watcher 已建立
    RM->>K8s: 查询已存在的 TaskManager Pods
    K8s-->>RM: 返回现有 Pod 列表
    RM->>RM: recoverWorkerNodesFromPreviousAttempts()

    Note over D,K8s: 阶段五：集群就绪

    D->>D: 等待 Job 提交 (Session) / 自动提交 (Application)
```

### 3.2 启动流程详解

#### Session 模式启动

```
KubernetesSessionClusterEntrypoint.main()
│
├── KubernetesEntrypointUtils.loadConfiguration()
│   ├── 读取 FLINK_CONF_DIR 下的配置
│   ├── 如果使用 Host Network → 设置端口为 0 (自动分配)
│   └── 如果启用 HA → 从 POD_IP 环境变量设置 JM 地址
│
├── ClusterEntrypoint.runClusterEntrypoint(entrypoint)
│   └── entrypoint.startCluster()
│       ├── initializeServices(configuration)
│       │   ├── 创建 commonRpcService (RPC 通信)
│       │   ├── 创建 haServices (高可用)
│       │   ├── 创建 blobServer (大文件传输)
│       │   ├── 创建 heartbeatServices (心跳检测)
│       │   └── 创建 metricRegistry (监控指标)
│       │
│       └── runCluster(configuration, pluginManager)
│           └── createDispatcherResourceManagerComponentFactory()
│               │   → DefaultDispatcherResourceManagerComponentFactory
│               │       使用 KubernetesResourceManagerFactory
│               │
│               └── factory.create(...)
│                   ├── 创建 Dispatcher (接受 Job)
│                   └── 创建 ResourceManager (管理资源)
│                       └── KubernetesResourceManagerDriver.initializeInternal()
│                           ├── watchTaskManagerPods() — 监听 Pod 事件
│                           └── recoverWorkerNodesFromPreviousAttempts() — 恢复历史 Pod
```

#### Application 模式启动

```
KubernetesApplicationClusterEntrypoint.main()
│
├── 加载配置 (同 Session)
├── 初始化基础服务 (同 Session)
├── 创建 DispatcherResourceManagerComponent (同 Session)
│
└── 额外步骤:
    ├── getPackagedProgram() — 从远程获取用户 JAR
    ├── configureExecution() — 配置执行参数
    └── 自动提交并执行 Job (不等待外部提交)
```

---

## 4. Job 提交与启动流程

### 4.1 Job 提交全链路时序图

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client
    participant D as Dispatcher
    participant JMR as JobManagerRunner
    participant JM as JobMaster
    participant Sched as DefaultScheduler
    participant Strategy as SchedulingStrategy
    participant Deployer as ExecutionDeployer
    participant SAlloc as SlotAllocator
    participant RM as ResourceManager
    participant SM as SlotManager
    participant K8sDrv as K8sRM Driver
    participant K8s as Kubernetes

    Note over Client,K8s: 阶段一：Job 提交

    Client->>D: submitJob(ExecutionPlan)
    D->>D: 验证 Job 状态 (未重复/未终止)
    D->>JMR: createJobMasterRunner(ExecutionPlan)
    JMR->>JM: 创建 JobMaster 实例

    Note over JM,K8s: 阶段二：Job 执行启动

    JM->>JM: startJobExecution()
    JM->>JM: 创建 JobShuffleContext
    JM->>JM: 注册到 ShuffleMaster
    JM->>JM: startJobMasterServices()
    JM->>RM: registerJobMaster()
    RM-->>JM: 注册成功

    Note over Sched,K8s: 阶段三：调度规划

    JM->>Sched: startScheduling()
    Sched->>Strategy: 计算调度拓扑
    Strategy-->>Sched: 返回待调度 Vertex 列表
    Sched->>Deployer: allocateSlotsAndDeploy(vertices)

    Note over Deployer,K8s: 阶段四：Slot 分配

    Deployer->>Deployer: 转换 Execution 状态 → SCHEDULED
    Deployer->>SAlloc: allocateSlotsFor(executionAttemptIds)
    SAlloc->>RM: 声明资源需求
    RM->>SM: processResourceRequirements()
    SM->>SM: 检查可用 Slot

    alt Slot 不足
        SM->>K8sDrv: requestResource(TaskExecutorProcessSpec)
        K8sDrv->>K8sDrv: 构建 TaskManager Pod 参数
        K8sDrv->>K8s: createTaskManagerPod(KubernetesPod)
        K8s-->>K8sDrv: Pod 创建成功
        Note over K8sDrv,K8s: 等待 Pod 被调度和启动...
        K8s->>K8sDrv: Pod 事件: ADDED/MODIFIED (Scheduled)
        K8sDrv->>RM: onWorkerAdded(KubernetesWorkerNode)
    end

    Note over Deployer,K8s: 阶段五：TaskManager 注册

    K8s->>RM: TaskExecutor.registerTaskManager()
    RM->>SM: registerTaskManager(connection, slotReport)
    SM->>SM: 更新 Slot 可用状态

    Note over Deployer,K8s: 阶段六：Task 部署

    SM-->>SAlloc: Slot 就绪通知
    SAlloc-->>Deployer: SlotAssignment 完成
    Deployer->>JM: deployToSlot(Execution, Slot)
    JM->>K8s: submitTask(TaskDeploymentDescriptor)
    K8s-->>JM: Task 启动成功
```

### 4.2 Job 提交详细流程

```mermaid
flowchart TD
    A["Client 提交 Job<br/>(REST API / CLI)"] --> B{"Dispatcher<br/>验证 Job"}
    B -->|"已存在"| B1["返回错误:<br/>DuplicateJobSubmission"]
    B -->|"已终止"| B2["返回错误:<br/>JobExecutionFinished"]
    B -->|"验证通过"| C["createJobMasterRunner()"]

    C --> D["JobMaster 实例化"]
    D --> E["startJobExecution()"]

    E --> F["创建 ShuffleContext"]
    F --> G["注册到 ShuffleMaster"]
    G --> H["startJobMasterServices()"]
    H --> I["向 ResourceManager 注册"]

    I --> J["startScheduling()"]
    J --> K["SchedulingStrategy<br/>计算调度拓扑"]
    K --> L["确定待部署 Vertex 集合"]

    L --> M["allocateSlotsAndDeploy()"]
    M --> N["Execution 状态 → SCHEDULED"]
    N --> O["allocateSlotsFor()"]

    O --> P{"Slot 是否充足?"}
    P -->|"是"| Q["直接分配 Slot"]
    P -->|"否"| R["声明资源需求"]
    R --> S["SlotManager 处理需求"]
    S --> T["请求新 TaskManager"]
    T --> U["创建 K8s Pod"]
    U --> V["等待 Pod 就绪"]
    V --> W["TaskManager 注册"]
    W --> Q

    Q --> X["waitForAllSlotsAndDeploy()"]
    X --> Y["部署 Task 到 TaskManager"]
    Y --> Z["Job 开始执行"]

    style A fill:#e3f2fd,stroke:#1565c0
    style Z fill:#e8f5e9,stroke:#2e7d32
    style B1 fill:#ffebee,stroke:#c62828
    style B2 fill:#ffebee,stroke:#c62828
    style U fill:#e1f5fe,stroke:#0277bd
```

### 4.3 核心源码调用链

```
Dispatcher.submitJob()                     [Dispatcher.java:774]
  └─ Dispatcher.createJobMasterRunner()    [Dispatcher.java:1265]
     └─ jobManagerRunnerFactory.createJobManagerRunner()
        └─ JobMaster 构造函数
           └─ JobMaster.startJobExecution() [JobMaster.java:1172]
              ├─ createJobShuffleContext()
              ├─ startJobMasterServices()   [JobMaster.java:1178]
              └─ startScheduling()          [JobMaster.java:1272]
                 └─ schedulerNG.startScheduling()
                    └─ DefaultScheduler.allocateSlotsAndDeploy()  [DefaultScheduler.java:485]
                       └─ executionDeployer.allocateSlotsAndDeploy()
                          └─ DefaultExecutionDeployer [DefaultExecutionDeployer.java:90]
                             ├─ allocateSlotsFor() → ExecutionSlotAllocator
                             └─ waitForAllSlotsAndDeploy()
```

---

## 5. 资源调度与 Slot 管理

### 5.1 资源请求与分配流程

```mermaid
flowchart TD
    subgraph "JobMaster 端"
        A["SlotPool<br/>维护 Slot 需求"]
    end

    subgraph "ResourceManager 端"
        B["ResourceManager<br/>接收资源声明"]
        C["FineGrainedSlotManager"]
        D["ResourceTracker<br/>跟踪资源需求"]
        E["TaskManagerTracker<br/>跟踪可用资源"]
        F["ResourceAllocation<br/>Strategy<br/>分配策略"]
    end

    subgraph "Kubernetes 驱动层"
        G["KubernetesResource<br/>ManagerDriver"]
        H["KubernetesTask<br/>ManagerFactory"]
        I["FlinkKubeClient"]
    end

    subgraph "Kubernetes 集群"
        J["K8s API Server"]
        K["K8s Scheduler"]
        L["TaskManager Pod"]
    end

    A -->|"1. 声明资源需求"| B
    B -->|"2. 处理需求"| C
    C --> D
    C --> E
    C -->|"3. 计算缺口"| F
    F -->|"4. 需要新 Worker"| G
    G -->|"5. 构建 Pod Spec"| H
    H -->|"6. 创建 Pod"| I
    I -->|"7. API 调用"| J
    J -->|"8. 调度到 Node"| K
    K -->|"9. 启动容器"| L
    L -->|"10. 注册到 RM"| B
    B -->|"11. Slot 就绪"| A

    style A fill:#fff3e0,stroke:#e65100
    style C fill:#e8f5e9,stroke:#1b5e20
    style G fill:#e3f2fd,stroke:#1565c0
    style J fill:#f5f5f5,stroke:#424242
    style L fill:#e1f5fe,stroke:#0277bd
```

### 5.2 SlotManager 工作机制

```mermaid
stateDiagram-v2
    [*] --> 空闲Slot池

    空闲Slot池 --> 需求匹配 : processResourceRequirements()
    需求匹配 --> Slot已分配 : 找到匹配 Slot
    需求匹配 --> 请求新Worker : Slot 不足

    请求新Worker --> 等待Pod就绪 : requestResource()
    等待Pod就绪 --> TaskManager注册 : Pod Scheduled + TM Started
    TaskManager注册 --> 空闲Slot池 : registerTaskManager()

    Slot已分配 --> Task执行中 : deployTask()
    Task执行中 --> Slot已释放 : Task 完成/失败
    Slot已释放 --> 空闲Slot池 : releaseSlot()

    Task执行中 --> 异常处理 : TaskManager 丢失
    异常处理 --> 请求新Worker : 需要替代 Worker
    异常处理 --> 需求匹配 : 重新调度
```

### 5.3 FineGrainedSlotManager 核心逻辑

`FineGrainedSlotManager` 是 Kubernetes 部署下的默认 SlotManager 实现，采用声明式资源管理：

```
FineGrainedSlotManager 核心组件:
│
├── TaskManagerTracker    — 跟踪所有已注册的 TaskManager 及其 Slot
│   ├── 已注册 TM 列表
│   ├── 每个 TM 的 Slot 状态 (空闲/已分配/待分配)
│   └── TM 资源概况 (CPU/Memory)
│
├── ResourceTracker       — 跟踪所有 Job 的资源需求
│   ├── 各 Job 的 ResourceRequirement
│   └── 需求变更事件
│
├── ResourceAllocationStrategy — 资源分配决策
│   ├── 匹配现有 Slot 到需求
│   ├── 计算还需要多少新 Worker
│   └── 决定是否释放多余 Worker
│
└── SlotStatusSyncer      — Slot 状态同步
    ├── 同步 TM 上报的 Slot 状态
    └── 处理 Slot 分配/释放的状态更新
```

**关键方法调用链:**

| 方法 | 位置 | 说明 |
|------|------|------|
| `processResourceRequirements()` | `SlotManager.java:101` | 处理 Job 的资源需求声明 |
| `registerTaskManager()` | `SlotManager.java:113` | 注册新的 TaskManager |
| `reportSlotStatus()` | `SlotManager.java:136` | 接收 TM 的 Slot 状态上报 |
| `triggerResourceRequirementsCheck()` | `SlotManager.java:153` | 触发资源需求检查 |

---

## 6. Kubernetes Pod 生命周期管理

### 6.1 Pod 创建流程

```mermaid
sequenceDiagram
    autonumber
    participant SM as SlotManager
    participant Driver as K8sRM Driver
    participant Factory as TM Factory
    participant Client as FlinkKubeClient
    participant K8sAPI as K8s API Server
    participant Sched as K8s Scheduler
    participant Kubelet as Kubelet
    participant Watcher as PodWatcher

    SM->>Driver: requestResource(TaskExecutorProcessSpec)

    Note over Driver: 生成 Pod 参数
    Driver->>Driver: 生成 Pod 名称<br/>{clusterId}-taskmanager-{attemptId}-{podIndex}
    Driver->>Driver: 创建 KubernetesTaskManagerParameters

    Driver->>Factory: buildTaskManagerKubernetesPod(params)

    Note over Factory: 装饰器链处理
    Factory->>Factory: InitTaskManagerDecorator<br/>→ 元数据、标签、ServiceAccount
    Factory->>Factory: EnvSecretsDecorator<br/>→ 环境变量 Secrets
    Factory->>Factory: MountSecretsDecorator<br/>→ Secret 卷挂载
    Factory->>Factory: CmdTaskManagerDecorator<br/>→ 启动命令配置
    Factory->>Factory: FlinkConfMountDecorator<br/>→ Flink 配置挂载
    Factory-->>Driver: 返回 KubernetesPod

    Driver->>Client: createTaskManagerPod(kubernetesPod)

    Note over Client: 异步执行
    Client->>Client: 设置 OwnerReference<br/>→ JobManager Deployment
    Client->>K8sAPI: pods().create(pod)
    K8sAPI-->>Client: Pod 已创建 (Pending)
    Client-->>Driver: CompletableFuture

    K8sAPI->>Sched: 调度 Pod
    Sched->>Sched: 选择合适 Node<br/>(资源/亲和性/污点)
    Sched->>K8sAPI: 绑定 Pod 到 Node

    K8sAPI->>Kubelet: 启动 Pod
    Kubelet->>Kubelet: 拉取镜像
    Kubelet->>Kubelet: 创建容器
    Kubelet->>K8sAPI: 更新 Pod 状态 → Running

    K8sAPI->>Watcher: Pod 事件: ADDED/MODIFIED
    Watcher->>Driver: onPodScheduled()
    Driver->>Driver: 完成 requestResourceFuture
    Driver->>SM: onWorkerAdded(KubernetesWorkerNode)
```

### 6.2 Pod 生命周期状态机

```mermaid
stateDiagram-v2
    [*] --> PodCreating : requestResource()

    PodCreating --> Pending : K8s API 创建成功
    PodCreating --> CreateFailed : K8s API 创建失败

    Pending --> Scheduled : K8s 调度器分配 Node
    Pending --> ScheduleFailed : 无可用 Node

    Scheduled --> Running : 容器启动成功
    Scheduled --> ImagePullFailed : 镜像拉取失败

    Running --> TMRegistered : TaskManager 向 RM 注册
    TMRegistered --> 正常运行 : Slot 上报成功

    正常运行 --> Terminating : releaseResource() / 缩容
    正常运行 --> PodFailed : OOM / 节点故障 / 崩溃

    Terminating --> [*] : Pod 删除完成

    CreateFailed --> 重试 : RetryableException
    ScheduleFailed --> 重试 : 指数退避
    ImagePullFailed --> 重试 : 指数退避
    PodFailed --> 重试 : onPodTerminated()

    重试 --> PodCreating : requestResource()

    note right of 正常运行
        Pod Watcher 持续监控
        心跳检测保持连接
    end note

    note right of PodFailed
        Watcher 检测到
        pod.isTerminated()
    end note
```

### 6.3 Pod 事件处理机制

```mermaid
flowchart TD
    A["Kubernetes API Server<br/>Pod 事件"] -->|"Fabric8 Watch"| B["KubernetesPodsWatcher"]

    B --> C{"事件类型"}

    C -->|"ADDED"| D["onAdded(pods)"]
    C -->|"MODIFIED"| E["onModified(pods)"]
    C -->|"DELETED"| F["onDeleted(pods)"]
    C -->|"ERROR"| G["onError(pods)"]

    D --> H["PodCallbackHandler<br/>handlePodEventsInMainThread()"]
    E --> H
    F --> H
    G --> H

    H --> I{"Pod 状态判断"}

    I -->|"pod.isScheduled()"| J["onPodScheduled()"]
    I -->|"pod.isTerminated()"| K["onPodTerminated()"]
    I -->|"其他"| L["忽略，等待后续事件"]

    J --> M["完成 requestResourceFuture"]
    M --> N["创建 KubernetesWorkerNode"]
    N --> O["resourceEventHandler<br/>.onWorkerAdded()"]

    K --> P["Future 异常完成<br/>(RetryableException)"]
    P --> Q["resourceEventHandler<br/>.onWorkerTerminated()"]
    Q --> R["stopPod() 清理"]

    O --> S["SlotManager 注册新 Slot"]

    style A fill:#f5f5f5,stroke:#424242
    style J fill:#e8f5e9,stroke:#2e7d32
    style K fill:#ffebee,stroke:#c62828
    style S fill:#e3f2fd,stroke:#1565c0
```

### 6.4 Pod 命名规范

```
格式: {clusterId}-taskmanager-{attemptId}-{podIndex}
示例: my-flink-cluster-taskmanager-0-3

┌────────────────────┬─────────────┬──────────┬──────────┐
│    clusterId       │   固定前缀   │ attemptId│ podIndex │
│  my-flink-cluster  │ taskmanager │    0     │    3     │
└────────────────────┴─────────────┴──────────┴──────────┘

命名正则: \S+-taskmanager-([\d]+)-([\d]+)
```

- **clusterId**: 集群唯一标识，来自配置
- **attemptId**: ResourceManager 重启次数，用于区分不同代
- **podIndex**: 在当前 attempt 中的递增编号

---

## 7. Pod 装饰器模式详解

### 7.1 装饰器架构

```mermaid
graph TB
    subgraph "KubernetesStepDecorator 接口"
        I["decorateFlinkPod(FlinkPod)<br/>buildAccompanyingKubernetesResources()"]
    end

    subgraph "JobManager 装饰器链"
        JM1["InitJobManagerDecorator<br/>━━━━━━━━━━━━━━<br/>Pod 元数据、标签<br/>ServiceAccount<br/>Tolerations、Affinity"]
        JM2["EnvSecretsDecorator<br/>━━━━━━━━━━━━━━<br/>环境变量 Secrets"]
        JM3["MountSecretsDecorator<br/>━━━━━━━━━━━━━━<br/>Secret 卷挂载"]
        JM4["CmdJobManagerDecorator<br/>━━━━━━━━━━━━━━<br/>启动命令和参数"]
        JM5["InternalServiceDecorator<br/>━━━━━━━━━━━━━━<br/>内部 K8s Service"]
        JM6["ExternalServiceDecorator<br/>━━━━━━━━━━━━━━<br/>外部暴露 Service"]
        JM7["FlinkConfMountDecorator<br/>━━━━━━━━━━━━━━<br/>flink-conf 挂载"]
        JM8["HadoopConfMountDecorator<br/>━━━━━━━━━━━━━━<br/>Hadoop 配置挂载"]
    end

    subgraph "TaskManager 装饰器链"
        TM1["InitTaskManagerDecorator<br/>━━━━━━━━━━━━━━<br/>Pod 元数据、DNS<br/>NodeAffinity (屏蔽节点)"]
        TM2["EnvSecretsDecorator"]
        TM3["MountSecretsDecorator"]
        TM4["CmdTaskManagerDecorator<br/>━━━━━━━━━━━━━━<br/>TM 启动命令和参数"]
        TM5["FlinkConfMountDecorator"]
        TM6["HadoopConfMountDecorator"]
        TM7["KerberosMountDecorator<br/>━━━━━━━━━━━━━━<br/>Kerberos 凭据"]
        TM8["PodTemplateMountDecorator<br/>━━━━━━━━━━━━━━<br/>用户自定义模板"]
    end

    I --> JM1
    I --> TM1

    JM1 --> JM2 --> JM3 --> JM4 --> JM5 --> JM6 --> JM7 --> JM8
    TM1 --> TM2 --> TM3 --> TM4 --> TM5 --> TM6 --> TM7 --> TM8

    style I fill:#fff9c4,stroke:#f9a825
    style JM1 fill:#e1bee7,stroke:#6a1b9a
    style TM1 fill:#b2dfdb,stroke:#00695c
```

### 7.2 装饰器处理流程

```mermaid
flowchart LR
    A["空白 FlinkPod"] --> B["Decorator 1<br/>Init"]
    B --> C["Decorator 2<br/>EnvSecrets"]
    C --> D["Decorator 3<br/>MountSecrets"]
    D --> E["Decorator 4<br/>Cmd"]
    E --> F["Decorator 5<br/>FlinkConf"]
    F --> G["Decorator N<br/>..."]
    G --> H["最终 Pod Spec"]

    style A fill:#ffecb3,stroke:#ff8f00
    style H fill:#c8e6c9,stroke:#2e7d32
```

每个装饰器接收一个 `FlinkPod`，在其基础上添加配置后返回新的 `FlinkPod`：

```java
// 装饰器链伪代码
FlinkPod pod = new FlinkPod.Builder().build();
for (KubernetesStepDecorator decorator : decorators) {
    pod = decorator.decorateFlinkPod(pod);
}
// pod 现在包含所有配置
```

---

## 8. 容错与恢复机制

### 8.1 ResourceManager 重启恢复

```mermaid
sequenceDiagram
    autonumber
    participant RM as ResourceManager<br/>(新实例)
    participant Driver as K8sRM Driver
    participant K8s as Kubernetes API

    Note over RM,K8s: ResourceManager 重启后的恢复流程

    RM->>Driver: initializeInternal()
    Driver->>K8s: watchTaskManagerPods()
    K8s-->>Driver: Watcher 建立

    Driver->>K8s: getPodsWithLabels<br/>({cluster=xxx, component=taskmanager})
    K8s-->>Driver: 返回现有 Pod 列表

    loop 遍历每个 Pod
        Driver->>Driver: 检查 Pod 状态
        alt Pod 正在运行 (Scheduled)
            Driver->>Driver: 从 Pod 名提取 attemptId
            Driver->>RM: onWorkerAdded(node)
            Note over RM: 重新注册为可用 Worker
        else Pod 已终止
            Driver->>K8s: stopPod(podName)
            Note over Driver: 清理已终止的 Pod
        else Pod 未调度
            Driver->>K8s: stopPod(podName)
            Note over Driver: 清理未调度的 Pod
        end
    end

    Driver->>Driver: 更新 currentMaxAttemptId
    Note over Driver: 后续新 Pod 使用<br/>新的 attemptId
```

### 8.2 Pod 故障处理

```mermaid
flowchart TD
    A["检测到 Pod 异常"] --> B{"异常类型"}

    B -->|"Pod 被删除"| C["onPodTerminated()"]
    B -->|"Pod OOM/Crash"| C
    B -->|"Node 故障"| C
    B -->|"Watch 连接断开"| D["KubernetesTooOld<br/>ResourceVersionException"]

    C --> E["通知 ResourceManager:<br/>onWorkerTerminated()"]
    E --> F["SlotManager 释放相关 Slot"]
    F --> G["触发资源需求重新检查"]
    G --> H{"需要新 Worker?"}
    H -->|"是"| I["requestResource()<br/>创建新 Pod"]
    H -->|"否"| J["等待下次需求"]

    D --> K["使用指数退避重连"]
    K --> L["重新建立 Pod Watch"]
    L --> M["全量同步 Pod 状态"]

    style A fill:#ffebee,stroke:#c62828
    style I fill:#e8f5e9,stroke:#2e7d32
    style L fill:#e3f2fd,stroke:#1565c0
```

### 8.3 BlockList 机制

当某个 Kubernetes 节点频繁出错时，Flink 会将其加入黑名单：

```
InitTaskManagerDecorator 中会设置 NodeAffinity:
│
├── 检查 blockedNodes 列表
├── 如果有被屏蔽的节点
│   └── 设置 Pod 的 nodeAffinity:
│       └── requiredDuringSchedulingIgnoredDuringExecution:
│           └── nodeSelectorTerms:
│               └── matchExpressions:
│                   └── key: kubernetes.io/hostname
│                       operator: NotIn
│                       values: [blocked-node-1, blocked-node-2, ...]
│
└── 新创建的 Pod 不会被调度到这些节点上
```

---

## 9. 关键源码索引

### 9.1 Kubernetes 模块

| 文件 | 路径 | 核心方法 |
|------|------|---------|
| **KubernetesResourceManagerDriver** | `flink-kubernetes/src/.../KubernetesResourceManagerDriver.java` | `initializeInternal()`, `requestResource()`, `releaseResource()`, `onPodScheduled()`, `onPodTerminated()` |
| **KubernetesWorkerNode** | `flink-kubernetes/src/.../KubernetesWorkerNode.java` | `getAttempt()`, 资源 ID 编码 |
| **FlinkKubeClient** | `flink-kubernetes/src/.../kubeclient/FlinkKubeClient.java` | `createTaskManagerPod()`, `stopPod()`, `watchPodsAndDoCallback()` |
| **Fabric8FlinkKubeClient** | `flink-kubernetes/src/.../kubeclient/Fabric8FlinkKubeClient.java` | API 调用实现、OwnerReference 管理、重试逻辑 |
| **KubernetesJobManagerFactory** | `flink-kubernetes/src/.../factory/KubernetesJobManagerFactory.java` | JM Deployment 构建 |
| **KubernetesTaskManagerFactory** | `flink-kubernetes/src/.../factory/KubernetesTaskManagerFactory.java` | TM Pod 构建 |
| **KubernetesPodsWatcher** | `flink-kubernetes/src/.../resources/KubernetesPodsWatcher.java` | Pod 事件监听 |
| **KubernetesSessionClusterEntrypoint** | `flink-kubernetes/src/.../entrypoint/KubernetesSessionClusterEntrypoint.java` | Session 模式入口 |
| **KubernetesApplicationClusterEntrypoint** | `flink-kubernetes/src/.../entrypoint/KubernetesApplicationClusterEntrypoint.java` | Application 模式入口 |
| **KubernetesResourceManagerFactory** | `flink-kubernetes/src/.../entrypoint/KubernetesResourceManagerFactory.java` | RM 驱动工厂 |
| **KubernetesEntrypointUtils** | `flink-kubernetes/src/.../entrypoint/KubernetesEntrypointUtils.java` | 配置加载、端口处理 |

### 9.2 Runtime 核心模块

| 文件 | 路径 | 核心方法 |
|------|------|---------|
| **ClusterEntrypoint** | `flink-runtime/src/.../entrypoint/ClusterEntrypoint.java` | `startCluster()`, `initializeServices()`, `runCluster()` |
| **Dispatcher** | `flink-runtime/src/.../dispatcher/Dispatcher.java` | `submitJob()`, `createJobMasterRunner()` |
| **JobMaster** | `flink-runtime/src/.../jobmaster/JobMaster.java` | `startJobExecution()`, `startScheduling()`, `registerTaskManager()` |
| **DefaultScheduler** | `flink-runtime/src/.../scheduler/DefaultScheduler.java` | `allocateSlotsAndDeploy()` |
| **DefaultExecutionDeployer** | `flink-runtime/src/.../scheduler/DefaultExecutionDeployer.java` | `allocateSlotsAndDeploy()`, `waitForAllSlotsAndDeploy()` |
| **ResourceManager** | `flink-runtime/src/.../resourcemanager/ResourceManager.java` | `registerJobMaster()`, `registerJobMasterInternal()` |
| **SlotManager** | `flink-runtime/src/.../slotmanager/SlotManager.java` | `processResourceRequirements()`, `registerTaskManager()`, `reportSlotStatus()` |
| **FineGrainedSlotManager** | `flink-runtime/src/.../slotmanager/FineGrainedSlotManager.java` | 声明式 Slot 管理实现 |

### 9.3 关键设计模式

| 模式 | 应用场景 | 说明 |
|------|---------|------|
| **装饰器模式** | Pod 规格构建 | `KubernetesStepDecorator` 链式装饰 FlinkPod |
| **工厂模式** | 组件创建 | `KubernetesResourceManagerFactory`, `KubernetesJobManagerFactory` |
| **观察者模式** | Pod 事件监听 | `KubernetesPodsWatcher` + `WatchCallbackHandler` |
| **异步 Future** | K8s API 调用 | 所有 K8s 操作返回 `CompletableFuture` |
| **策略模式** | 调度与分配 | `SchedulingStrategy`, `ResourceAllocationStrategy` |
| **模板方法** | 集群入口点 | `ClusterEntrypoint` 定义骨架，子类实现差异 |

---

> **本文档基于 Apache Flink 源码分析生成，覆盖了 Flink on Kubernetes 的核心调度流程与 Job 启动全链路。**
