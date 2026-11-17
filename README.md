# 制品下载切流代理（Artifact Relay）

单进程 HTTP GET 代理 + 切换 API：值班员把新下载切到新版上游，同时给旧版上游已受理的下载
留出收尾时间。FastAPI 提供控制面，aiohttp 提供上游连接，uvicorn 单 worker。

```
客户端 ──GET──▶ relay (FastAPI/uvicorn, 单进程) ──GET──▶ blue  (候选 A)
                       │  POST /api/switch                green (候选 B)
                       └─ 探活 GET <target>/ready
```

## 关键语义

| 需求 | 实现 |
| --- | --- |
| 候选探活 200 才切流 | `POST /api/switch` 先向候选的 `RELAY_PROBE_PATH`（默认 `/ready`）发 GET，仅 200 提交；其余状态码、超时、连接错误都算失败 |
| 失败保留现行目标 | 探活失败只写一条 failed 操作记录，不改 `current`，也不产生新代 |
| 后受理的切换废止先前未完成候选 | 新切换被受理时，旧候选立即标为 `superseded` 并取消探活任务。提交路径会再次校验“我仍是当前待定候选”，因此**探活已经返回 200 但尚未提交**的旧候选也无法翻流 |
| 迟到探活不得切回 | 同上：`_commit_if_pending` 在状态锁内比较 `self.pending is op`，不匹配直接丢弃 |
| 每个 GET 固定受理时的目标 | 请求进入路由函数后，**第一次 await 之前**同步读取 `state.current` 并在该代上登记；之后即使切流也不会改变这条请求的上游 |
| 长连接后续请求按新目标 | 目标解析发生在每个请求（而非每条连接）上，keep-alive/pipeline 的下一个请求重新读取当前代 |
| 切流后旧代可继续 5 秒 | 提交时给旧代开 drain 窗口（`RELAY_DRAIN_SECONDS`，默认 5s，状态里带剩余秒数）。窗口内旧代请求照常转发 |
| 到期只中断该代未结束请求 | 定时器只调用**那一代**的 `abort_all()`；每条代理请求只监听自己代的 abort 事件，响应体在下一个 chunk 边界停止发送并让 uvicorn 关闭该连接（客户端看到截断）。其他代（含新代）完全不受影响 |
| 连续切换、回切同址不得误断新代 | 每次成功切流都会新建**代**（epoch），即使目标 URL 与历史某代相同。drain 定时器绑定的是代对象而不是 target 名，所以“蓝→绿→蓝→绿”里第 2 代的到期绝不会影响第 4 代的连接 |
| 响应逐块转发、不整份缓存 | `upstream.content.iter_chunked(RELAY_CHUNK_SIZE)` 逐块 `send`；有测试断言首字节在整份响应结束前很早就到达客户端 |
| 慢探活不挡切换/查询 | 探活是独立任务，`POST /api/switch` 默认立刻返回 202（`wait=true` 才阻塞，且可设超时）；`/api/status` 是纯同步快照，不取锁、不 await 任何网络 |
| 慢下载不挡切换/查询 | 探活与代理使用**两个独立的 aiohttp session/连接池**；控制面不等待任何下载 |
| `/docs` 可切换、查目标与各代在途数 | OpenAPI 见 `/docs`；`GET /api/status` 返回当前目标/代、各代状态与 `in_flight`、进行中的请求明细；`/demo` 是可视化演示页 |
| 核验下载：只下发整包、核验通过才发首字节 | `GET|POST /api/verified-download` 把上游响应逐块写入临时文件并同时算 SHA-256；**匹配后**才发响应头和文件。不通过时不发任何制品字节，只回 JSON 错误并删除暂存文件 |
| 核验下载固定受理时的目标 | 与其他请求相同：在第一个 `await` 之前同步读取 `state.current` 并登记为 `kind="verified-download"` 的在途请求 |
| 核验下载只接受完整 200、单件 ≤16 MiB | 非 200（含 3xx）→ `502 upstream_status`（不转发上游错误体）；`Content-Length` 超限立即 `413`，无长度时边读边计数、超限即停 + `413`；上游提前断流 → `502 upstream_truncated` |
| 核验与下发都算在途；切流后旧代 5 秒到期 | 暂存阶段和下发阶段共用同一个 `InFlightRequest`/abort 事件；drain 到期会中止核验（未发头 → `503 generation_draining`）或截断下发（不发最终 body 帧，客户端看到不完整传输） |
| 客户端断连停止并清暂存 | 断开信号同时参与暂存读、文件发两处的等待；所有退出路径都 unlink 暂存文件，未发完的部分绝不当作完成 |

补充约定：

* **同址切换是 no-op**：候选目标就是当前目标时，探活仍需 200，但明确返回 `status: "noop"`，
  不新建代、不 drain —— 所以重复点“切到现在这版”不会误伤正在跑的下载。
* 探活只决定提交与否，**不读取探活响应体**：探活端点返回超慢/无限长的 body 也不会拖住切换。
* 旧代 drain 到期时，若该代已无在途请求，会立即标为 `closed`，状态快照保持如实。
* 只代理 GET/HEAD；WebSocket / `CONNECT` 不支持。上游返回的 3xx `Location` 原样透传
  （不重写 Location，所以跨代的绝对跳转不会自动改写到新目标）。
* 切流不重放任何请求：已经发出并失败的请求不会被自动重试，由客户端自行重试。

## 运行（Docker Compose）

```bash
docker compose up --build
```

| 地址 | 内容 |
| --- | --- |
| http://localhost:8080/docs | 切换 API、状态查询、核验下载 API |
| http://localhost:8080/demo | 演示页（切换按钮 + 代/在途数 + 下载链接 + 核验下载） |
| http://localhost:8080/api/status | 当前目标、各代状态/在途数、最近操作 |
| http://localhost:9001 | blue 上游（直连，用于调探活） |
| http://localhost:9002 | green 上游（直连，用于调探活） |

两个上游都带可控探活延迟与慢下载：

```bash
# 让 green 的探活慢 4 秒（relay 的探活超时默认 2s → 该次切换会失败）
curl -X POST 'http://localhost:9002/_control/probe?delay=4'
# 让 green 的探活直接 503（非 200 → 不切流）
curl -X POST 'http://localhost:9002/_control/probe?delay=0&status=503'
# 恢复
curl -X POST 'http://localhost:9002/_control/probe?delay=0&status=200'

# 触发切换
curl -X POST http://localhost:8080/api/switch \
  -H 'content-type: application/json' -d '{"target":"green","wait":true}'
curl http://localhost:8080/api/status | python3 -m json.tool
```

下载演示脚本（先起 compose，或者用下面的本地方式）：

```bash
./demo/download.sh          # 慢下载 + 切流 + 观察 5s 后截断 + 新下载立即走新目标
```

它做三件事：开一个约 75 秒的下载 → 切到另一版 → 新下载立刻带 `x-relay-target` 新值 →
旧下载在切流后约 5 秒被截断（`curl: (18)`），`/api/status` 里旧代 `closed`、在途数归零。
脚本读 `RELAY` 环境变量，默认 `http://localhost:8080`（本地手跑时用
`RELAY=http://127.0.0.1:<port> ./demo/download.sh`）。

核验下载演示：

```bash
./demo/verified_download.sh   # 取摘要 → 核验下发 → 错摘要 422 → 404 上游 502 → 切流中止
```

### 手动跑两个可控上游（不用 docker）

```bash
python -m uvicorn upstream.app:app --port 9001 &   # UPSTREAM_NAME=blue
UPSTREAM_NAME=green python -m uvicorn upstream.app:app --port 9002 &
RELAY_TARGETS='blue=http://127.0.0.1:9001,green=http://127.0.0.1:9002' \
  RELAY_DRAIN_SECONDS=5 python -m relay.main
```

（`python -m relay.main` 发的是单进程 uvicorn；或 `uvicorn relay.main:build_app --factory`。）

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RELAY_TARGETS` | `blue=…:9001,green=…:9002` | `name=url` 逗号分隔，也接受 JSON `{"name":"url"}` |
| `RELAY_INITIAL_TARGET` | 第一个 target | 启动时的现行目标（**启动不探活**） |
| `RELAY_PROBE_PATH` | `/ready` | 候选探活路径 |
| `RELAY_PROBE_TIMEOUT` | `2` | 探活总超时（秒），超时=失败 |
| `RELAY_DRAIN_SECONDS` | `5` | 旧代收尾窗口 |
| `RELAY_CONNECT_TIMEOUT` | `5` | 上游连接超时 |
| `RELAY_SOCK_READ_TIMEOUT` | `0` | 上游读超时，0=不限（慢下载需要） |
| `RELAY_CHUNK_SIZE` | `65536` | 转发块大小（不缓冲整份响应） |
| `RELAY_HOST` / `RELAY_PORT` | `0.0.0.0` / `8080` | 监听地址 |
| `RELAY_SHUTDOWN_GRACE_SECONDS` | `5` | 退出时等长下载收尾的上限，超时后强制断开并 abort 所有代 |
| `RELAY_MAX_CLOSED_GENERATIONS` | `100` | 状态里保留的已关闭代数（活跃/收尾中的代永远不会被裁掉） |
| `RELAY_VERIFIED_MAX_BYTES` | `16777216` | 核验下载单件上限；默认且最大为 16 MiB，只能调低（调高会在启动时报错）；超限 `413` |
| `RELAY_STAGING_DIR` | 系统临时目录 | 核验暂存目录；每件一个 `mkstemp` 唯一文件（0600），不共享缓存 |

## 控制面 API

* `POST /api/switch` — body `{"target": "green", "wait": false, "wait_timeout": 30}`。
  立即返回 `202` + `operation.status == "pending"`；探活完成后状态变为
  `committed` / `noop` / `failed` / `superseded`。`wait=true` 时最多等 `wait_timeout` 秒
  （该操作完成后返回 `200`）。未知目标 → `404`，body 不合法 → `422`。
* `GET /api/status` — `current`、`current_generation`、`pending`、`generations[]`
  （`state` = `active`/`draining`/`closed`、`in_flight`、`drain_remaining_seconds`、
  `in_flight_requests[]`）、`recent_operations[]`（最近 50 次）。
* `GET /api/operations/{id}` — 单次切换的最终状态。
* `GET /api/targets` — 目标表 + 当前目标。

## 核验下载 API

`GET|POST /api/verified-download`（别名 `GET|POST /api/download`，仅 GET/POST，不占用普通
GET 路径；别名不出现在 `/docs`，避免重复条目）。

参数：

| 名称 | 位置 | 说明 |
| --- | --- | --- |
| `path` | query / JSON body | 上游相对路径，**可带查询串**。必须相对（拒绝 `//host`、`scheme://`、控制字符）；不带前导 `/` 时会补上 |
| `sha256` | query / JSON body | 期望的 SHA-256：64 位十六进制，大小写均可，可带 `sha256:` / `sha256=` 前缀 |

```bash
# 演示上游会公布确定性制品的摘要
DIGEST=$(curl -sS 'http://localhost:8080/verified/manifest?seed=demo&bytes=1048576' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["sha256"])')

curl -sS -D - -o /tmp/artifact.bin \
  "http://localhost:8080/api/verified-download?path=%2Fverified%2Fartifact%3Fseed%3Ddemo%26bytes%3D1048576&sha256=$DIGEST"
# 或者 POST:
curl -sS -o /tmp/artifact.bin http://localhost:8080/api/verified-download \
  -H 'content-type: application/json' \
  -d "{\"path\":\"/verified/artifact?seed=demo&bytes=1048576\",\"sha256\":\"$DIGEST\"}"
```

成功的响应带 `x-relay-verified: sha256`、`x-relay-sha256`、`x-relay-target`、
`x-relay-generation`，并给出准确的 `content-length`。其他结果：

| 情况 | 状态码 | `error` |
| --- | --- | --- |
| 必需参数缺失（GET query / POST body 结构错） | `422` | FastAPI 校验详情 |
| 参数存在但非法（空、非 64 位十六进制、绝对 URL、控制字符） | `400` | `detail` |
| 上游非 200（3xx 不跟随） | `502` | `upstream_status` |
| 上游带非 identity `Content-Encoding` | `502` | `upstream_encoding` |
| 连接失败 / 上游读超时 | `502` / `504` | `upstream_error` / `upstream_timeout` |
| 声明长度或实际流量超 16 MiB | `413` | `artifact_too_large` |
| 上游提前断流或少于声明长度 | `502` | `upstream_truncated` |
| SHA-256 不符 | `422` | `digest_mismatch`（含 `expected_sha256` / `actual_sha256`） |
| 代 drain 到期（暂存中/下发前） | `503` | `generation_draining` |
| 代 drain 到期（下发中） | 已发 `200`，随后截断、无最终帧 | 客户端看到不完整传输（如 `curl: (18)`） |
| 客户端断连 | `499`（未发头时） | `client_disconnected` |

语义要点：

* **核验通过前不发任何响应头**。客户端在暂存/核验期间收不到 `200`；失败时收到的只有
  JSON 错误，不包含制品字节，暂存文件随即删除。
* 暂存是**逐块写临时文件**，不把整件读进内存（哈希也是逐块更新）；文件在全部退出路径上
  都会删除。下发时先打开文件再立即 unlink 文件名，因此下发期间磁盘上也不留制品副本，
  进程崩溃/断连也不会残留（文件名唯一、0600）。
* 请求侧会去掉 `Range`/条件请求/`Accept-Encoding` 等头并强制 `identity`，保证核验的是
  将要下发的整包字节；上游响应 `Content-Length` 与实际字节数不一致按断流处理。
* 核验与下发是**同一个在途请求**：`/api/status` 里 `kind == "verified-download"`，
  `detail` 依次显示 `fetching` / `verifying` / `delivering` 与 `bytes_staged`。切流后旧代
  满 5 秒会中止正在核验的请求（`503`）或截断正在下发的文件；截断时不发最终 body 帧，
  客户端只会看到不完整传输，不会误以为整包下载完成。
* 每次请求独立暂存，没有跨请求的“下载缓存”或命中判断 —— 需要缓存请放在上游。
* 演示上游的 `/verified/artifact` 字节只由 `(seed, bytes)` 决定（与分块大小、上游名无关），
  `/verified/manifest` 发布其摘要，因此切到 blue/green 任一侧都能核验同一份清单；
  `/verified/broken` 模拟“声明长度但提前断流”。

## 设计要点（为什么这么做）

* **代(epoch) + 每代独立 abort 事件**：只有“代”能表达“切流那一刻已经在跑的请求”。
  用 target 名做键时，回切同址会把两批请求混在一起，正是“误断新代”的来源。
* **同步登记**：登记发生在路由函数首个 await 之前，配合单进程单事件循环，
  切流提交与请求受理之间不存在交错窗口。
* **两个连接池**：慢下载会占满连接，探活必须有自己的池，否则“慢下载不挡切换”不成立。
* **不缓冲**：`StreamingResponse` 自定义为逐块 `send`；同时把 `send` 与 abort 事件
  `asyncio.wait` 竞速，避免慢客户端把 5 秒 drain 拖成无限长。
* **中断方式**：drain 到期后不再发送最终 body 帧，uvicorn 关闭该连接，客户端得到截断
  （`curl: (18)` / `ClientPayloadError`）而不是假的 200 完整文件。uvicorn 会因此打一条
  `ERROR: ASGI callable returned without completing response` —— 这是被中断下载的预期日志。
* 上游自己返回的 `x-relay-*` 响应头会被剥掉，避免伪造路由标记。
* 上游返回的 `date`/`server` 也被剥掉，由 uvicorn 生成，避免出现两个同名响应头。
* **核验下载为什么先落盘**：只有“先完整拿到 + 校验”才能保证 200 响应头之后一定是正确整包；
  若边转发边校验，摘要不符时客户端已经收到部分字节。落盘用 `mkstemp` 的唯一文件，
  下发前 unlink 文件名、保留已打开的描述符，既避免残留，也不占额外内存。
  核验请求复用普通代理的同一个上游连接池与代（epoch）机制，因此切流/在途计数/5 秒
  收尾的语义与普通下载完全一致。核验读块、文件读写都通过 `asyncio.to_thread` 让出事件循环，
  慢磁盘不挡切换/状态查询。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

`tests/` 分两层：`test_switch_unit.py` 用假探活 session 覆盖状态机（失败保留、supersede、
迟到 200、drain 只作用于本代、回切同址、no-op）；`test_proxy_integration.py` 起真实
TCP 上游 + relay，覆盖逐块转发、长连接重路由、在途计数、5 秒截断、502/404、断开清理等。
`test_verified_download.py` 覆盖核验下载：成功整包、GET/POST、别名、摘要大小写与前缀、
错摘要 `422` 无字节、非 200/重定向 `502`、声明/流式超限 `413`、断流 `502`、暂存清理、
核验中切流 `503`、下发中 drain 截断、客户端断连（核验/下发两阶段）清理、并发独立暂存、
零字节与恰好等于上限、慢核验不挡切换/查询、HEAD 不落到上游；`test_verified_unit.py`
覆盖路径与摘要参数校验。
