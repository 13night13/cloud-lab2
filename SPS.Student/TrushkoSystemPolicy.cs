using SPS.Abstractions;

namespace SPS.Student;

// Лабораторная работа №2: стратегия управления сервисом.
//
// Идеи решения:
//  1) Кэш. Храним ответ, пока он может быть свежим (TTL ~ MaxDataAge клиента). Отдаём из кэша,
//     если возраст данных с запасом на доставку укладывается в MaxDataAge. Иначе присоединяемся
//     к уже идущей загрузке того же ключа (coalescing) или начинаем новую. При переполнении
//     вытесняем самые холодные (редко запрашиваемые) и самые старые записи.
//  2) Маршрутизация. API выбираем по минимальной относительной загрузке (OutstandingCalls / Slots).
//  3) Здоровье экземпляров. Если последний вызов дал таймаут — блокируем экземпляр и проверяем
//     его пробой раз в секунду. Воркер блокируем также при пропаже heartbeat.
//  4) Задания. Держим очередь короткой: если она длиннее порога, отклоняем наименее выгодное
//     задание — иначе клиенты ждут дольше своего (скрытого) терпения и работа воркера пропадает.
//     Из очереди берём задание с максимальной ожидаемой выручкой на секунду работы воркера.
//  5) Мощности. Число воркеров подстраиваем под наблюдаемую интенсивность потока Generate и
//     масштабируемся вверх только если воркер окупается. Один API справляется с потоком,
//     второй поднимается на время сбоя первого.
public sealed class TrushkoSystemPolicy : ISystemPolicy
{
    private static readonly TimeSpan CacheSafetyMargin = TimeSpan.FromMilliseconds(30);
    private static readonly TimeSpan ProbeInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan HeartbeatLimit = TimeSpan.FromSeconds(2.5);
    private static readonly TimeSpan RecentFailureWindow = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan RateWindow = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RequeueDelay = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan JoinWindowMetadata = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan JoinWindowPreview = TimeSpan.FromMilliseconds(700);

    // Средняя длительность работы Generate (нужна только как оценка, пока нет наблюдений).
    private const double DefaultWorkerSeconds = 3.3;
    // 1 API * 1 CU + 5 воркеров * 2 CU = 11 CU <= 12 — лимит сценария соблюдён.
    private const int MaximumWorkers = 5;
    // Один API справляется с потоком (4 слота, работа ~15 мс). Второй нужен только как резерв
    // на время сбоя: тогда он и поднимается.
    private const int BaseApis = 1;
    private const int ReserveApis = 2;
    // Общие диапазоны терпения Generate из задания.
    private const double MinimumPatienceSeconds = 15;
    private const double MaximumPatienceSeconds = 60;

    private readonly Dictionary<long, RequestKind> kinds = new();
    private readonly Dictionary<string, int> reads = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TimeSpan> maxAges = new(StringComparer.Ordinal);
    private readonly Dictionary<long, TimeSpan> firstSeen = new();
    private readonly Dictionary<long, TimeSpan> lastProbe = new();
    private readonly List<GenerateSample> generateSamples = new();
    private TimeSpan lastActivity = TimeSpan.Zero;
    private TimeSpan lastCapacityReview = TimeSpan.MinValue;
    private TimeSpan lastApiFailure;
    private bool apiFailureSeen;
    private double rateEstimate;
    private int plannedWorkers = 2;
    private int reduceHold;

    public TrushkoSystemPolicy() => RpcPolicy = new RequestPolicy(this);

    public IRequestPolicy RpcPolicy { get; }

    // ---------- Входящие запросы ----------

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        kinds[request.Id] = request.Kind;
        if (request.CreatedAt > lastActivity) lastActivity = request.CreatedAt;

        if (request.Kind == RequestKind.Generate)
        {
            generateSamples.Add(new GenerateSample(request.CreatedAt, request.Revenue,
                Math.Max(0.1, request.EstimatedWorkSeconds)));
            if (generateSamples.Count > 2048) generateSamples.RemoveRange(0, 1024);
        }

        // Жёсткий лимит жизни не нужен: RPC ограничены таймаутами, а очередь заданий — политикой.
        return null;
    }

    // ---------- Кэш ----------

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var request = context.Request;
        string key = request.ResourceKey;
        reads[key] = reads.GetValueOrDefault(key) + 1;
        maxAges[key] = maxAges.TryGetValue(key, out var known) && known > request.MaxDataAge
            ? known
            : request.MaxDataAge;

        if (context.Entry is { } entry)
        {
            var age = context.Now - entry.SourceObservedAt;
            if (age < TimeSpan.Zero) age = TimeSpan.Zero;
            // Запас на сеть и доставку, чтобы не получить StaleResponse.
            if (age + CacheSafetyMargin <= request.MaxDataAge)
                return CacheReadDecision.ServeCached();
        }

        // Объединение одинаковых загрузок: второй клиент бесплатно едет на уже начатой.
        FetchInfo? youngest = null;
        var window = request.Kind == RequestKind.Metadata ? JoinWindowMetadata : JoinWindowPreview;
        foreach (var fetch in context.PendingFetches)
        {
            if (fetch.WaitingCustomers <= 0) continue;
            var elapsed = context.Now - fetch.StartedAt;
            if (elapsed < TimeSpan.Zero || elapsed > window) continue;
            if (youngest == null || fetch.StartedAt > youngest.StartedAt) youngest = fetch;
        }

        return youngest != null
            ? CacheReadDecision.JoinFetch(youngest.Id)
            : CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var value = context.Value;
        long capacity = context.CapacityBytes;
        if (value.SizeBytes <= 0 || value.SizeBytes > capacity) return CacheWriteDecision.Skip();

        var ttl = RetentionFor(value.Key);

        long used = 0;
        foreach (var entry in context.Entries)
            if (!string.Equals(entry.Key, value.Key, StringComparison.Ordinal)) used += entry.SizeBytes;

        if (used + value.SizeBytes <= capacity) return CacheWriteDecision.Retain(ttl);

        // Места нет: вытесняем самые холодные, затем самые близкие к истечению записи.
        var evictions = new List<string>();
        long need = used + value.SizeBytes - capacity;
        foreach (var candidate in context.Entries
                     .Where(e => !string.Equals(e.Key, value.Key, StringComparison.Ordinal))
                     .OrderBy(e => reads.GetValueOrDefault(e.Key))
                     .ThenBy(e => e.ExpiresAt)
                     .ThenBy(e => e.Key, StringComparer.Ordinal))
        {
            evictions.Add(candidate.Key);
            need -= candidate.SizeBytes;
            if (need <= 0) break;
        }

        return need <= 0
            ? CacheWriteDecision.Retain(ttl, evictions.ToArray())
            : CacheWriteDecision.Skip();
    }

    private TimeSpan RetentionFor(string key)
    {
        if (maxAges.TryGetValue(key, out var age) && age > TimeSpan.Zero && age <= TimeSpan.FromMinutes(1))
            return age;
        return key.StartsWith("Preview", StringComparison.Ordinal)
            ? TimeSpan.FromSeconds(5)
            : TimeSpan.FromSeconds(2);
    }

    // ---------- API ----------

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;

        EndpointInfo? best = null;
        foreach (var endpoint in context.Endpoints)
            if (best == null || Better(endpoint, best)) best = endpoint;

        return EndpointSelectionDecision.Assign(best!.Id);
    }

    private static bool Better(EndpointInfo candidate, EndpointInfo current)
    {
        double load = RelativeLoad(candidate), currentLoad = RelativeLoad(current);
        if (load != currentLoad) return load < currentLoad;
        if (candidate.Speed != current.Speed) return candidate.Speed > current.Speed;
        return candidate.Id < current.Id;
    }

    private static double RelativeLoad(EndpointInfo endpoint) =>
        endpoint.Slots <= 0 ? double.MaxValue : (double)endpoint.OutstandingCalls / endpoint.Slots;

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var endpoint = context.Endpoint;
        var now = context.Now;
        long id = endpoint.Id;
        if (endpoint.State != InstanceState.Ready) return EndpointDecision.Allow();
        firstSeen.TryAdd(id, now);

        CallResultInfo? last = context.RecentResults.Count == 0
            ? null
            : context.RecentResults[context.RecentResults.Count - 1];

        if (endpoint.Mode == EndpointMode.BlockTraffic)
        {
            if (endpoint.ProbeOutstanding) return EndpointDecision.Block();
            if (!lastProbe.TryGetValue(id, out var probedAt) || now - probedAt >= ProbeInterval)
            {
                lastProbe[id] = now;
                return EndpointDecision.Probe();
            }
            return EndpointDecision.Block();
        }

        // Результат последней пробы: таймаут — экземпляр всё ещё мёртв, иначе возвращаем в работу.
        if (last is { IsProbe: true })
            return last.Outcome == RequestOutcome.Timeout
                ? Block(id, now, endpoint.Role)
                : EndpointDecision.Allow();

        if (endpoint.Mode == EndpointMode.Probe) return EndpointDecision.Probe();

        if (last != null && last.Outcome == RequestOutcome.Timeout && now - last.Time <= RecentFailureWindow)
            return Block(id, now, endpoint.Role);

        if (endpoint.Role == InstanceRole.Worker)
        {
            var reference = endpoint.LastHeartbeat ?? firstSeen[id];
            if (now - reference > HeartbeatLimit) return Block(id, now, endpoint.Role);
        }

        return EndpointDecision.Allow();
    }

    private EndpointDecision Block(long id, TimeSpan now, InstanceRole role)
    {
        lastProbe[id] = now;
        if (role == InstanceRole.Api)
        {
            apiFailureSeen = true;
            lastApiFailure = now;
        }
        return EndpointDecision.Block();
    }

    // ---------- Задания ----------

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        // Контроль допуска: длинная очередь — это клиенты, которые уже не дождутся.
        int cap = Math.Max(4, (int)Math.Ceiling(plannedWorkers * 2.5) + 1);
        if (context.QueuedJobs.Count > cap)
        {
            JobInfo? victim = null;
            double victimScore = double.MaxValue;
            foreach (var job in context.QueuedJobs)
            {
                double score = Score(job, context.Now);
                if (victim == null || score < victimScore)
                {
                    victim = job;
                    victimScore = score;
                }
            }
            if (victim != null) return JobAssignmentDecision.Reject(victim.Id);
        }

        JobInfo? best = null;
        double bestScore = double.MinValue;
        foreach (var job in context.QueuedJobs)
        {
            double score = Score(job, context.Now);
            if (best == null || score > bestScore)
            {
                best = job;
                bestScore = score;
            }
        }
        if (best == null) return null;
        if (bestScore <= 0) return JobAssignmentDecision.Reject(best.Id);

        EndpointInfo? worker = null;
        foreach (var candidate in context.AvailableWorkers)
            if (worker == null || candidate.Speed > worker.Speed ||
                (candidate.Speed == worker.Speed && candidate.Id < worker.Id))
                worker = candidate;

        return JobAssignmentDecision.Assign(best.Id, worker!.Id);
    }

    // Ожидаемая выручка на секунду работы воркера с поправкой на вероятность, что клиент ещё ждёт.
    private static double Score(JobInfo job, TimeSpan now)
    {
        var waited = now - job.QueuedAt;
        if (waited < TimeSpan.Zero) waited = TimeSpan.Zero;

        double work = Math.Max(job.Request.EstimatedWorkSeconds, 0.2);
        double alive = AliveProbability(waited.TotalSeconds + work);
        if (alive <= 0) return -1;

        return (double)job.Request.Revenue * alive / work;
    }

    private static double AliveProbability(double horizonSeconds)
    {
        if (horizonSeconds <= MinimumPatienceSeconds) return 1;
        if (horizonSeconds >= MaximumPatienceSeconds) return 0;
        return (MaximumPatienceSeconds - horizonSeconds) / (MaximumPatienceSeconds - MinimumPatienceSeconds);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        if (job.AssignedAt is not { } assignedAt) return JobRecoveryDecision.KeepWaiting();

        var reference = job.LastHeartbeat ?? assignedAt;
        return context.Now - reference > HeartbeatLimit
            ? JobRecoveryDecision.RequeueAfter(RequeueDelay)
            : JobRecoveryDecision.KeepWaiting();
    }

    // ---------- Мощности ----------

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var now = context.Now;
        generateSamples.RemoveAll(sample => now - sample.At > RateWindow + TimeSpan.FromSeconds(2));

        // Сглаженная оценка интенсивности потока Generate: окно в 1 с слишком шумное,
        // из-за этого воркеры дёргались бы вверх-вниз и сгорали на старте.
        double dt = lastCapacityReview == TimeSpan.MinValue
            ? 1.0
            : Math.Max(0.2, (now - lastCapacityReview).TotalSeconds);
        lastCapacityReview = now;

        int sinceLastReview = 0;
        int recent = 0;
        double workSum = 0;
        decimal revenueSum = 0;
        foreach (var sample in generateSamples)
        {
            var age = now - sample.At;
            if (age <= TimeSpan.FromSeconds(dt)) sinceLastReview++;
            if (age > RateWindow) continue;
            recent++;
            workSum += sample.Work;
            revenueSum += sample.Revenue;
        }

        double instant = sinceLastReview / dt;
        double smoothing = dt / (8.0 + dt);
        rateEstimate += (instant - rateEstimate) * smoothing;

        double averageWork = recent > 0 ? workSum / recent : DefaultWorkerSeconds;
        double averageRevenue = recent > 0 ? (double)(revenueSum / recent) : 0;

        int queued = context.QueuedJobs.Count;
        int assigned = 0;
        foreach (var instance in context.Instances)
            if (instance.Role == InstanceRole.Worker) assigned += instance.AssignedJobs;

        // Будущий поток + разгрузка очереди примерно за 8 секунд, в пересчёте на воркеро-секунды.
        double demand = rateEstimate + queued / 8.0;
        int wanted = (int)Math.Ceiling(demand * averageWork);

        // Расширяемся, только если воркер окупается: выручка на секунду работы заметно выше цены.
        double valuePerWorkerSecond = averageWork > 0 ? averageRevenue / averageWork : 0;
        bool profitable = valuePerWorkerSecond > (double)context.Settings.Worker.PricePerSecond * 2;
        if (!profitable) wanted = Math.Min(wanted, 2);

        // В начале прогона не гасим стартовую пару: поток ещё не успел проявиться в оценке.
        if (now < TimeSpan.FromSeconds(15)) wanted = Math.Max(wanted, 2);
        if ((queued > 0 || assigned > 0 || rateEstimate > 0.02) && wanted < 1) wanted = 1;
        if (queued == 0 && assigned == 0 && rateEstimate < 0.02 && now - lastActivity > TimeSpan.FromSeconds(20))
            wanted = 0;

        wanted = Math.Clamp(wanted, 0, MaximumWorkers);

        // Резервный API держим 5 секунд после сбоя единственного: пока он проверяется пробой
        // и восстанавливается, трафик должен кому-то достаться.
        int desiredApis = apiFailureSeen && now - lastApiFailure <= TimeSpan.FromSeconds(5)
            ? ReserveApis
            : BaseApis;

        // Гистерезис на снижение: одна шумная секунда не должна гасить только что поднятый воркер.
        if (wanted >= plannedWorkers)
        {
            plannedWorkers = wanted;
            reduceHold = 0;
        }
        else if (++reduceHold >= 2)
        {
            plannedWorkers = wanted;
            reduceHold = 0;
        }

        return new CapacityPlan(desiredApis, plannedWorkers);
    }

    public void Observe(SystemObservation observation)
    {
        // Решения принимаются по контекстам; отдельное состояние из событий не требуется.
    }

    // ---------- RPC-политика (первая лабораторная) ----------

    private RequestKind KindOf(long requestId) =>
        kinds.TryGetValue(requestId, out var kind) ? kind : RequestKind.Generate;

    private sealed class RequestPolicy : IRequestPolicy
    {
        private readonly TrushkoSystemPolicy owner;

        public RequestPolicy(TrushkoSystemPolicy owner) => this.owner = owner;

        public TimeSpan GetTimeout(RequestContext context, Random random) => owner.KindOf(context.RequestId) switch
        {
            RequestKind.Metadata => TimeSpan.FromMilliseconds(350),
            RequestKind.Preview => TimeSpan.FromMilliseconds(900),
            _ => TimeSpan.FromMilliseconds(900)
        };

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (observation.Outcome == RequestOutcome.Success) return RetryDecision.NoRetry();
            if (observation.Attempt >= 3) return RetryDecision.NoRetry();

            double limit = owner.KindOf(observation.RequestId) switch
            {
                RequestKind.Metadata => 0.9,
                RequestKind.Preview => 1.8,
                _ => 8.0
            };

            return observation.Elapsed.TotalSeconds < limit
                ? RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(60))
                : RetryDecision.NoRetry();
        }
    }

    private sealed class GenerateSample
    {
        public GenerateSample(TimeSpan at, decimal revenue, double work)
        {
            At = at;
            Revenue = revenue;
            Work = work;
        }

        public TimeSpan At { get; }
        public decimal Revenue { get; }
        public double Work { get; }
    }
}
