# Finding 07 — Producer Logs
**Session 1 captured:** 2026-08-25 22:29 IST  
**Session 2 captured:** 2026-08-27 16:44–16:52 IST

---

## Session 1 — Healthy Run (2026-08-25)

### Command
```bash
docker compose logs producer --tail=10
```

### Result
```
producer  | 2026-08-25T16:59:10 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:10 [INFO]    producer — Poll #30 — message counts: {'bitcoin': 30, 'ethereum': 30, 'solana': 30, 'binancecoin': 30}
producer  | 2026-08-25T16:59:13 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:19 [WARNING] producer — Rate limited (429). Retry 1/5 in 2s
producer  | 2026-08-25T16:59:21 [WARNING] producer — Rate limited (429). Retry 2/5 in 4s
producer  | 2026-08-25T16:59:25 [WARNING] producer — Rate limited (429). Retry 3/5 in 8s
producer  | 2026-08-25T16:59:34 [WARNING] producer — Rate limited (429). Retry 4/5 in 16s
producer  | 2026-08-25T16:59:51 [WARNING] producer — Rate limited (429). Retry 5/5 in 32s
```

### Notes
- Poll #30 — 30 successful API poll cycles completed
- 30 polls × 4 coins = 120 messages published to Kafka
- Rate limiting (HTTP 429) already occurring at 3s poll interval
- Backoff: 2s ? 4s ? 8s ? 16s ? 32s = ~62s wait per episode
- Prices at capture: BTC $79,108 | ETH $2,471.71 | SOL $98.21 | BNB $698.83

---

## Session 2 — Rate Limiting Severe (2026-08-27)

### Symptom
Rate limiting occurring every ~5 successful polls. Each 429 cycle
costs ~62 seconds, causing significant data gaps in Kafka.

### Log Extract
```
producer  | 2026-08-27T16:44:52 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$80,492.00', 'ethereum': '$2,528.50', 'solana': '$108.67', 'binancecoin': '$712.86'}
producer  | 2026-08-27T16:44:55 [WARNING] producer — Rate limited (429). Retry 1/5 in 2s
producer  | 2026-08-27T16:44:57 [WARNING] producer — Rate limited (429). Retry 2/5 in 4s
producer  | 2026-08-27T16:45:01 [WARNING] producer — Rate limited (429). Retry 3/5 in 8s
producer  | 2026-08-27T16:45:09 [WARNING] producer — Rate limited (429). Retry 4/5 in 16s
producer  | 2026-08-27T16:45:25 [WARNING] producer — Rate limited (429). Retry 5/5 in 32s
producer  | 2026-08-27T16:45:57 [ERROR]   producer — CoinGecko fetch failed after 5 attempts.
producer  | 2026-08-27T16:45:57 [ERROR]   producer — Skipping this poll cycle — no data.
producer  | 2026-08-27T16:45:58 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$80,504.00', ...}
producer  | 2026-08-27T16:46:01 [INFO]    producer — Poll #30 — message counts: {'bitcoin': 30, 'ethereum': 30, 'solana': 30, 'binancecoin': 30}
```

### Pattern Observed
| Time | Event |
|---|---|
| 16:44:52 | Last successful publish |
| 16:44:55 | 429 — retry 1 |
| 16:45:57 | All 5 retries exhausted (~62s wasted) |
| 16:45:57 | Cycle skipped — Kafka gets NO data for 62s |
| 16:45:58 | Next call succeeds — resumes |
| 16:46:15 | 429 again — same cycle repeats |

### Root Cause
3-second polling = 20 req/min, exceeding CoinGecko free-tier burst threshold.

### Fix Applied
See `10_rate_limiting_fix.md` for full details.

After fix — new behaviour:
```
producer  | [INFO]    Published 4 messages | prices: {'bitcoin': '$80,521.00', ...}
producer  | [WARNING] [STALE] CoinGecko unavailable — republishing last known prices.
producer  | [INFO]    Published 4 messages | prices: {'bitcoin': '$80,521.00', ...}  ? no gap
```
