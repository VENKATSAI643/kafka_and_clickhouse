# Finding 07 — Producer Logs
**Captured:** 2026-08-25 22:29 IST

---

## Command

```bash
docker compose logs producer --tail=10
```

## Result

```
producer  | 2026-08-25T16:59:10 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:10 [INFO]    producer — Poll #30 — message counts: {'bitcoin': 30, 'ethereum': 30, 'solana': 30, 'binancecoin': 30}
producer  | 2026-08-25T16:59:10 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:13 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:17 [INFO]    producer — Published 4 messages | prices: {'bitcoin': '$79,108.00', 'ethereum': '$2,471.71', 'solana': '$98.21', 'binancecoin': '$698.83'}
producer  | 2026-08-25T16:59:19 [WARNING] producer — Rate limited (429). Retry 1/5 in 2s
producer  | 2026-08-25T16:59:21 [WARNING] producer — Rate limited (429). Retry 2/5 in 4s
producer  | 2026-08-25T16:59:25 [WARNING] producer — Rate limited (429). Retry 3/5 in 8s
producer  | 2026-08-25T16:59:34 [WARNING] producer — Rate limited (429). Retry 4/5 in 16s
producer  | 2026-08-25T16:59:51 [WARNING] producer — Rate limited (429). Retry 5/5 in 32s
```

## Notes

- Poll #30 reached — producer has completed 30 successful API poll cycles
- 30 polls × 4 coins = 120 messages published to Kafka total
- Currently rate-limited (HTTP 429) — CoinGecko free tier allows ~30 req/min
- Exponential backoff: 2s → 4s → 8s → 16s → 32s = ~62s wait per rate-limit episode
- Producer successfully auto-recovers and resumes after each rate-limit episode
- Published prices at time of capture:
    BTC  $79,108.00
    ETH   $2,471.71
    SOL      $98.21
    BNB     $698.83
