# Performance Comparison Summary

Profile: Full profile (includes flat user reads)
Logical average latency: 0.0059 s
Direct average latency: 0.0068 s
Logical latency wins: 4/16
Logical throughput wins: 4/16

| Scenario | Size | Logical avg (s) | Direct avg (s) | Overhead (s) | Overhead (%) |
| --- | ---: | ---: | ---: | ---: | ---: |
| flat_user_read | 25 | 0.0039 | 0.0035 | 0.0005 | 13.3636 |
| highly_nested_read | 25 | 0.0050 | 0.0030 | 0.0020 | 68.6044 |
| multi_entity_update | 25 | 0.0099 | 0.0163 | -0.0065 | -39.5426 |
| jsonb_drift_read | 25 | 0.0031 | 0.0026 | 0.0006 | 21.5347 |
| flat_user_read | 50 | 0.0035 | 0.0025 | 0.0010 | 39.8930 |
| highly_nested_read | 50 | 0.0034 | 0.0028 | 0.0006 | 20.3245 |
| multi_entity_update | 50 | 0.0102 | 0.0225 | -0.0123 | -54.5795 |
| jsonb_drift_read | 50 | 0.0058 | 0.0040 | 0.0017 | 43.0456 |
| flat_user_read | 100 | 0.0036 | 0.0029 | 0.0007 | 22.9583 |
| highly_nested_read | 100 | 0.0037 | 0.0033 | 0.0004 | 13.6603 |
| multi_entity_update | 100 | 0.0083 | 0.0121 | -0.0038 | -31.5219 |
| jsonb_drift_read | 100 | 0.0048 | 0.0033 | 0.0016 | 47.3435 |
| flat_user_read | 200 | 0.0057 | 0.0039 | 0.0018 | 45.8214 |
| highly_nested_read | 200 | 0.0062 | 0.0042 | 0.0020 | 46.5434 |
| multi_entity_update | 200 | 0.0117 | 0.0167 | -0.0050 | -29.7290 |
| jsonb_drift_read | 200 | 0.0060 | 0.0053 | 0.0006 | 11.9870 |
