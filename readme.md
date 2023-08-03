# IndexSquare

## ART

cpu: amd ryzen 9 7945hx(#of CPU Cores 16, #of Threads 32)

随机生成字符串
```
std::mt19937_64 rng(0);
std::to_string(rng());
```

### single thread bench
对art/std::map/std::unordered_map进行插入测试

```
type   |    art    |  std::map  | std::unordered_map | 
------------------------------------------------------
Mop/s  |   2.12    |     0.32   |      1.38          |
```

### multi thread bench

```
thread |   1   |   2   |   4   |   8   |   16   |   24   |   32   |
-------------------------------------------------------------------
Mop/s  | 1.73  | 3.40  |  6.33 | 12.42 |  15.42 |  22.72 |  24.26 |
```

