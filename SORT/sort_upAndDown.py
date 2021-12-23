n = int(input())
ans = [int(input()) for _ in range(n)]
ans = sorted(ans, reverse=True)
for i in ans:
    print(i, end=' ')