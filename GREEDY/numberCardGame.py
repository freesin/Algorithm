n, m = map(int, input().split())
ans = []
for _ in range(n):
    data = list(map(int, input().split()))
    ans.append(min(data))
print(max(ans))
