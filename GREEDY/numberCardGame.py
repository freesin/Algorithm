n, m = map(int, input().split())
ans_list = []
ans = 0
for _ in range(n):
    data = list(map(int, input().split()))
    ans_list.append(min(data))
print(max(ans_list))