n = int(input())

ans = []
for i in range(n):
    ans.append(list(map(str, input().split())))

ans = sorted(ans, key=lambda ans:ans[1])
for i in ans:
    print(i[0], end=' ')
