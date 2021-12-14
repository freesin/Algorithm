n, m = map(int, input().split())

frame = []
for i in range(n):
    frame.append(list(map(int, input().split())))


# DFS로 특정한 노드를 방문한 뒤에 연결된 모든 노드들도 방문
def dfs(x, y):
    # 주어진 범위를 벗어나는 경우에는 즉시 종료
    if x <= -1 or x >= n or y <= -1 or y >= m:
        return False
    #현재 노드들 아직 방문하지 않았다면
    if frame[x][y] == 0:
        #해당 노드는 방문 처리
        frame[x][y] = 1
        # 상, 하, 좌, 우 모두 재귀적으로 호출
        dfs(x - 1, y)
        dfs(x, y - 1)
        dfs(x + 1, y)
        dfs(x, y + 1)
        return True
    return False

#모든 노드(위치)에 대하여 음료수 채우기
ans = 0
for i in range(n):
    for j in range(m):
        # 현재 위치에서 DFS 수행
        if dfs(i, j) == True:
            ans += 1

print(ans)


